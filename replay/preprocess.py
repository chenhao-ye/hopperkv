"""
Preprocess a cache trace. The input cache trace is expected to be a CSV with fields:
    timestamp, key, key_size, val_size, client_id, op, ttl
"""

import argparse
import logging
import sys
from collections import OrderedDict
from pathlib import Path


def format_size(num_bytes: int) -> str:
    """
    Format a size in bytes into a human-readable string using GB, MB, KB, or bytes.
    """
    if num_bytes >= 1024**3:
        return f"{num_bytes / (1024**3):.2f} GB"
    elif num_bytes >= 1024**2:
        return f"{num_bytes / (1024**2):.2f} MB"
    elif num_bytes >= 1024:
        return f"{num_bytes / 1024:.2f} KB"
    else:
        return f"{num_bytes} B"


class ImageBuilder:
    def __init__(self, update_val_size: bool, file_path: str | Path):
        # image: key -> val_size (LRU)
        self.image: OrderedDict[str, int] = OrderedDict()
        self.image_size = 0
        self.update_val_size = update_val_size
        self.append_mode = False
        # Always open file handle for writing
        self.file_handle = open(file_path, "w")
        self.file_handle.write("key,val_size\n")

    def access(self, key: str, val_size: int):
        if self.append_mode:
            # In append mode, write directly to file
            self.file_handle.write(f"{key},{val_size}\n")
            self.image_size += len(key) + val_size
        else:
            # Normal LRU mode
            if key in self.image:
                self.image.move_to_end(key)  # move to end (most recently used)
                if self.update_val_size:
                    old_val_size = self.image[key]
                    self.image[key] = val_size
                    self.image_size += val_size - old_val_size
            else:
                self.image[key] = val_size
                self.image_size += len(key) + val_size

    def _dump_and_clear(self) -> None:
        logging.info(
            f"Dumped {len(self.image):,} keys ({format_size(self.image_size)})"
        )
        for key, val_size in self.image.items():
            self.file_handle.write(f"{key},{val_size}\n")
        # clear the LRU dict to save memory
        self.image.clear()

    def switch_to_append_mode(self) -> None:
        """Dump existing LRU data and switch to append mode for subsequent accesses"""
        assert not self.append_mode

        # First dump existing LRU data to file
        self._dump_and_clear()

        # Switch to append mode - subsequent writes will be appended
        self.append_mode = True

    def dump(self, auto_clear=True) -> None:
        if not self.append_mode:
            self._dump_and_clear()

        # Always close file handle after dump
        self.file_handle.close()
        self.file_handle = None

        if auto_clear:  # release memory
            self.clear()

    def clear(self) -> None:  # release memory
        self.image.clear()
        self.image_size = 0
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None


class TraceProcessor:
    """
    Preprocess a cache trace. Here we have two cut-off timepoint t1 and t2.
    - t1 is determined by the max min_line and min_timestamp.
    - t2 is determined by the min between max_line and max_timestamp.

    All *_image.csv has two columns: key, val_size.
    All *_trace.csv has four columns: timestamp, op, key, val_size.

    Convert a cache trace file into five files:
    - pre_trace.csv: the requests stream during [0, t1).
    - pre_image.csv: all data needed to serve replay of pre_trace.csv.
        If a kv-pair is accessed multiple times, val_size is its first appearance
        during [0, t1).
    - req_trace.csv: the requests stream during [t1, t2].
    - cache_image.csv: all data accessed until t1, sorted from LRU to MRU.
        If a kv-pair is accessed multiple times, val_size is its last appearance
        during [0, t1).
        Loading this image to Redis is equivalent to the cache image at t1.
    - persist_image.csv: all data needed to serve replay of req_trace.csv.
        If a kv-pair is accessed multiple times, val_size is its first appearance
        during [t1, t2).
        Loading this image to DynamoDB to ensure Redis can fetch data during [t1, t2].

    Optionally, support another t0 (by the min of cache_append_since_line and
    cache_append_since_timestamp) to switch cache_image.csv to append mode. In the
    append mode, append new accesses to cache_image.csv instead of updating the LRU.
    """

    def __init__(
        self,
        dump_dir: Path,
        trace_file: Path | None,
        min_line: int,
        max_line: int,
        min_timestamp: float,
        max_timestamp: float,
        cache_append_since_line: float,
        cache_append_since_timestamp: float,
        report_interval: int = 1_000_000,
    ):
        if trace_file is not None:
            if not trace_file.exists() or not trace_file.is_file():
                raise FileNotFoundError(
                    f"Trace path {trace_file} does not exist or is not a file."
                )

        self.trace_file = trace_file
        self.dump_dir = dump_dir

        if dump_dir.exists():
            if not dump_dir.is_dir():
                raise NotADirectoryError(
                    f"Dump path {dump_dir} exists but is not a directory."
                )
        else:
            self.dump_dir.mkdir(parents=True, exist_ok=True)

        self.min_line = min_line
        self.max_line = max_line
        self.min_timestamp = min_timestamp
        self.max_timestamp = max_timestamp
        self.cache_append_since_line = cache_append_since_line
        self.cache_append_since_timestamp = cache_append_since_timestamp
        self.report_interval = report_interval

        self.pre_image_builder = ImageBuilder(
            update_val_size=False, file_path=self.dump_dir / "pre_image.csv"
        )  # for [0, t1)
        self.cache_image_builder = ImageBuilder(
            update_val_size=True, file_path=self.dump_dir / "cache_image.csv"
        )  # for [0, t1)
        self.persist_image_builder = ImageBuilder(
            update_val_size=False, file_path=self.dump_dir / "persist_image.csv"
        )  # for [t1, t2)

        self.in_cache: bool = True  # whether in [0, t1) window
        self.num_lines = 0
        self.num_reqs = 0
        self.last_timestamp = 0

    def process_file(self) -> None:
        # Use stdin if no trace file specified
        f_input = open(self.trace_file, "r") if self.trace_file else sys.stdin

        try:
            with (
                open(self.dump_dir / "pre_trace.csv", "w") as f_pre,
                open(self.dump_dir / "req_trace.csv", "w") as f_req,
            ):
                f_pre.write("timestamp,op,key,val_size\n")
                f_req.write("timestamp,op,key,val_size\n")

                for line in f_input:
                    success = self.process_line(line.strip(), f_pre, f_req)
                    if not success:
                        break

            self.persist_image_builder.dump()

            logging.info(
                f"Completed processing {self.trace_file if self.trace_file else 'stdin'}"
            )
        finally:
            # Close file if it was opened (don't close stdin)
            if self.trace_file:
                f_input.close()

    def process_line(self, line: str, f_pre, f_req) -> bool:
        if self.num_lines >= self.max_line:
            logging.info(f"Stop at #line={self.num_lines} @{self.last_timestamp}")
            return False

        parts = line.split(",")
        if len(parts) != 7:
            self.num_lines += 1
            logging.warning(
                f"Malformed row at #line={self.num_lines}; will skip: {line}"
            )
            with open(self.dump_dir / "malformed.txt", "a") as f_mal:
                f_mal.write(f"{line}\n")
            return True

        timestamp, key, key_size, val_size, client_id, op, ttl = line.split(",")
        timestamp = int(timestamp)
        val_size = int(val_size)
        assert len(key) == int(key_size)

        if op not in {"get", "set"}:
            self.num_lines += 1
            logging.warning(
                f"Warning: Skipping unsupported {op=} "
                f"at #line={self.num_lines} @{timestamp}: {line}"
            )
            return True

        if timestamp > self.max_timestamp:
            logging.info(f"Stop at #line={self.num_lines} @{self.last_timestamp}")
            return False

        # check if we have reached t0 (cache append mode starts)
        if (
            self.in_cache
            and not self.cache_image_builder.append_mode
            and (
                self.num_lines >= self.cache_append_since_line
                or timestamp >= self.cache_append_since_timestamp
            )
        ):
            # dump existing LRU data first, then switch to append mode
            self.cache_image_builder.switch_to_append_mode()

        if (
            self.in_cache
            and self.num_lines >= self.min_line
            and timestamp >= self.min_timestamp
        ):  # reach t1
            self.pre_image_builder.dump()
            self.cache_image_builder.dump()
            self.in_cache = False

        if self.in_cache:
            f_pre.write(f"{timestamp},{op},{key},{val_size}\n")
            self.pre_image_builder.access(key, val_size)
            self.cache_image_builder.access(key, val_size)
        else:
            f_req.write(f"{timestamp},{op},{key},{val_size}\n")
            self.num_reqs += 1
            self.persist_image_builder.access(key, val_size)

        self.num_lines += 1
        self.last_timestamp = timestamp

        if self.num_lines % self.report_interval == 0:
            if self.in_cache:
                logging.info(
                    f"Processed {self.num_lines:,} lines [@{timestamp:,}]: "
                    f"pre_size={format_size(self.pre_image_builder.image_size)}, "
                    f"cache_size={format_size(self.cache_image_builder.image_size)}"
                )
            else:
                logging.info(
                    f"Processed {self.num_lines:,} lines [@{timestamp:,}]: "
                    f"persist_size={format_size(self.persist_image_builder.image_size)}, "
                    f"#reqs={self.num_reqs}"
                )

        return True


def main():
    parser = argparse.ArgumentParser(description="Preprocess a cache trace")
    parser.add_argument(
        "--dump-dir",
        type=Path,
        required=True,
        help="Directory to dump the output files",
    )
    parser.add_argument(
        "--trace-file",
        type=Path,
        help="CSV file containing cache trace data (default: stdin)",
    )
    # below min/max constraints determine the cut-off line jointly
    # the min limit is the higher one between two, and the max limit is the lower one
    # between two; if leave one unset, the default value will be equivalent to no limit
    parser.add_argument(
        "--min-line",
        type=float,
        default=0,
        help="Minimum line number to process (default: 0)",
    )
    parser.add_argument(
        "--max-line",
        type=float,
        default=float("inf"),
        help="Maximum line number to process (default: no limit)",
    )
    parser.add_argument(
        "--min-timestamp",
        type=float,
        default=0,
        help="Minimum timestamp to process (default: 0)",
    )
    parser.add_argument(
        "--max-timestamp",
        type=float,
        default=float("inf"),
        help="Maximum timestamp to process (default: no limit)",
    )
    parser.add_argument(
        "--cache-append-since-line",
        type=float,
        default=float("inf"),
        help="Line number to start append mode to cache_image.csv",
    )
    parser.add_argument(
        "--cache-append-since-timestamp",
        type=float,
        default=float("inf"),
        help="Timestamp to start append mode to cache_image.csv",
    )
    parser.add_argument(
        "--report-interval",
        "-r",
        type=int,
        default=1_000_000,
        help="Print progress every N lines (default: 1,000,000)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    # Create workload builder
    p = TraceProcessor(
        dump_dir=args.dump_dir,
        trace_file=args.trace_file,
        min_line=args.min_line,
        max_line=args.max_line,
        min_timestamp=args.min_timestamp,
        max_timestamp=args.max_timestamp,
        cache_append_since_line=args.cache_append_since_line,
        cache_append_since_timestamp=args.cache_append_since_timestamp,
        report_interval=args.report_interval,
    )

    logging.info(
        "Starting workload building from "
        f"{args.trace_file if args.trace_file else 'stdin'}..."
    )

    # Process file
    p.process_file()

    # Print results
    print("=== Trace Preprocess Summary ===")
    print(f"Total lines processed: {p.num_lines:,}")
    print(f"Total requests:        {p.num_reqs:,}")


if __name__ == "__main__":
    main()
