import hashlib
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List

from .base import Req, ReqGenEngine, Workload


class TraceReplayGenEngine(ReqGenEngine):
    def __init__(
        self,
        trace_filepath: Path,
        use_ts: bool,
        trace_shard_idx: int,
        trace_num_shards: int,
        max_timestamp: float,
        max_line: int,
        queue_size: int,
    ):
        self.use_ts = use_ts
        self.max_timestamp = max_timestamp
        self.max_line = max_line
        assert trace_shard_idx < trace_num_shards

        self.trace_queue = queue.Queue(maxsize=queue_size)
        self.line_count = 0
        self.timestamp = 0

        # Start background thread to read trace data
        self.reader_thread = threading.Thread(
            target=self._read_trace_data_wrapper,
            args=(trace_filepath, trace_shard_idx, trace_num_shards),
            daemon=True,
        )
        self.reader_thread.start()

        # Wait until queue is fully populated OR reader thread has completed
        while self.reader_thread.is_alive() and self.trace_queue.qsize() < queue_size:
            curr_queue_size = self.trace_queue.qsize()
            logging.info(
                "Wait for trace_queue to be populated: "
                f"{curr_queue_size:,} / {queue_size:,} = "
                f"{curr_queue_size * 100 / queue_size:.1f}%"
            )
            time.sleep(5)

        self.ts_begin = time.perf_counter()

    def _hash(self, key: str) -> int:
        """Deterministic (across processes) hash using SHA256"""
        return int(hashlib.sha256(key.encode()).hexdigest()[:8], 16)

    def _read_trace_data_wrapper(
        self, trace_filepath: Path, trace_shard_idx: int, trace_num_shards: int
    ):
        """Wrapper function to catch exceptions and exit process if reader crashes"""
        try:
            self._read_trace_data(trace_filepath, trace_shard_idx, trace_num_shards)
        except Exception as e:
            logging.error(f"Reader thread crashed: {e}")
            logging.error("Exiting process due to reader thread crash")
            os._exit(1)

    def _read_trace_data(
        self, trace_filepath: Path, trace_shard_idx: int, trace_num_shards: int
    ):
        """Background thread function to read trace data and put into queue"""

        with open(trace_filepath, "r") as f:
            next(f, None)  # skip header row
            for line_num, line in enumerate(f):
                line = line.strip()
                parts = line.split(",")
                assert len(parts) == 4, f"Malformed row: {line}"
                timestamp, op, key, val_size = parts
                timestamp = int(timestamp)
                val_size = int(val_size)

                # Stop if timestamp exceeds max_timestamp or line_num exceeds max_line
                if timestamp > self.max_timestamp:
                    break
                if line_num >= self.max_line:
                    break

                if op not in ("get", "set"):
                    logging.warning(
                        f"Warning: Skipping unsupported {op=} in row {line}"
                    )
                    continue

                key_hash = int(hashlib.sha256(key.encode()).hexdigest()[:8], 16)
                if key_hash % trace_num_shards != trace_shard_idx:
                    continue

                # Put the trace entry into the queue
                self.trace_queue.put((timestamp, op == "set", key, val_size))

            # Put a sentinel value to indicate end of data
            self.trace_queue.put(None)

    def reset_begin_ts(self, ts: float | None = None) -> None:
        self.ts_begin = ts if ts is not None else time.perf_counter()

    def make_req(self) -> Req | None:
        if self.trace_queue.empty():
            logging.warning("Trace queue is empty, trace I/O may become a bottleneck")
        # Get next trace entry from queue
        trace_entry = self.trace_queue.get()
        if trace_entry is None:
            return None  # End of trace data

        self.timestamp, is_write, key, val_size = trace_entry
        if self.use_ts:
            now = time.perf_counter()
            target = self.ts_begin + self.timestamp
            if now < target:
                time.sleep(target - now)
        v = "v" * val_size if is_write else None
        req = Req(key, v, self._hash(key))
        self.line_count += 1
        return req

    def is_done(self, elapsed: float = 0) -> bool:
        # Check if reader thread is still alive and queue is empty
        return not self.reader_thread.is_alive() and self.trace_queue.empty()

    def __str__(self) -> str:
        return f"TraceReplay[progress={self.line_count}, timestamp={self.timestamp}]"


@dataclass
class TraceReplayWorkload(Workload):
    trace_filepath: Path
    use_ts: bool

    @classmethod
    def from_string(cls, s: str):
        """
        formatted as `TRACE:[replay_mode]:[trace_filepath]`.
        replay_mode can be `timestamp` (submit requests based on the timestamps in "
        "the trace) or `loop` (submit requests in a closed-loop)
        """
        s = s.strip()
        if not s.startswith("TRACE:"):
            raise ValueError("TraceReplayWorkload string must start with 'TRACE:'")

        _, replay_mode, trace_filepath = s.split(":", 2)

        replay_mode = replay_mode.strip().lower()
        if replay_mode not in ("timestamp", "loop"):
            raise ValueError("replay_mode must be 'timestamp' or 'loop'")
        use_ts = replay_mode == "timestamp"

        return cls(Path(trace_filepath.strip()), use_ts)

    def __str__(self) -> str:
        return f"TRACE:{'timestamp' if self.use_ts else 'loop'}:{self.trace_filepath}"

    def build_req_gen(
        self,
        trace_shard_idx: int,
        trace_num_shards: int,
        max_timestamp: float,
        max_line: int,
        queue_size: int,
    ) -> List[ReqGenEngine]:
        return [
            TraceReplayGenEngine(
                self.trace_filepath,
                self.use_ts,
                trace_shard_idx,
                trace_num_shards,
                max_timestamp=max_timestamp,
                max_line=max_line,
                queue_size=queue_size,
            )
        ]


class ImageLoadGenEngine(ReqGenEngine):
    def __init__(
        self, image_filepath: Path, image_shard_idx: int, image_num_shards: int
    ):
        self.image_queue = queue.Queue(maxsize=10_000_000)
        self.line_count = 0

        # Start background thread to read image data
        self.reader_thread = threading.Thread(
            target=self._read_image_data_wrapper,
            args=(image_filepath, image_shard_idx, image_num_shards),
            daemon=True,
        )
        self.reader_thread.start()

    def _hash(self, key: str) -> int:
        """Deterministic (across processes) hash using SHA256"""
        return int(hashlib.sha256(key.encode()).hexdigest()[:8], 16)

    def _read_image_data_wrapper(
        self, image_filepath: Path, image_shard_idx: int, image_num_shards: int
    ):
        """Wrapper function to catch exceptions and exit process if reader crashes"""
        try:
            self._read_image_data(image_filepath, image_shard_idx, image_num_shards)
        except Exception as e:
            logging.error(f"Image reader thread crashed: {e}")
            logging.error("Exiting process due to image reader thread crash")
            os._exit(1)

    def _read_image_data(
        self, image_filepath: Path, image_shard_idx: int, image_num_shards: int
    ):
        """Background thread function to read image data and put into queue"""

        with open(image_filepath, "r") as f:
            next(f, None)  # skip header row
            for line_number, line in enumerate(f):
                # Only process lines that match this shard
                if line_number % image_num_shards != image_shard_idx:
                    continue
                line = line.strip()
                if not line:
                    continue
                parts = line.split(",")
                assert len(parts) == 2, f"Malformed row: {line}"
                key, val_size = parts
                val_size = int(val_size)

                # Put the image entry into the queue
                self.image_queue.put((key, val_size))

        # Put a sentinel value to indicate end of data
        self.image_queue.put(None)

    def make_req(self) -> Req | None:
        # Get next image entry from queue
        image_entry = self.image_queue.get()
        if image_entry is None:
            return None  # End of image data

        key, val_size = image_entry
        v = "v" * val_size
        req = Req(key, v, self._hash(key))
        self.line_count += 1
        return req

    def is_done(self, elapsed: float = 0) -> bool:
        # Check if reader thread is still alive and queue is empty
        return not self.reader_thread.is_alive() and self.image_queue.empty()

    def __str__(self) -> str:
        return f"ImageLoad[progress={self.line_count}]"


@dataclass
class ImageLoadWorkload(Workload):
    image_filepath: Path

    @classmethod
    def from_string(cls, s: str):
        """
        formatted as `IMAGE:[image_filepath]`.
        """
        s = s.strip()
        if not s.startswith("IMAGE:"):
            raise ValueError("ImageLoadWorkload string must start with 'IMAGE:'")
        _, image_filepath = s.split(":", 1)

        return cls(image_filepath=Path(image_filepath.strip()))

    def __str__(self) -> str:
        return f"IMAGE:{self.image_filepath}"

    def build_req_gen(
        self, image_shard_idx: int, image_num_shards: int
    ) -> List[ReqGenEngine]:
        return [
            ImageLoadGenEngine(self.image_filepath, image_shard_idx, image_num_shards)
        ]
