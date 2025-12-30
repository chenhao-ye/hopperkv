"""
LRU Cache Simulation

Reads a cache_image.csv (initial cache state) and req_trace.csv (request trace),
then simulates LRU replacement with a fixed cache size limit and calculates miss ratio.
"""

import argparse
import logging
from collections import OrderedDict, deque
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


def get_key_shard(key: str, num_shards: int) -> int:
    return hash(key) % num_shards


def round(s):
    # common case: quick lookup
    if s <= 4:
        return 4
    if s <= 60:
        return (s + 3) // 8 * 8 + 4
    if s <= 124:
        return (s + 3) // 16 * 16 + 12
    if s <= 252:
        return (s + 3) // 32 * 32 + 28
    if s <= 508:
        return (s + 3) // 64 * 64 + 60
    if s <= 1020:
        return (s + 3) // 128 * 128 + 124
    if s <= 2044:
        return (s + 3) // 256 * 256 + 252
    if s <= 4092:
        return (s + 3) // 512 * 512 + 508

    # generalized version
    power = 3  # Start with 2^3 = 8
    while True:
        divisor = 2**power
        upper_bound = divisor * 8 - 4

        if s <= upper_bound:
            offset = divisor - 4
            return (s + 3) // divisor * divisor + offset

        power += 1


def estimate_memory(key_size, val_size):
    return 55 + round(key_size) + round(val_size)


class LRUCache:
    """
    LRU Cache implementation using OrderedDict.
    """

    def __init__(self, max_size_bytes: int, recent_window_size: int):
        self.max_size_bytes = max_size_bytes
        self.cache: OrderedDict[str, int] = OrderedDict()  # key -> val_size
        self.current_size = 0
        self.hit_cnt = 0
        self.miss_cnt = 0
        self.req_cnt = 0

        # Sliding window for recent requests
        self.recent_window_size = recent_window_size
        # True for hit, False for miss
        self.recent_requests = deque(maxlen=recent_window_size)

    def access(
        self,
        key: str,
        val_size: int,
        update_miss_ratio: bool,
        update_req_cnt: bool = True,
    ) -> None:
        """
        Add or update a key-value pair in the cache.
        """
        if update_req_cnt:
            self.req_cnt += 1

        if key in self.cache:
            self.cache.move_to_end(key)  # move to end (most recently used)
            old_val_size = self.cache[key]
            self.cache[key] = val_size
            self.current_size += round(val_size) - round(old_val_size)
            if update_miss_ratio:
                self.hit_cnt += 1
                self.recent_requests.append(True)  # Hit
        else:
            self.cache[key] = val_size
            self.current_size += estimate_memory(len(key), val_size)

            if update_miss_ratio:
                self.miss_cnt += 1
                self.recent_requests.append(False)  # Miss

        # Evict LRU items if we exceed the size limit
        while self.current_size > self.max_size_bytes and self.cache:
            # Remove least recently used item (first item)
            lru_key, lru_val_size = self.cache.popitem(last=False)
            self.current_size -= estimate_memory(len(lru_key), lru_val_size)

    def get_recent_miss_ratio(self) -> float:
        """
        Get the miss ratio of the most recent requests (up to recent_window_size).
        """
        if not self.recent_requests:
            return 0.0

        miss_count = sum(1 for is_hit in self.recent_requests if not is_hit)
        return miss_count / len(self.recent_requests)

    def get_stats(self) -> dict:
        """
        Get cache statistics.
        """
        total_requests = self.hit_cnt + self.miss_cnt
        miss_ratio = self.miss_cnt / total_requests if total_requests > 0 else 0.0
        hit_ratio = self.hit_cnt / total_requests if total_requests > 0 else 0.0
        recent_miss_ratio = self.get_recent_miss_ratio()

        return {
            "hit_cnt": self.hit_cnt,
            "miss_cnt": self.miss_cnt,
            "req_cnt:": self.req_cnt,
            "miss_ratio": miss_ratio,
            "hit_ratio": hit_ratio,
            "recent_miss_ratio": recent_miss_ratio,
            "recent_window_size": self.recent_window_size,
            "recent_requests_count": len(self.recent_requests),
            "write_ratio": (self.req_cnt - total_requests) / self.req_cnt,
            "current_size": self.current_size,
            "num_items": len(self.cache),
        }

    def load_cache_image(self, cache_image_path: Path) -> OrderedDict[str, int]:
        """
        Load the initial cache state from cache_image.csv.
        Returns an OrderedDict with key -> val_size, sorted from LRU to MRU.
        """

        with open(cache_image_path, "r") as f:
            next(f)  # Skip header

            for line in f:
                line = line.strip()
                if line:
                    key, val_size = line.split(",")
                    self.access(
                        key,
                        int(val_size),
                        update_miss_ratio=False,
                        update_req_cnt=False,
                    )

        logging.info(f"Loaded {len(self.cache):,} items from cache image")


def simulate_lru(
    cache_image_path: Path,
    req_trace_path: Path,
    cache_size_bytes: int,
    report_interval: int = 100000,
    recent_window_size: int = 10000,
    req_shard: int = 1,
    max_line: int = float("inf"),
    max_timestamp: float = float("inf"),
) -> dict:
    """
    Simulate LRU cache replacement.

    Args:
        cache_image_path: Path to cache_image.csv
        req_trace_path: Path to req_trace.csv
        cache_size_bytes: Maximum cache size in bytes
        report_interval: Print progress every N requests
        recent_window_size: Number of recent requests to track for miss ratio
        req_shard: Number of shards to partition requests (1 = no sharding)
        max_line: Maximum line number to process from req_trace.csv
        max_timestamp: Maximum timestamp to process from req_trace.csv

    Returns:
        Dictionary with simulation statistics
    """
    # Initialize LRU cache with the specified size limit
    lru_cache = LRUCache(cache_size_bytes, recent_window_size)
    lru_cache.load_cache_image(cache_image_path)

    logging.info(
        f"Initialized cache with {len(lru_cache.cache):,} items "
        f"({format_size(lru_cache.current_size)})"
    )

    if req_shard > 1:
        logging.info(f"Using request sharding with {req_shard} shards")

    # Initialize shard queues
    shard_queues = [deque() for _ in range(req_shard)]
    num_req = 0
    shard_idx = 0
    line_number = 0

    with open(req_trace_path, "r") as f:
        # Skip header
        next(f)

        while True:
            # Parse more lines until current shard has a request
            while not shard_queues[shard_idx]:
                line = f.readline()
                if not line:  # End of file
                    # Check if any shard has remaining requests
                    if any(shard_queues):
                        shard_idx = (shard_idx + 1) % req_shard
                        continue
                    else:
                        return lru_cache.get_stats()  # No more requests to process

                line_number += 1

                # Check line limit
                if line_number > max_line:
                    logging.info(
                        f"Reached maximum line limit ({max_line}), stopping simulation"
                    )
                    return lru_cache.get_stats()

                line = line.strip()
                if line:
                    parts = line.split(",")
                    if len(parts) != 4:
                        raise ValueError(
                            f"Incorrect format at line {line_number}: {line}"
                        )
                    timestamp, op, key, val_size = parts
                    timestamp = int(timestamp)
                    val_size = int(val_size)

                    # Check timestamp limit
                    if timestamp > max_timestamp:
                        logging.info(
                            f"Reached maximum timestamp limit ({max_timestamp}), "
                            "stopping simulation"
                        )
                        return lru_cache.get_stats()

                    # Determine which shard this request belongs to
                    key_shard_idx = get_key_shard(key, req_shard)
                    shard_queues[key_shard_idx].append((timestamp, op, key, val_size))

            # Replay the request from current shard
            timestamp, op, key, val_size = shard_queues[shard_idx].popleft()

            # Simulate the request
            assert op in {"get", "set"}
            # print(op)
            lru_cache.access(key, val_size, update_miss_ratio=(op == "get"))
            num_req += 1

            if num_req % report_interval == 0:
                stats = lru_cache.get_stats()
                logging.info(
                    f"Processed {num_req:,} requests @{timestamp}: "
                    f"miss_ratio={stats['miss_ratio'] * 100:.2f}%, "
                    f"recent_miss_ratio={stats['recent_miss_ratio'] * 100:.2f}%, "
                    f"write_ratio={stats['write_ratio'] * 100:.2f}%, "
                    f"cache_size={format_size(lru_cache.current_size)}"
                )

    return lru_cache.get_stats()


def main():
    parser = argparse.ArgumentParser(description="Simulate LRU cache replacement")
    parser.add_argument(
        "cache_image_path", type=Path, help="Path to cache_image.csv file"
    )
    parser.add_argument("req_trace_path", type=Path, help="Path to req_trace.csv file")
    parser.add_argument(
        "--cache-size",
        type=int,
        default=2 * 1024 * 1024 * 1024,  # 2GB
        help="Maximum cache size in bytes",
    )
    parser.add_argument(
        "--report-interval",
        type=int,
        default=100_000,
        help="Print progress every N requests (default: 100,000)",
    )
    parser.add_argument(
        "--recent-window-size",
        type=int,
        default=10_000,
        help="Number of recent requests to track for miss ratio (default: 10,000)",
    )
    parser.add_argument(
        "--req-shard",
        type=int,
        default=1,
        help="Number of shards to partition requests (1 = no sharding, default: 1)",
    )
    parser.add_argument(
        "--max-line",
        type=int,
        default=float("inf"),
        help="Maximum line number to process from req_trace.csv (default: unlimited)",
    )
    parser.add_argument(
        "--max-timestamp",
        type=float,
        default=float("inf"),
        help="Maximum timestamp to process from req_trace.csv (default: unlimited)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    # Validate input files
    if not args.cache_image_path.exists():
        logging.error(f"Cache image file not found: {args.cache_image_path}")
        return 1

    if not args.req_trace_path.exists():
        logging.error(f"Request trace file not found: {args.req_trace_path}")
        return 1

    logging.info("Starting LRU simulation...")
    logging.info(f"Cache image: {args.cache_image_path}")
    logging.info(f"Request trace: {args.req_trace_path}")
    logging.info(f"Cache size limit: {format_size(args.cache_size)}")
    logging.info(f"Recent window size: {args.recent_window_size:,} requests")
    logging.info(f"Request sharding: {args.req_shard} shards")

    # Run simulation
    stats = simulate_lru(
        args.cache_image_path,
        args.req_trace_path,
        args.cache_size,
        args.report_interval,
        args.recent_window_size,
        args.req_shard,
        args.max_line,
        args.max_timestamp,
    )

    # Print results
    print("\n=== LRU Simulation Results ===")
    print(f"Hit cnt:           {stats['hit_cnt']:,}")
    print(f"Miss cnt:          {stats['miss_cnt']:,}")
    print(f"Hit ratio:         {stats['hit_ratio']:.4f}")
    print(f"Miss ratio:        {stats['miss_ratio']:.4f}")
    print(
        f"Recent miss ratio: {stats['recent_miss_ratio']:.4f} "
        f"(last {stats['recent_requests_count']:,} requests)"
    )
    print(f"Final cache size:  {format_size(stats['current_size'])}")
    print(f"Final cache items: {stats['num_items']:,}")


if __name__ == "__main__":
    main()
