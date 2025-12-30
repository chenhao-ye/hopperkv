# This script aims to study the memory behavior of Redis.

import argparse
import concurrent.futures
import logging
import signal
import subprocess
from pathlib import Path
from typing import List, Tuple

import psutil

from driver.launch import cleanup_redis, launch_servers
from driver.utils import prepare_data_dir
from hopperkv.hopper_redis import HopperRedis


def get_mem_stat(s, r) -> Tuple[float, float, float, float]:
    # `rmem` prefix means it's reported from Redis server
    # `pmem` prefix means it's a process info reported from OS
    mem_stat_dict = r.memory_stats()
    rmem_total = mem_stat_dict["total.allocated"]
    rmem_startup = mem_stat_dict["startup.allocated"]
    rmem_per_key = mem_stat_dict["keys.bytes-per-key"]
    pmem_total = psutil.Process(s.pid).memory_info().rss
    logging.debug(
        f"rmem.total={rmem_total / 1024 / 1024:.2f}MB, "
        f"rmem.startup={rmem_startup / 1024 / 1024:.2f}MB, "
        f"rmem.bytes_per_key={rmem_per_key:,.0f}B, "
        f"pmem.total={pmem_total / 1024 / 1024:.2f}MB"
    )
    return rmem_total, rmem_startup, rmem_per_key, pmem_total


# Simple port allocation
_next_port = 6400


def get_next_port():
    global _next_port
    port = _next_port
    _next_port += 1
    return port


def generate_cache_image(args):
    """Generate cache_image.csv file for a specific test configuration"""
    num_keys, key_size, val_size, data_dir = args

    prepare_data_dir(data_dir, cleanup=True)
    cache_image_path = data_dir / "cache_image.csv"

    # Generate keys with zero-padding
    with open(cache_image_path, "w") as f:
        f.write("key,val_size\n")

        for i in range(num_keys):
            # Zero-pad key to key_size
            key = str(i).zfill(key_size)
            f.write(f"{key},{val_size}\n")

    return str(cache_image_path.absolute())


def run_single_test(args: Tuple):
    """Run a single test: use ProcessPoolExecutor to generate cache image, then start Redis server and load image"""
    num_keys, key_size, val_size, cache_size, data_dir = args

    # Step 1: Generate cache image using ProcessPoolExecutor
    cache_gen_args = (num_keys, key_size, val_size, data_dir)
    with concurrent.futures.ProcessPoolExecutor() as executor:
        future = executor.submit(generate_cache_image, cache_gen_args)
        cache_image_path = future.result()
        logging.debug(f"Generated cache image for val_size={val_size:,}")

    # Step 2: Set up data directory and start Redis server
    port = get_next_port()
    s = launch_servers(ports=[port], data_dir=data_dir, cleanup=False)[0]

    # Step 3: Connect and configure Redis
    r = HopperRedis(host="localhost", port=port)
    r.wait_ready()
    # this should not make a difference
    r.set_config("dynamo.mock", "image")
    r.set_resrc(cache_size, -1, -1, -1)

    # Step 4: Load cache image
    r.load(cache_image_path)
    logging.debug(f"Loaded cache for val_size={val_size:,}")

    # Step 5: Collect metrics
    rmem_total, rmem_startup, rmem_per_key, pmem_total = get_mem_stat(s, r)
    logging.debug(f"Completed val_size={val_size:,}")

    # Cleanup
    if s.poll() is None:
        s.send_signal(signal.SIGINT)
        try:
            s.wait(timeout=5)
        except subprocess.TimeoutExpired:
            s.kill()
            s.wait()

    cache_image_file = Path(cache_image_path)
    if cache_image_file.exists():
        cache_image_file.unlink()
        logging.debug(f"Deleted cache image: {cache_image_path}")
    return (key_size, val_size, rmem_total, rmem_startup, rmem_per_key, pmem_total)


def run_all_tests_parallel(
    test_configs: List[Tuple[int, int, int]], cache_size: int, data_dir: Path
) -> List[Tuple[int, int, float, float, float, float]]:
    """Run all tests in parallel using ThreadPoolExecutor - each thread uses ProcessPoolExecutor for cache generation"""
    logging.info(f"Running {len(test_configs)} tests in parallel...")

    # Prepare arguments for each test
    test_args = []
    for num_keys, key_size, val_size in test_configs:
        data_subdir = (
            data_dir / f"k={key_size},v={val_size},n={num_keys},c={cache_size}"
        )
        test_args.append((num_keys, key_size, val_size, cache_size, data_subdir))

    # Run all tests in parallel using ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(run_single_test, test_args))

    logging.info("All tests completed.")
    return results


def create_lookup_table(
    key_sizes: List[int],
    val_sizes: List[int],
    metrics: List[str],
    cache_size: int,
    num_keys: int,
    working_set_size: int,
    data_dir: Path,
):
    """Create a lookup table mapping val_size to the selected memory metric"""

    metric_indices = {
        "rmem_total": 0,
        "rmem_startup": 1,
        "rmem_per_key": 2,
        "pmem_total": 3,
        "overhead": 4,
        "overhead_pct": 5,
    }

    for metric in metrics:
        if metric not in metric_indices:
            raise ValueError(
                f"Unknown metric: {metric}. Available: {list(metric_indices.keys())}"
            )

    # Generate all permutations of key_size and val_size
    test_configs = []
    if working_set_size:
        print(f"\nUsing working_set_size: {working_set_size:,} bytes")
        print("Computing num-keys for each (key_size, val_size) combination:")
        print("-" * 70)

        for key_size in key_sizes:
            for val_size in val_sizes:
                computed_num_keys = working_set_size // (key_size + val_size)
                print(
                    f"key_size={key_size:,}, val_size={val_size:,}: num_keys={computed_num_keys:,}"
                )
                test_configs.append((computed_num_keys, key_size, val_size))

        header_info = (
            f"working_set_size={working_set_size:,}, cache_size={cache_size:,}"
        )
    else:
        actual_num_keys = num_keys if num_keys is not None else 102400
        for key_size in key_sizes:
            for val_size in val_sizes:
                test_configs.append((actual_num_keys, key_size, val_size))
        header_info = f"num_keys={actual_num_keys:,}, cache_size={cache_size:,}"

    # Run all tests in parallel - each thread handles cache generation, Redis setup, loading, and metrics collection
    results = run_all_tests_parallel(test_configs, cache_size, data_dir)

    # Sort results by key_size, then val_size
    results.sort(key=lambda x: (x[0], x[1]))

    # Process results once and prepare data for output
    processed_results = []
    for result in results:
        key_size, val_size, rmem_total, rmem_startup, rmem_per_key, pmem_total = result
        overhead = rmem_per_key - (key_size + val_size)
        overhead_percent = (overhead / (key_size + val_size)) * 100
        all_metrics = [
            rmem_total,
            rmem_startup,
            rmem_per_key,
            pmem_total,
            overhead,
            overhead_percent,
        ]
        processed_results.append((key_size, val_size, all_metrics))

    # Calculate column widths
    col_width = 15
    total_width = 2 * col_width + len(metrics) * (col_width + 3) + 7

    # Write results to text file
    with open(data_dir / "result.txt", "w") as f:
        print(f"\nConfig: {header_info}", file=f)
        print("=" * total_width, file=f)

        # Header row
        header = f"| {'key_size':^15} | {'val_size':^15} |"
        for metric in metrics:
            header += f" {metric:^{col_width}} |"
        print(header, file=f)
        print("-" * total_width, file=f)

        for key_size, val_size, all_metrics in processed_results:
            row = f"| {key_size:>15,} | {val_size:>15,} |"
            for metric in metrics:
                metric_idx = metric_indices[metric]
                metric_value = all_metrics[metric_idx]
                if metric == "overhead_pct":
                    row += f" {metric_value:>{col_width}.1f} |"
                else:
                    row += f" {metric_value:>{col_width},.0f} |"
            print(row, file=f)

        print("=" * total_width, file=f)

    # Write results to CSV file
    with open(data_dir / "result.csv", "w") as f:
        # CSV header
        header = "key_size,val_size"
        for metric in metrics:
            header += f",{metric}"
        print(header, file=f)

        # CSV data
        for key_size, val_size, all_metrics in processed_results:
            row = f"{key_size},{val_size}"
            for metric in metrics:
                metric_idx = metric_indices[metric]
                metric_value = all_metrics[metric_idx]
                if metric == "overhead_pct":
                    row += f",{metric_value:.1f}"
                else:
                    row += f",{metric_value:.0f}"
            print(row, file=f)

    # Final cleanup to ensure no Redis servers are left running
    cleanup_redis()


def parse_range(range_str: str) -> List[int]:
    """Parse range string like '100-4000:400' or '100,200,400,800'"""
    if ":" in range_str:
        # Range format: start-end:step
        parts = range_str.split(":")
        if len(parts) != 2:
            raise ValueError("Range format should be 'start-end:step'")

        range_part, step_str = parts
        if "-" not in range_part:
            raise ValueError("Range format should be 'start-end:step'")

        start_str, end_str = range_part.split("-", 1)
        start, end, step = int(start_str), int(end_str), int(step_str)
        return list(range(start, end + 1, step))
    else:
        # Comma-separated format
        return [int(x.strip()) for x in range_str.split(",")]


def main():
    parser = argparse.ArgumentParser(
        description="Create lookup table for val_size to memory metric mapping"
    )
    parser.add_argument(
        "-k",
        "--key-sizes",
        type=str,
        default="16,32,48,64,128",
        help='Key sizes range. Format: "start-end:step" or "size1,size2,size3" (default: 16)',
    )
    parser.add_argument(
        "-v",
        "--val-sizes",
        type=str,
        default="0-1000:10",
        help='Value sizes range. Format: "start-end:step" or "val1,val2,val3" (default: 0-1000:10)',
    )
    parser.add_argument(
        "-w",
        "--working_set_size",
        type=int,
        default=2 * 1024 * 1024 * 1024,
        help="Working set size in bytes (default: 2GB). num-keys = working_set_size/(key_size + val_size)",
    )
    parser.add_argument(
        "-c",
        "--cache-size",
        type=int,
        default=2 * 1024 * 1024 * 1024,
        help="Redis cache size limit in bytes (default: 2GB)",
    )
    parser.add_argument(
        "-m",
        "--metric",
        type=str,
        default=["rmem_per_key", "overhead", "overhead_pct"],
        choices=[
            "rmem_total",
            "rmem_startup",
            "rmem_per_key",
            "pmem_total",
            "overhead",
            "overhead_pct",
        ],
        nargs="+",
        help="Memory metric to display (default: rmem_per_key, overhead, overhead_pct)",
    )
    parser.add_argument(
        "-n",
        "--num-keys",
        type=int,
        help="Number of keys (overrides working_set_size if specified)",
    )
    parser.add_argument(
        "-d",
        "--data_dir",
        help="Directory for experiment data",
        type=Path,
        default=Path("results/profile_memory"),
    )

    args = parser.parse_args()

    # Parse key_sizes and val_sizes
    key_sizes = parse_range(args.key_sizes)
    val_sizes = parse_range(args.val_sizes)

    if args.num_keys:
        print(
            f"Using explicit num-keys: {args.num_keys:,} (overriding working_set_size)"
        )

    cleanup_redis()

    prepare_data_dir(data_dir=args.data_dir, cleanup=True)

    create_lookup_table(
        key_sizes=key_sizes,
        val_sizes=val_sizes,
        metrics=args.metric,
        cache_size=args.cache_size,
        num_keys=args.num_keys,
        working_set_size=None if args.num_keys else args.working_set_size,
        data_dir=args.data_dir,
    )


if __name__ == "__main__":
    try:
        main()
    finally:
        cleanup_redis()
