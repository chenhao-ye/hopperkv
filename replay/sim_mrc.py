"""
Miss Ratio Curve (MRC) Simulation

Runs multiple LRU simulations with different cache sizes in parallel to generate
miss ratio curves. The x-axis represents cache size and y-axis represents miss ratio.
"""

import argparse
import json
import logging
import multiprocessing as mp
import subprocess
from pathlib import Path
from typing import List, Tuple

import matplotlib.pyplot as plt
import numpy as np


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


def run_lru_simulation(args_tuple: Tuple) -> Tuple[int, float]:
    """
    Run a single LRU simulation with specified cache size.

    Args:
        args_tuple: (cache_image_path, req_trace_path, cache_size_bytes, sim_lru_path,
            req_shard, max_line, max_timestamp)

    Returns:
        Tuple of (cache_size_bytes, miss_ratio)
    """
    (
        cache_image_path,
        req_trace_path,
        cache_size_bytes,
        sim_lru_path,
        req_shard,
        max_line,
        max_timestamp,
    ) = args_tuple

    cmd = [
        "uv",
        "run",
        str(sim_lru_path),
        str(cache_image_path),
        str(req_trace_path),
        "--cache-size",
        str(cache_size_bytes),
        "--req-shard",
        str(req_shard),
        "--report-interval",
        "1000000",  # Less frequent reporting for parallel runs
    ]

    # Add max-line and max-timestamp arguments if they are not infinite
    if max_line != float("inf"):
        cmd.extend(["--max-line", str(max_line)])
    if max_timestamp != float("inf"):
        cmd.extend(["--max-timestamp", str(max_timestamp)])

    try:
        logging.info(
            f"Starting LRU simulation with cache size {format_size(cache_size_bytes)}"
        )
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        # Parse the output to extract miss ratio
        lines = result.stdout.strip().split("\n")
        miss_ratio = None

        for line in lines:
            if line.startswith("Miss ratio:"):
                miss_ratio_str = line.split(":")[1].strip()
                miss_ratio = float(miss_ratio_str)
                break

        if miss_ratio is None:
            logging.error(
                "Could not parse miss ratio from output for cache size "
                f"{format_size(cache_size_bytes)}"
            )
            logging.error(f"stdout: {result.stdout}")
            logging.error(f"stderr: {result.stderr}")
            return cache_size_bytes, float("nan")

        logging.info(
            f"Completed simulation for {format_size(cache_size_bytes)}: "
            f"miss_ratio={miss_ratio * 100:.2f}%"
        )
        return cache_size_bytes, miss_ratio

    except subprocess.CalledProcessError as e:
        logging.error(
            f"LRU simulation failed for cache size {format_size(cache_size_bytes)}"
        )
        logging.error(f"Command: {' '.join(cmd)}")
        logging.error(f"Return code: {e.returncode}")
        logging.error(f"stdout: {e.stdout}")
        logging.error(f"stderr: {e.stderr}")
        return cache_size_bytes, float("nan")


def generate_cache_sizes(
    min_size: int, max_size: int, num_points: int, scale: str = "log"
) -> List[int]:
    """
    Generate a list of cache sizes for MRC simulation.

    Args:
        min_size: Minimum cache size in bytes
        max_size: Maximum cache size in bytes
        num_points: Number of data points
        scale: "log" for logarithmic scaling or "linear" for linear scaling

    Returns:
        List of cache sizes in bytes
    """
    if scale == "log":
        log_min = np.log10(min_size)
        log_max = np.log10(max_size)
        log_sizes = np.linspace(log_min, log_max, num_points)
        sizes = [int(10**log_size) for log_size in log_sizes]
    else:
        sizes = [int(size) for size in np.linspace(min_size, max_size, num_points)]

    return sorted(list(set(sizes)))  # Remove duplicates and sort


def plot_mrc(
    cache_sizes: List[int], miss_ratios: List[float], output_path: Path
) -> None:
    """
    Plot the miss ratio curve.

    Args:
        cache_sizes: List of cache sizes in bytes
        miss_ratios: List of corresponding miss ratios
        output_path: Path to save the plot
    """
    # Convert cache sizes to GB for plotting
    cache_sizes_gb = [size / (1024**3) for size in cache_sizes]

    plt.figure(figsize=(10, 6))
    plt.plot(cache_sizes_gb, miss_ratios, "b-o", linewidth=2, markersize=6)
    plt.xlabel("Cache Size (GB)")
    plt.ylabel("Miss Ratio")
    plt.title("Miss Ratio Curve (MRC)")
    plt.grid(True, alpha=0.3)

    # Add text annotations for some points
    for i in range(0, len(cache_sizes_gb), max(1, len(cache_sizes_gb) // 5)):
        plt.annotate(
            f"{miss_ratios[i]:.3f}",
            (cache_sizes_gb[i], miss_ratios[i]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=8,
        )

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logging.info(f"MRC plot saved to {output_path}")


def save_results(
    cache_sizes: List[int], miss_ratios: List[float], output_path: Path
) -> None:
    """
    Save results to a JSON file.

    Args:
        cache_sizes: List of cache sizes in bytes
        miss_ratios: List of corresponding miss ratios
        output_path: Path to save the JSON file
    """
    results = {
        "cache_sizes_bytes": cache_sizes,
        "cache_sizes_gb": [size / (1024**3) for size in cache_sizes],
        "miss_ratios": miss_ratios,
    }

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    logging.info(f"Results saved to {output_path}")


def simulate_mrc(
    cache_image_path: Path,
    req_trace_path: Path,
    min_cache_size: int,
    max_cache_size: int,
    num_points: int,
    scale: str,
    num_processes: int,
    req_shard: int,
    output_dir: Path,
    max_line: int = float("inf"),
    max_timestamp: float = float("inf"),
) -> None:
    """
    Run MRC simulation with multiple cache sizes in parallel.

    Args:
        cache_image_path: Path to cache_image.csv
        req_trace_path: Path to req_trace.csv
        min_cache_size: Minimum cache size in bytes
        max_cache_size: Maximum cache size in bytes
        num_points: Number of data points for the MRC
        scale: Scaling method ("log" or "linear")
        num_processes: Number of parallel processes
        req_shard: Number of shards for request partitioning
        output_dir: Directory to save results
        max_line: Maximum line number to process from req_trace.csv
        max_timestamp: Maximum timestamp to process from req_trace.csv
    """
    # Generate cache sizes
    cache_sizes = generate_cache_sizes(
        min_cache_size, max_cache_size, num_points, scale
    )

    logging.info(
        f"Generated {len(cache_sizes)} cache sizes from "
        f"{format_size(min_cache_size)} to {format_size(max_cache_size)}"
    )
    logging.info(f"Cache sizes: {[format_size(size) for size in cache_sizes]}")

    # Find sim_lru.py path
    sim_lru_path = Path(__file__).parent / "sim_lru.py"
    if not sim_lru_path.exists():
        raise FileNotFoundError(f"sim_lru.py not found at {sim_lru_path}")

    # Prepare arguments for parallel execution
    args_list = [
        (
            cache_image_path,
            req_trace_path,
            cache_size,
            sim_lru_path,
            req_shard,
            max_line,
            max_timestamp,
        )
        for cache_size in cache_sizes
    ]

    # Run simulations in parallel
    logging.info(
        f"Running {len(cache_sizes)} LRU simulations with "
        f"{num_processes} parallel processes"
    )

    with mp.Pool(processes=num_processes) as pool:
        results = pool.map(run_lru_simulation, args_list)

    # Sort results by cache size and filter out failed runs
    results = [(size, ratio) for size, ratio in results if not np.isnan(ratio)]
    results.sort(key=lambda x: x[0])

    if not results:
        logging.error("All simulations failed, no results to plot")
        return

    cache_sizes_final, miss_ratios = zip(*results)
    cache_sizes_final = list(cache_sizes_final)
    miss_ratios = list(miss_ratios)

    logging.info(
        f"Successfully completed {len(results)} out of {len(cache_sizes)} simulations"
    )

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save results
    json_output = output_dir / "mrc_results.json"
    save_results(cache_sizes_final, miss_ratios, json_output)

    # Plot MRC
    plot_output = output_dir / "mrc_plot.png"
    plot_mrc(cache_sizes_final, miss_ratios, plot_output)

    # Print summary
    print("\n=== Miss Ratio Curve Results ===")
    print(
        "Cache Size Range: "
        f"{format_size(min(cache_sizes_final))} - {format_size(max(cache_sizes_final))}"
    )
    print(
        "Miss Ratio Range: "
        f"{min(miss_ratios) * 100:.2f}% - {max(miss_ratios) * 100:.2f}%"
    )
    print(f"Results saved to: {output_dir}")
    print("\nDetailed Results:")
    for size, ratio in zip(cache_sizes_final, miss_ratios):
        print(f"  {format_size(size):>12}: {ratio * 100:>5.2f}%")


def main():
    parser = argparse.ArgumentParser(
        description="Generate Miss Ratio Curve (MRC) using LRU simulation"
    )

    parser.add_argument(
        "cache_image_path", type=Path, help="Path to cache_image.csv file"
    )
    parser.add_argument("req_trace_path", type=Path, help="Path to req_trace.csv file")

    parser.add_argument(
        "--min-cache-size",
        type=int,
        default=64 * 1024 * 1024,  # 64MB
        help="Minimum cache size in bytes (default: 64MB)",
    )
    parser.add_argument(
        "--max-cache-size",
        type=int,
        default=4 * 1024 * 1024 * 1024,  # 4GB
        help="Maximum cache size in bytes (default: 4GB)",
    )
    parser.add_argument(
        "--num-points",
        type=int,
        default=16,
        help="Number of data points for the MRC (default: 16)",
    )
    parser.add_argument(
        "--scale",
        choices=["log", "linear"],
        default="linear",
        help="Scaling method for cache sizes (default: linear)",
    )
    parser.add_argument(
        "--num-processes",
        type=int,
        default=mp.cpu_count(),
        help=f"Number of parallel processes (default: {mp.cpu_count()})",
    )
    parser.add_argument(
        "--req-shard",
        type=int,
        default=1,
        help="Number of shards for request partitioning (default: 1)",
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
        "-o",
        "--output-dir",
        type=Path,
        required=True,
        help="Output directory for results (default: mrc_results)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    # Validate input files
    if not args.cache_image_path.exists():
        logging.error(f"Cache image file not found: {args.cache_image_path}")
        return 1

    if not args.req_trace_path.exists():
        logging.error(f"Request trace file not found: {args.req_trace_path}")
        return 1

    if args.min_cache_size >= args.max_cache_size:
        logging.error("Minimum cache size must be less than maximum cache size")
        return 1

    logging.info("Starting MRC simulation...")
    logging.info(f"Cache image: {args.cache_image_path}")
    logging.info(f"Request trace: {args.req_trace_path}")
    logging.info(
        "Cache size range: "
        f"{format_size(args.min_cache_size)} - {format_size(args.max_cache_size)}"
    )
    logging.info(f"Number of points: {args.num_points}")
    logging.info(f"Scaling: {args.scale}")
    logging.info(f"Parallel processes: {args.num_processes}")

    # Run MRC simulation
    simulate_mrc(
        args.cache_image_path,
        args.req_trace_path,
        args.min_cache_size,
        args.max_cache_size,
        args.num_points,
        args.scale,
        args.num_processes,
        args.req_shard,
        args.output_dir,
        args.max_line,
        args.max_timestamp,
    )

    return 0


if __name__ == "__main__":
    exit(main())
