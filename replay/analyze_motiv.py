"""
Motivation Analysis for Miss Ratio Curves (MRC)

Reads MRC results from multiple subdirectories and plots motivation curves,
where motivation is defined as -miss_ratio_derivative / miss_ratio.
"""

import argparse
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np


def load_mrc_results(json_path: Path) -> Tuple[List[float], List[float]]:
    """
    Load MRC results from a JSON file.

    Args:
        json_path: Path to mrc_results.json file

    Returns:
        Tuple of (cache_sizes_gb, miss_ratios)
    """
    with open(json_path, "r") as f:
        data = json.load(f)

    return data["cache_sizes_gb"], data["miss_ratios"]


def compute_motivation(
    cache_sizes_gb: List[float], miss_ratios: List[float]
) -> Tuple[List[float], List[float]]:
    """
    Compute motivation curve from miss ratio curve.

    Motivation is defined as -miss_ratio_derivative / miss_ratio
    Gradient is computed manually: for cache_size=x, take miss ratio at x-delta and x+delta,
    then divide by 2*delta (ignoring endpoints).

    Args:
        cache_sizes_gb: Cache sizes in GB
        miss_ratios: Corresponding miss ratios

    Returns:
        Tuple of (cache_sizes_interior, motivation_values)
    """
    if len(cache_sizes_gb) < 3 or len(miss_ratios) < 3:
        return [], []

    cache_sizes = np.array(cache_sizes_gb)
    miss_ratios = np.array(miss_ratios)

    # Compute manual gradient: (f(x+delta) - f(x-delta)) / (2*delta)
    # Only for interior points (ignore endpoints)
    interior_indices = range(1, len(cache_sizes) - 1)
    interior_cache_sizes = []
    motivation_values = []

    for i in interior_indices:
        x_minus = cache_sizes[i - 1]
        x_plus = cache_sizes[i + 1]
        y_minus = miss_ratios[i - 1]
        y_plus = miss_ratios[i + 1]
        y_center = miss_ratios[i]

        # Manual gradient calculation
        delta = (x_plus - x_minus) / 2
        if delta > 0:
            derivative = (y_plus - y_minus) / (2 * delta)

            # Compute motivation = -derivative / miss_ratio
            if y_center > 0:
                motivation = -derivative / y_center
                interior_cache_sizes.append(cache_sizes[i])
                motivation_values.append(motivation)

    return interior_cache_sizes, motivation_values


def plot_motivation_curves(
    results: Dict[str, Tuple[List[float], List[float]]], output_path: Path
) -> None:
    """
    Plot motivation curves for all datasets on the same figure.

    Args:
        results: Dictionary mapping dataset names to (cache_sizes, motivation) tuples
        output_path: Path to save the plot
    """
    plt.figure(figsize=(12, 8))

    # Use a colormap with enough distinct colors
    colors = plt.cm.tab20(np.linspace(0, 1, len(results)))

    # Plot all curves on the same figure
    for i, (dataset_name, (cache_sizes, motivation)) in enumerate(results.items()):
        if len(cache_sizes) > 0:
            plt.plot(
                cache_sizes,
                motivation,
                "o-",
                label=dataset_name,
                color=colors[i],
                linewidth=2,
                markersize=4,
            )

    plt.xlabel("Cache Size (GB)")
    plt.ylabel("Motivation (-derivative/miss_ratio)")
    plt.title("Motivation Curves for Different Datasets")
    plt.grid(True, alpha=0.3)
    plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")

    # Set y-axis to log scale if there are large variations
    y_values = [
        val for _, (_, motivation) in results.items() for val in motivation if val > 0
    ]
    if y_values and len(y_values) > 0 and max(y_values) / min(y_values) > 100:
        plt.yscale("log")
        plt.ylabel("Motivation (log scale)")

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    logging.info(f"Motivation plot saved to {output_path}")


def save_motivation_results(
    results: Dict[str, Tuple[List[float], List[float]]], output_path: Path
) -> None:
    """
    Save motivation results to a JSON file.

    Args:
        results: Dictionary mapping dataset names to (cache_sizes, motivation) tuples
        output_path: Path to save the JSON file
    """
    output_data = {}

    for dataset_name, (cache_sizes, motivation) in results.items():
        output_data[dataset_name] = {
            "cache_sizes_gb": cache_sizes,
            "motivation": motivation,
        }

    with open(output_path, "w") as f:
        json.dump(output_data, f, indent=2)

    logging.info(f"Motivation results saved to {output_path}")


def analyze_motivation(input_dir: Path, output_dir: Path) -> None:
    """
    Analyze motivation curves from MRC results in subdirectories.

    Args:
        input_dir: Directory containing subdirectories with mrc_results.json files
        output_dir: Directory to save analysis results
    """
    # Find all subdirectories with mrc_results.json
    mrc_files = []
    for subdir in input_dir.iterdir():
        if subdir.is_dir():
            mrc_file = subdir / "mrc_results.json"
            if mrc_file.exists():
                mrc_files.append((subdir.name, mrc_file))

    if not mrc_files:
        logging.error(
            f"No mrc_results.json files found in subdirectories of {input_dir}"
        )
        return

    logging.info(f"Found {len(mrc_files)} datasets: {[name for name, _ in mrc_files]}")

    # Load and process each dataset
    results = {}

    for dataset_name, mrc_file in mrc_files:
        try:
            cache_sizes_gb, miss_ratios = load_mrc_results(mrc_file)
            cache_sizes_motiv, motivation = compute_motivation(
                cache_sizes_gb, miss_ratios
            )

            if len(motivation) > 0:
                results[dataset_name] = (cache_sizes_motiv, motivation)
                logging.info(
                    f"Processed {dataset_name}: {len(motivation)} motivation points"
                )
            else:
                logging.warning(f"No valid motivation data for {dataset_name}")

        except Exception as e:
            logging.error(f"Failed to process {dataset_name}: {e}")
            continue

    if not results:
        logging.error("No valid datasets processed")
        return

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Plot motivation curves
    plot_output = output_dir / "motivation_curves.png"
    plot_motivation_curves(results, plot_output)

    # Save results
    json_output = output_dir / "motivation_results.json"
    save_motivation_results(results, json_output)

    # Print summary
    print("\n=== Motivation Analysis Results ===")
    print(f"Datasets processed: {len(results)}")
    print(f"Results saved to: {output_dir}")
    print("\nDataset Summary:")

    for dataset_name, (_, motivation) in results.items():
        if len(motivation) > 0:
            print(
                f"  {dataset_name:>15}: {len(motivation):>3} points, "
                f"motivation range: {min(motivation):>8.3f} - {max(motivation):>8.3f}"
            )


def main():
    parser = argparse.ArgumentParser(
        description="Analyze motivation curves from MRC results"
    )

    parser.add_argument(
        "input_dir",
        type=Path,
        help="Directory containing subdirectories with mrc_results.json files",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default="motivation_analysis",
        help="Output directory for analysis results (default: motivation_analysis)",
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

    # Validate input directory
    if not args.input_dir.exists():
        logging.error(f"Input directory not found: {args.input_dir}")
        return 1

    if not args.input_dir.is_dir():
        logging.error(f"Input path is not a directory: {args.input_dir}")
        return 1

    logging.info(f"Analyzing motivation curves from: {args.input_dir}")
    logging.info(f"Output directory: {args.output_dir}")

    # Run analysis
    analyze_motivation(args.input_dir, args.output_dir)

    return 0


if __name__ == "__main__":
    exit(main())
