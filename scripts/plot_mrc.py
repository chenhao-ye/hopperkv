"""
This script load data produced by `run.py` and plot them.
"""

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt


def load_data(data_dir: Path) -> Dict[int, Tuple[int, List[float], List[float]]]:
    views = {}
    for fname in os.listdir(data_dir):
        if not os.path.isfile(data_dir / fname):
            continue
        if not (fname.startswith("alloc_view@") and fname.endswith(".json")):
            continue
        ts = int(fname[11:-5])
        with open(data_dir / fname, "r") as f:
            data = json.load(f)
            views[ts] = [
                (
                    tenant_data["tid"],
                    tenant_data["ghost_ticks"],
                    tenant_data["ghost_miss_ratios"],
                )
                for tenant_data in data
            ]
    return views


def plot_mrc(view: List, fname: str):
    fig, ax = plt.subplots(nrows=1, ncols=1)
    fig.set_size_inches(5, 5)

    max_cache_size = 0

    for tid, ghost_ticks, ghost_miss_ratios in view:
        if ghost_ticks is None or ghost_miss_ratios is None:
            continue
        # unit to MB
        ghost_ticks = [t / 1024 / 1024 for t in ghost_ticks]
        max_cache_size = max(max_cache_size, ghost_ticks[-1])
        ax.plot(ghost_ticks, ghost_miss_ratios, label=f"t{tid}")

    # tick = 25 * 1024 * 1024
    # ticks = [tick * i for i in range(int(max_cache_size / tick) + 1)]

    # ax.set_xticks(ticks)
    # ax.set_xlim([0, max_cache_size * 1.05])
    ax.set_xlim(xmin=0)
    ax.set_ylim([0, 1])

    ax.set_xlabel("Cache size (MB)")
    ax.set_ylabel("Miss ratio")

    fig.legend()
    fig.set_tight_layout({"pad": 0, "w_pad": 0, "h_pad": 0})
    fig.savefig(fname)
    logging.info(f"Save MRC plot to `{fname}`")
    plt.close(fig)  # otherwise may get complaint too many figures open


def plot_ghost_ticks_index(view: List, fname: str):
    fig, ax = plt.subplots(nrows=1, ncols=1)
    fig.set_size_inches(5, 5)

    for tid, ghost_ticks, ghost_miss_ratios in view:
        if ghost_ticks is None or ghost_miss_ratios is None:
            continue
        # unit to MB
        ghost_ticks_mb = [t / 1024 / 1024 for t in ghost_ticks]
        indices = list(range(len(ghost_ticks_mb)))
        ax.plot(indices, ghost_ticks_mb, label=f"t{tid}")

    ax.set_xlim(xmin=0)
    ax.set_ylim(ymin=0)

    ax.set_xlabel("Ghost tick index")
    ax.set_ylabel("Cache size (MB)")

    fig.legend()
    fig.set_tight_layout({"pad": 0, "w_pad": 0, "h_pad": 0})
    fig.savefig(fname)
    logging.info(f"Save ghost ticks index plot to `{fname}`")
    plt.close(fig)  # otherwise may get complaint too many figures open


def main(data_dir: Path):
    views = load_data(data_dir)

    for ts, view in views.items():
        plot_mrc(view, data_dir / f"mrc@{ts}.png")
        plot_ghost_ticks_index(view, data_dir / f"ghost_ticks@{ts}.png")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir", help="directory for experiment data", type=Path)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    main(data_dir=args.data_dir)
