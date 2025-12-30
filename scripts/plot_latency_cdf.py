"""
This script load data produced by `run.py` and plot them.
"""

import argparse
import logging
import math
from pathlib import Path
from typing import List

import matplotlib.pyplot as plt
import pandas as pd


def load_data(data_dir: Path) -> pd.DataFrame:
    return pd.read_csv(data_dir / "data_summary.csv")


def get_sid_list(df: pd.DataFrame):
    return df["sid"].unique()


def get_tenant_latency(df: pd.DataFrame, sid: str, ts: int):
    tenant_df = df[(df["sid"] == sid) & (df["elapsed"] == ts)]

    def get_value(field):
        d_list = tenant_df[field].to_list()
        assert len(d_list) == 1, (
            f"Incorrect number of data: field={field}, sid={sid}, ts={ts}, tenant_df={tenant_df}"
        )
        return d_list[0]

    return (
        [
            get_value("lat_min"),
            get_value("p10"),
            get_value("p20"),
            get_value("p30"),
            get_value("p40"),
            get_value("p50"),
            get_value("p60"),
            get_value("p70"),
            get_value("p80"),
            get_value("p90"),
            get_value("p99"),
            get_value("p999"),
            get_value("lat_max"),
        ],
        [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99, 99.9, 100],
    )


def plot_latency(ts: int, df: pd.DataFrame, data_dir: Path):
    fig, ax = plt.subplots(nrows=1, ncols=1)
    fig.set_size_inches(5, 5)

    for sid in get_sid_list(df):
        x, y = get_tenant_latency(df, sid, ts)
        x = [latency_us / 1000 for latency_us in x]
        ax.plot(x, y, label=sid, marker="o")

    ax.set_ylim(ymin=0, ymax=101)
    ax.set_xlim(xmin=0)

    ax.set_xlabel("Latency (ms)")
    ax.set_ylabel("CDF (%)")
    ax.legend(loc="best")

    fig.set_tight_layout({"pad": 0, "w_pad": 0, "h_pad": 0})
    fig.savefig(data_dir / f"latency_cdf@{ts}.pdf")
    logging.info(f"Save latency CDF plot to `{data_dir / f'latency_cdf@{ts}.pdf'}`")


def plot_latency_logscale(ts: int, df: pd.DataFrame, data_dir: Path):
    fig, ax = plt.subplots(nrows=1, ncols=1)
    fig.set_size_inches(5, 5)

    def logscale_mapper(y_val):
        return -math.log10(100 - y_val)

    for sid in get_sid_list(df):
        x, y = get_tenant_latency(df, sid, ts)
        x = [latency_us / 1000 for latency_us in x[:-1]]
        y = [logscale_mapper(y_val) for y_val in y[:-1]]
        ax.plot(x, y, label=sid, marker="o")

    ax.set_ylim(ymin=logscale_mapper(0))
    ax.set_yticks(
        [
            logscale_mapper(50),
            logscale_mapper(90),
            logscale_mapper(99),
            logscale_mapper(99.9),
        ],
        ["p50", "p90", "p99", "p999"],
    )
    ax.set_xlim(xmin=0)

    ax.set_xlabel("Latency (ms)")
    ax.set_ylabel("CDF (%)")
    ax.legend(loc="best")

    fig.set_tight_layout({"pad": 0, "w_pad": 0, "h_pad": 0})
    fig.savefig(data_dir / f"latency_cdf_logscale@{ts}.pdf")
    logging.info(
        f"Save latency CDF (logscale) plot to `{data_dir / f'latency_cdf_logscale@{ts}.pdf'}`"
    )


def main(data_dir: Path, timestamps: List[int]):
    df = load_data(data_dir)

    for ts in timestamps:
        plot_latency(ts, df, data_dir)
        plot_latency_logscale(ts, df, data_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir", help="directory for experiment data", type=Path)
    parser.add_argument(
        "timestamps",
        help="timestamps to perform allocation in seconds, e.g., 5 10 15",
        nargs="+",
        type=int,
        default=[],
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    main(data_dir=args.data_dir, timestamps=args.timestamps)
