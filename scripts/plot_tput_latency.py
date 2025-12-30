"""
This script load data produced by `run.py` and plot them.
"""

import argparse
import logging
import pathlib

import matplotlib.pyplot as plt
import pandas as pd


def load_data(data_dir: pathlib.Path) -> pd.DataFrame:
    return pd.read_csv(data_dir / "data_summary.csv")


def get_sid_list(df: pd.DataFrame):
    return df["sid"].unique()


def get_tenant_field(df: pd.DataFrame, sid: str, field: str):
    tenant_df = df[df["sid"] == sid]
    return tenant_df["elapsed"].to_list(), tenant_df[field].to_list()


def plot_tput(df: pd.DataFrame, data_dir: pathlib.Path):
    fig, ax = plt.subplots(nrows=1, ncols=1)
    fig.set_size_inches(5, 5)

    xmax = 0

    for sid in get_sid_list(df):
        x, y = get_tenant_field(df, sid, "tput")
        y = [tput / 1000 for tput in y]
        ax.plot(x, y, label=sid)
        xmax = max(xmax, max(x))

    ax.set_ylim(ymin=0)
    ax.set_xlim([0, xmax])

    ax.set_xlabel("Time (second)")
    ax.set_ylabel("Throughput (K req/s)")

    fig.legend()
    fig.set_tight_layout({"pad": 0, "w_pad": 0, "h_pad": 0})
    fig.savefig(data_dir / "tput.pdf")
    logging.info(f"Save tput plot to `{data_dir}/tput.pdf`")


def plot_latency(field: str, df: pd.DataFrame, data_dir: pathlib.Path):
    fig, ax = plt.subplots(nrows=1, ncols=1)
    fig.set_size_inches(5, 5)

    for sid in get_sid_list(df):
        x, y = get_tenant_field(df, sid, field)
        y = [latency_us / 1000 for latency_us in y]
        ax.plot(x, y, label=sid)

    ax.set_ylim(ymin=0)

    ax.set_xlabel("Time (second)")
    ax.set_ylabel(f"Latency {field} (ms)")

    fig.legend()
    fig.set_tight_layout({"pad": 0, "w_pad": 0, "h_pad": 0})
    fig.savefig(data_dir / f"{field}.pdf")
    logging.info(f"Save {field} latency plot to `{data_dir / field}.pdf`")


def main(data_dir: str):
    data_dir = pathlib.Path(data_dir)
    df = load_data(data_dir)

    plot_tput(df, data_dir)
    plot_latency("lat_mean", df, data_dir)
    plot_latency("p50", df, data_dir)
    plot_latency("p90", df, data_dir)
    plot_latency("p99", df, data_dir)
    plot_latency("p999", df, data_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir", help="directory for experiment data", type=str)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    main(data_dir=args.data_dir)
