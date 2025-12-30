import csv
import json
import logging
import os

import pandas as pd


def df_col_to_int(df, col):
    df[col] = df[col].astype(int)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    data_dir = "results/profile_mrc"

    details = []
    summary = []

    summary_cols = [
        "key_size",
        "val_size",
        "num_keys",
        "keys.count",
        "dataset.bytes",
        "total.allocated",
        "startup.allocated",
        "overhead.total",
        "peak.allocated",
    ]

    for fname in os.listdir(data_dir):
        stat_path = f"{data_dir}/{fname}/stats.json"
        if os.path.isfile(stat_path):
            with open(stat_path, "r") as f_stats:
                mem_stats = json.load(f_stats)
            details.append(mem_stats)
            summary.append({k: mem_stats[k] for k in summary_cols})

    if not details:
        logging.warning(f"No data found in {data_dir}")
        exit(1)

    with open(f"{data_dir}/mem_details.csv", "w") as f_csv:
        writer = csv.DictWriter(f_csv, fieldnames=details[0].keys())
        writer.writeheader()
        writer.writerows(details)

    df_summary = pd.DataFrame.from_dict(summary)

    df_summary["bytes_per_key"] = df_summary["dataset.bytes"] / df_summary["keys.count"]
    df_col_to_int(df_summary, "bytes_per_key")

    df_summary["bytes_per_key2"] = (
        df_summary["total.allocated"] - df_summary["startup.allocated"]
    ) / df_summary["keys.count"]
    df_col_to_int(df_summary, "bytes_per_key2")

    df_summary["bytes_per_key3"] = (
        df_summary["total.allocated"] - df_summary["startup.allocated"] - 150000
    ) / df_summary["keys.count"]
    df_col_to_int(df_summary, "bytes_per_key3")

    df_summary["overhead_per_key"] = (
        df_summary["overhead.total"] - df_summary["startup.allocated"]
    ) / df_summary["keys.count"]
    df_col_to_int(df_summary, "overhead_per_key")

    df_summary.sort_values(["key_size", "val_size", "num_keys"], inplace=True)

    df_summary.to_csv(f"{data_dir}/mem_summary.csv", index=False, float_format="%.3f")
    logging.info(f"Saved summary to {data_dir}/mem_summary.csv")
