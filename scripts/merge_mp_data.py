import argparse
import json
import logging
import os
from pathlib import Path

import pandas as pd

from .merge_latency import extract_latency_distrib_within_window, merge_latency_hist


def load_perf(
    data_dir: Path, ts_min: int, ts_max: int, include_global=bool
) -> pd.DataFrame:
    df_results = None
    for policy in (
        ["base", "drf", "hare", "memshare", "global"]
        if include_global
        else ["base", "drf", "hare", "memshare"]
    ):
        data_subdir = data_dir / policy

        df_summary = pd.read_csv(data_subdir / "data_summary.csv")
        df_details = pd.read_csv(data_subdir / "data_details.csv")

        df_tput_filtered = df_summary[
            (df_summary["elapsed"] >= ts_min) & (df_summary["elapsed"] < ts_max)
        ][["sid", "tput"]]
        df_tput = df_tput_filtered.groupby("sid").mean()

        latency_dict = merge_latency_hist(df_details)
        df_lat = extract_latency_distrib_within_window(latency_dict, ts_min, ts_max)

        df_exper = df_lat.join(df_tput, on="sid")
        df_exper["policy"] = policy
        df_exper.set_index("policy", append=True, inplace=True, verify_integrity=True)
        if df_results is None:
            df_results = df_exper
        else:
            df_results = pd.concat([df_results, df_exper])
    return df_results


def load_alloc(data_dir: Path) -> pd.DataFrame:
    df_alloc = pd.read_csv(data_dir / "base" / "alloc.csv")
    # somehow alloc.csv has sid as int, not as `s{sid}` string
    df_alloc["sid"] = df_alloc["sid"].apply(lambda x: f"s{x}")
    df_alloc.set_index(["sid", "policy"], inplace=True)
    return df_alloc


def get_view_path(dir_path: Path) -> Path:
    ts_list = []
    for fname in os.listdir(dir_path):
        if not fname.startswith("alloc_view@") or not fname.endswith(".json"):
            continue
        ts = int(fname.split("alloc_view@")[1].split(".json")[0])
        ts_list.append(ts)
    ts_list.sort()
    assert len(ts_list) > 0, f"No view file found in {dir_path}"
    if len(ts_list) > 1:
        logging.warning(
            f"Multiple view files found in {dir_path}; swill use the last one"
        )
    ts = ts_list[-1]
    return dir_path / f"alloc_view@{ts}.json"


def load_miss_ratio(data_dir: Path) -> pd.DataFrame:
    mr_list = []
    for policy in ["base", "drf", "hare", "memshare"]:
        with open(get_view_path(data_dir / policy), "r") as f:
            for sid, s_view in enumerate(json.load(f)):
                assert sid == s_view["tid"]
                hit_cnt = s_view["epoch_stat"]["hit_cnt"]
                miss_cnt = s_view["epoch_stat"]["miss_cnt"]
                read_cnt = miss_cnt + hit_cnt
                miss_ratio = miss_cnt / read_cnt if read_cnt > 0 else None

                mr_list.append(
                    {"sid": f"s{sid}", "policy": policy, "miss_ratio": miss_ratio}
                )
    return pd.DataFrame.from_records(mr_list, index=["sid", "policy"])


def main(
    data_dir: Path, ts_min: int, ts_max: int, include_global: bool
) -> pd.DataFrame:
    df_results = load_perf(data_dir, ts_min, ts_max, include_global)
    df_alloc = load_alloc(data_dir)
    df_mr = load_miss_ratio(data_dir)
    df_results = df_results.join(df_alloc, on=["sid", "policy"], how="outer").join(
        df_mr, on=["sid", "policy"], how="outer"
    )
    df_results.to_csv(data_dir / "data.csv")
    return df_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir", help="Directory for experiment data", type=Path)
    parser.add_argument(
        "ts_min", help="Beginning timestamp for analysis (inclusive boundary)", type=int
    )
    parser.add_argument(
        "ts_max", help="End timestamp for analysis (exclusive boundary)", type=int
    )
    parser.add_argument(
        "--include_global",
        help="include a run of global pooling resource",
        action="store_true",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    args_dict = vars(args)
    main(**args_dict)
