import argparse
import json
import logging
import os
from pathlib import Path
from typing import List

from hopperkv.alloc.engine import MissRatioCurve


def format_sizes(sizes: List[int]) -> List[str]:
    scale = 1
    scale_suffix = ""
    max_size = min(sizes)
    if max_size > 1024 * 1024 * 1024:
        scale = 1024 * 1024 * 1024
        scale_suffix = "G"
    elif max_size > 1024 * 1024:
        scale = 1024 * 1024
        scale_suffix = "M"
    elif max_size > 1024:
        scale = 1024
        scale_suffix = "K"
    return [f"{size / scale:.1f}{scale_suffix}" for size in sizes]


def analyze_stats(data_dir: str):
    ts_list = []
    for fname in os.listdir(data_dir):
        stat_path = f"{data_dir}/{fname}"
        if not (
            os.path.isfile(stat_path)
            and fname.startswith("alloc_stats@")
            and fname.endswith(".json")
        ):
            continue
        ts = fname[len("alloc_stats@") : -len(".json")]
        view_path = f"{data_dir}/alloc_view@{ts}.json"
        if os.path.isfile(f"{data_dir}/alloc_view@{ts}.json"):
            ts_list.append(ts)
        else:
            logging.warning(f"Detect {fname} but not alloc_view@{ts}.json")

    ts_list.sort(key=lambda ts: int(ts))

    def print_ratio_long(
        field: str,
        numerator: float,
        denominator: float,
        marker_maker=lambda _: "",
        format_dense=False,
    ):
        ratio = numerator / denominator if denominator != 0 else float("inf")
        if format_dense:
            numerator_str, denominator_str = format_sizes([numerator, denominator])
        else:
            numerator_str, denominator_str = (f"{numerator:.0f}", f"{denominator:.0f}")
        print(
            f"{marker_maker(ratio):>2}  {f'{field}:':<15} {ratio * 100:>5.1f}% "
            f" =  {numerator_str:>7} / {denominator_str:>7}"
        )

    def print_ratio_short(
        field: str,
        ratio: float,
        marker: str = "",
    ):
        print(f"{marker:>2}  {f'{field}:':<15} {ratio * 100:>5.1f}%")

    for ts in ts_list:
        fname = f"alloc_stats@{ts}.json"
        stat_path = f"{data_dir}/{fname}"
        view_path = f"{data_dir}/alloc_view@{ts}.json"
        print(f"### Analyze {fname}\n")
        with open(stat_path, "r") as f_stat, open(view_path, "r") as f_view:
            stat_data = json.load(f_stat)
            view_data = json.load(f_view)
            for sid, view in enumerate(view_data):
                print(f"#### Server {sid}")
                hit_cnt = view["epoch_stat"]["hit_cnt"]
                miss_cnt = view["epoch_stat"]["miss_cnt"]
                req_cnt = view["epoch_stat"]["req_cnt"]
                read_cnt = hit_cnt + miss_cnt
                write_cnt = req_cnt - read_cnt
                miss_ratio = miss_cnt / read_cnt
                print_ratio_long("write_ratio", write_cnt, req_cnt)
                print_ratio_long("miss_ratio", miss_cnt, read_cnt)

                cache_alloc, rcu_alloc, wcu_alloc, net_alloc = stat_data[f"{sid}"][
                    "HOPPER.RESRC"
                ]

                ghost_ticks = view["ghost_ticks"]
                ghost_miss_ratios = view["ghost_miss_ratios"]
                mrc = MissRatioCurve(ghost_ticks, ghost_miss_ratios)
                pred_miss_ratio = mrc.get_miss_ratio(cache_alloc)
                print_ratio_short(
                    "gh_miss_ratio",
                    pred_miss_ratio,
                    marker="x" if abs(miss_ratio - pred_miss_ratio) >= 0.05 else " ",
                )

                cache_used = stat_data[f"{sid}"]["MEMORY_STATS"]["total.allocated"]
                print_ratio_long(
                    "cache_util",
                    cache_used,
                    cache_alloc,
                    marker_maker=lambda cache_util: (
                        "x" if cache_util < 0.95 else "?" if cache_util > 1.05 else ""
                    ),
                    format_dense=True,
                )

                duration = view["epoch_stat"]["duration"]
                rcu_used = view["epoch_stat"]["db_rcu_consump"]
                wcu_used = view["epoch_stat"]["db_wcu_consump"]
                net_used = view["epoch_stat"]["net_bw_consump"]
                rcu_alloc_total = rcu_alloc * duration
                wcu_alloc_total = wcu_alloc * duration
                net_alloc_total = net_alloc * duration
                # use "<=" instead of "<" to capture zero allocation cases
                all_low_util = (
                    rcu_used <= rcu_alloc_total * 0.95
                    and wcu_used <= wcu_alloc_total * 0.95
                    and net_used <= net_alloc_total * 0.95
                )

                def mm(util) -> str:
                    return (
                        "?"
                        if util > 1.05 and util != float("inf")
                        else "x"
                        if all_low_util
                        else "*"
                        if util >= 0.95
                        else ""
                    )

                print_ratio_long("rcu_util", rcu_used, rcu_alloc_total, mm)
                print_ratio_long("wcu_util", wcu_used, wcu_alloc_total, mm)
                print_ratio_long("net_util", net_used, net_alloc_total, mm, True)
            print()


def main(data_dir: Path):
    print(f"## Data Analysis for {data_dir}\n")
    analyze_stats(data_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir", help="directory for experiment data", type=Path)
    args = parser.parse_args()

    main(data_dir=args.data_dir)
