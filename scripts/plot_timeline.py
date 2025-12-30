"""
This script load data produced by `run.py` and plot them.
"""

import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Rectangle

from .plot_style import (
    SINGLE_COLUMN_WIDTH,
    color_map,
    config_auto_ticks,
    decide_scale,
    labels_map,
    line_style_map,
    scale_str_map,
    zorder_map,
)
from .plot_util import build_fig_single_col, make_legend, save_fig

# only apply to this script
linewidth_map = {
    "base": 0.8,
    "drf": 0.8,
    "hare": 0.8,
    "memshare": 0.8,
    "global": 0.5,
}


def load_tput_data(
    data_dir: Path, policies: List[str], group: int, xmax: float = float("inf")
) -> Dict[str, pd.DataFrame]:
    df_dict = {}
    for policy in policies:
        df = pd.read_csv(data_dir / policy / "data_summary.csv")
        df = df[df["elapsed"] < xmax]
        df["elapsed_grouped"] = df["elapsed"].apply(
            lambda x: (x + group - 1) // group * group
        )
        df = df[["sid", "elapsed_grouped", "tput"]]
        df = df.groupby(["sid", "elapsed_grouped"]).mean().reset_index()
        df_dict[policy] = df
    return df_dict


def load_resrc_data(
    data_dir: Path, policies: List[str], xmax: float = float("inf")
) -> Dict[str, pd.DataFrame]:
    df_dict = {}
    for policy in policies:
        df = pd.read_csv(data_dir / policy / "alloc.csv")
        df = df[df["policy"] == policy]
        # this should be a "<" instead of "<=" because elapsed is 0-based index
        df = df[df["elapsed"] < xmax]
        df["sid"] = df["sid"].apply(lambda x: f"s{x}")
        df_dict[policy] = df
    return df_dict


def get_sid_list(df: pd.DataFrame):
    return df["sid"].unique()


def get_tenant_field(
    df: pd.DataFrame, sid: str, field: str, xfield: str = "elapsed_grouped"
):
    tenant_df = df[df["sid"] == sid]
    return tenant_df[xfield].to_list(), tenant_df[field].to_list()


def get_max_val(df_dict: Dict[str, pd.DataFrame], sid: str, field: str):
    return max(df[df["sid"] == sid][field].max() for df in df_dict.values())


def scaleup_one_datapoint(x, xscale):
    scale_factor = 60 if xscale == "min" else 1
    return x * scale_factor


def scale_one_datapoint(x, xscale):
    scale_factor = 60 if xscale == "min" else 1
    return x / scale_factor


def scale_data(x, xscale):
    scale_factor = 60 if xscale == "min" else 1
    return [x_val / scale_factor for x_val in x]


def config_time_axis(ax, xscale, xticks, show_label=True):
    if show_label:
        if xscale == "min":
            ax.set_xlabel("Time (minute)")
        else:
            ax.set_xlabel("Time (second)")

    if xticks is not None:
        ax.set_xticks([float(t) for t in xticks], xticks)
        ax.set_xlim([float(xticks[0]), float(xticks[-1])])
        ax.tick_params(axis="x", labelbottom=True)


def plot_tput(
    ax,
    df_dict: Dict[str, pd.DataFrame],
    sid: str,
    tenant_name: str,
    xscale: str,
    policies: List[str],
    dense_params: Tuple[float, float] | None,
):
    x_max = get_max_val(df_dict, sid, "elapsed_grouped")
    y_max = get_max_val(df_dict, sid, "tput")
    yscale, yscale_factor = decide_scale(y_max)
    yscale_str = scale_str_map[yscale] + " " if yscale else ""

    for p in policies:
        df = df_dict[p]
        x, y = get_tenant_field(df, sid, "tput")
        y = [tput / yscale_factor for tput in y]
        x = scale_data(x, xscale)
        ax.plot(
            x,
            y,
            line_style_map[p],
            color=color_map[p],
            label=p,
            zorder=zorder_map[p],
            linewidth=linewidth_map[p],
            # clip_on=False,
        )

    x_max = scale_one_datapoint(x_max, xscale)

    ax.set_xlim((0, x_max))  # default x-axis limits
    ax.set_ylabel(f"{tenant_name} tput ({yscale_str}req/s)")

    if dense_params:
        dense_threshold, dense_scale = dense_params
        dense_threshold /= yscale_factor
        print(
            f"Scale y-axis for {tenant_name} from {dense_threshold} ({yscale_str}req/s) by {dense_scale}"
        )
        ax.set_yscale(
            "function",
            functions=(
                lambda y_: np.array(
                    [
                        (
                            y
                            if y <= dense_threshold
                            else dense_threshold + ((y - dense_threshold) / dense_scale)
                        )
                        for y in y_
                    ]
                ),
                lambda y_: np.array(
                    [
                        (
                            y
                            if y <= dense_threshold
                            else dense_threshold + ((y - dense_threshold) * dense_scale)
                        )
                        for y in y_
                    ]
                ),
            ),
        )
        _, ymax = ax.get_ylim()
        ax.add_patch(
            Rectangle(
                (0, dense_threshold),
                x_max,
                ymax,
                facecolor="0.95",
                zorder=-10,
            )
        )
    config_auto_ticks(ax, "y", y_max / yscale_factor)


def plot_resrc(
    ax,
    df_dict: Dict[str, pd.DataFrame],
    sid: str,
    resrc: str,
    tenant_name: str,
    xscale: str,
    binary_scale: bool,
    policies: List[str],
):
    x_max = get_max_val(df_dict, sid, "elapsed")
    y_max = get_max_val(df_dict, sid, resrc)
    yscale, yscale_factor = decide_scale(y_max, binary_scale)
    yscale_str = " " + scale_str_map[yscale] if yscale else ""

    for p in policies:
        df = df_dict[p]
        x, y = get_tenant_field(df, sid, resrc, xfield="elapsed")
        y = [tput / yscale_factor for tput in y]
        if p == "base":
            # the base policy may only have only one data point, which does not show any lines
            # we add a dummy data point to make the line visible
            assert len(y) == 1
            ax.axhline(
                y[0],
                linestyle="-",
                color=color_map[p],
                label=p,
                linewidth=linewidth_map[p],
                zorder=zorder_map[p],
            )
            continue
        x = scale_data(x, xscale)
        ax.plot(
            x,
            y,
            "-",  # force to be solid line
            color=color_map[p],
            label=p,
            zorder=zorder_map[p],
            linewidth=linewidth_map[p],
            # clip_on=False,
        )

    x_max = scale_one_datapoint(x_max, xscale)

    ax.set_xlim((0, x_max))  # default x-axis limits
    config_auto_ticks(ax, "y", y_max / yscale_factor)
    ax.set_ylabel(f"{tenant_name} {labels_map[resrc]}{yscale_str}")


def plot_min_norm_tput(
    ax,
    df_dict: Dict[str, pd.DataFrame],
    sid_list: List[str],
    xscale: str,
    policies: List[str],
):
    # Get base policy data for normalization
    base_df = df_dict["base"]

    # Find common time points across all policies
    all_time_points = set()
    for policy in policies:
        df = df_dict[policy]
        for sid in sid_list:
            tenant_df = df[df["sid"] == sid]
            all_time_points.update(tenant_df["elapsed_grouped"].tolist())

    time_points = sorted(all_time_points)
    x_scaled = scale_data(time_points, xscale)

    # Calculate min normalized throughput for each policy at each time point
    for policy in policies:
        if policy == "base":
            # Base policy normalized throughput is always 1.0
            min_norm_tputs = [1.0] * len(time_points)
        else:
            policy_df = df_dict[policy]
            min_norm_tputs = []

            for time_point in time_points:
                norm_tputs = []
                for sid in sid_list:
                    # Get policy throughput at this time point
                    policy_tenant = policy_df[
                        (policy_df["sid"] == sid)
                        & (policy_df["elapsed_grouped"] == time_point)
                    ]

                    # Get base throughput at this time point
                    base_tenant = base_df[
                        (base_df["sid"] == sid)
                        & (base_df["elapsed_grouped"] == time_point)
                    ]

                    if not policy_tenant.empty and not base_tenant.empty:
                        policy_tput = policy_tenant["tput"].iloc[0]
                        base_tput = base_tenant["tput"].iloc[0]
                        if base_tput > 0:
                            norm_tputs.append(policy_tput / base_tput)

                if norm_tputs:
                    min_norm_tputs.append(min(norm_tputs))
                else:
                    min_norm_tputs.append(0.0)

        ax.plot(
            x_scaled,
            min_norm_tputs,
            line_style_map[policy],
            color=color_map[policy],
            label=policy,
            zorder=zorder_map[policy],
            linewidth=linewidth_map[policy],
        )

    x_max = max(x_scaled) if x_scaled else 0
    ax.set_xlim((0, x_max))
    ax.set_ylabel("Min norm tput")
    ax.set_yticks([0, 0.5, 1, 1.5, 2], ["0", "0.5", "1", "1.5", "2"])
    ax.set_ylim([0, 2])


def plot_markers(
    markers: List[str],
    axes: List[plt.Axes],
    xscale: str,
):
    t_calib = scale_one_datapoint(1, xscale)
    for markers_str in markers:
        ax_idx, remain = markers_str.split(":", 1)
        ax = axes[int(ax_idx)]
        for marker_str in remain.split(";"):
            marker_txt, ts = marker_str.strip().split("@")
            ts = float(ts)

            _, ymax = ax.get_ylim()
            ax.plot(
                ts - t_calib,
                ymax,
                marker=11,
                markerfacecolor="black",
                markeredgecolor="none",
                markersize=3,
                clip_on=False,
            )
            # ax.text(
            #     ts - t_calib,
            #     ymax,
            #     marker_txt,
            #     fontsize=4,
            #     ha="center",
            #     va="center",
            #     color="blue",
            # )

        # if len(idx_vline) == 1:
        #     vline_axes = axes
        #     vline = int(idx_vline[0])
        # else:
        #     idx, vline = idx_vline
        #     vline_axes = [axes[int(idx)]]
        #     vline = int(vline)

        # for ax in vline_axes:
        #     # timestamp is zero-based index (the first datapoints start at ts=0)
        #     # so we need to subtract 1 to match
        #     ax.axvline(
        #         vline - t_calib, color="black", linestyle=":", alpha=0.3, linewidth=0.5
        #     )
        #     _, ymax = ax.get_ylim()
        #     ax.plot(
        #         vline - t_calib,
        #         ymax,
        #         marker=11,
        #         markerfacecolor="0.2",
        #         markeredgecolor="none",
        #         markersize=3,
        #         clip_on=False,
        #     )


def make_tput_plot(
    data_dir: Path,
    markers: List[str],
    group: int,
    xscale: str,
    xticks: List[str] | None,
    include_global: bool,
    dense_yaxis: List[str] | None,
    skip_policies: List[str],
):
    policies = (
        ["base", "drf", "memshare", "global", "hare"]
        if include_global
        else ["base", "drf", "memshare", "hare"]
    )
    policies = [p for p in policies if p not in skip_policies]
    df_dict = load_tput_data(
        data_dir,
        policies,
        group,
        xmax=float("inf")
        if xticks is None
        else scaleup_one_datapoint(int(xticks[-1]), xscale),
    )
    sid_list = get_sid_list(df_dict["base"])

    # Create figure with extra subplot for min normalized throughput
    fig, axes = build_fig_single_col(len(sid_list) + 1, 1, hw_ratio=0.26, sharex=False)

    # Plot min normalized throughput at the top
    plot_min_norm_tput(axes[0], df_dict, sid_list, xscale, policies)

    # Use remaining axes for individual tenant plots
    tenant_axes = axes[1:]

    dense_params_list = [None] * len(sid_list)
    for dense_yaxis_str in dense_yaxis or []:
        idx, threshold, scale = dense_yaxis_str.split(":")
        idx = int(idx)
        threshold = float(threshold)
        scale = float(scale)
        dense_params_list[idx] = (threshold, scale)

    for sid, ax, dense_params in zip(sid_list, tenant_axes, dense_params_list):
        plot_tput(ax, df_dict, sid, labels_map[sid], xscale, policies, dense_params)

    for ax in axes:
        config_time_axis(ax, xscale, xticks, show_label=ax is axes[-1])

    plot_markers(markers, tenant_axes, xscale)

    save_fig(fig, data_dir / "tput_timeline.pdf")

    make_legend(
        policies,
        data_dir=data_dir,
        width=SINGLE_COLUMN_WIDTH,
        line_markers=[line_style_map[p] for p in policies],
        handlelength=1.9,
        handletextpad=0.45,
    )


def make_resrc_plot(
    data_dir: Path,
    markers: List[str],
    xscale: str,
    xticks: List[str] | None,
    skip_policies: List[str],
):
    policies = ["base", "drf", "hare", "memshare"]
    policies = [p for p in policies if p not in skip_policies]
    df_dict = load_resrc_data(
        data_dir,
        policies,
        xmax=float("inf")
        if xticks is None
        else scaleup_one_datapoint(int(xticks[-1]), xscale),
    )
    sid_list = get_sid_list(df_dict["base"])

    for resrc, binary_scale in [
        ("cache_size", True),
        ("db_rcu", False),
        ("db_wcu", False),
        ("net_bw", False),
    ]:
        fig, axes = build_fig_single_col(len(sid_list), 1, hw_ratio=0.4, sharex=True)

        for sid, ax in zip(sid_list, axes):
            plot_resrc(
                ax, df_dict, sid, resrc, labels_map[sid], xscale, binary_scale, policies
            )
            config_time_axis(ax, xscale, xticks, show_label=ax is axes[-1])
        plot_markers(markers, axes, xscale)
        save_fig(fig, data_dir / f"{resrc}_timeline.pdf")


def main(
    data_dir: Path,
    markers: List[str],
    group: int,
    xscale: str,
    xticks: List[str] | None,
    include_global: bool,
    dense_yaxis: List[str] | None,
    skip_policies: List[str],
):
    make_tput_plot(
        data_dir,
        markers,
        group,
        xscale,
        xticks,
        include_global,
        dense_yaxis,
        skip_policies,
    )
    make_resrc_plot(data_dir, markers, xscale, xticks, skip_policies)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir", help="directory for experiment data", type=Path)
    parser.add_argument(
        "-m",
        "--markers",
        help="timestamps to draw vertical lines; format: `ax_idx:xxx@ts;yyy@ts2[...]`",
        nargs="+",
        type=str,
        default=[],
    )
    parser.add_argument(
        "-g",
        "--group",
        help="group a few seconds' average throughput into one data point",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--xscale",
        help="time axis scale",
        type=str,
        choices=["min", "sec"],
    )
    parser.add_argument(
        "--xticks",
        help="time axis ticks",
        type=str,
        nargs="+",
    )
    parser.add_argument(
        "--include_global",
        help="include a run of global pooling resource",
        action="store_true",
    )
    parser.add_argument(
        "--dense_yaxis",
        help="make the tput axis dense; each format as <axis_idx>:<threshold>:<scale>",
        type=str,
        nargs="*",
    )
    parser.add_argument(
        "--skip_policies",
        help="skip a set of polices",
        choices=["base", "drf", "hare", "memshare", "global"],
        nargs="*",
        default=[],
        required=False,
    )
    args = parser.parse_args()
    main(**vars(args))
