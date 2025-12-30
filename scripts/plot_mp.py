import argparse
from pathlib import Path
from typing import List

import matplotlib
import matplotlib.lines as mlines
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Rectangle

from .plot_style import (
    DOUBLE_COLUMN_WIDTH,
    LINEWIDTH,
    SINGLE_COLUMN_WIDTH,
    color_map,
    decide_scale,
    hatch_map,
    labels_map,
    line_style_map,
    scale_factor_map,
)
from .plot_util import build_fig, save_fig


def load_data(data_dir: Path) -> pd.DataFrame:
    return pd.read_csv(data_dir / "data.csv")


def get_sid_list(df: pd.DataFrame):
    return sorted(df["sid"].unique(), key=lambda x: int(x[1:]))


def get_datapoint(df: pd.DataFrame, sid: str, policy: str, field: str):
    df_filter = df[(df["sid"] == sid) & (df["policy"] == policy)][field]
    assert df_filter.shape[0] == 1, (
        "Unexpected number of matching data points: "
        f"sid={sid}, policy={policy}, field={field}, df_filter={df_filter}, df=\n{df}"
    )
    return df_filter.to_list()[0]


label_color_map = {
    "drf": "black",
    "hare": "white",
    "memshare": "black",
    "global": "black",
}


def plot_tput(
    ax,
    df: pd.DataFrame,
    s_list: List[str],
    yscale: str | None,
    tenants: List[str] | None,
    include_global: bool,
    yticks: List[str] | None,
    dense_threshold: float,
    dense_scale: float,
    sep_legend: bool,
    highlight: str | None,
    bar_width=0.23,
):
    policy_list = (
        ["drf", "memshare", "global", "hare"]
        if include_global
        else ["drf", "memshare", "hare"]
    )
    x_base = list(range(len(s_list)))
    y_base = [get_datapoint(df, sid, "base", "tput") for sid in s_list]
    if yscale is None:
        yscale, yscale_factor = decide_scale(max(y_base))
    else:
        yscale_factor = scale_factor_map[yscale]
    if tenants is None:
        # add space left-padding for more visual balance
        tenants = [" " * len(str(sid)) + labels_map[sid] for sid in s_list]
    else:
        assert len(tenants) == len(s_list)
    print(f"Use scale {yscale} for absolute throughput")
    y_norm_max = 0
    truncated_bars = []
    for offset, p in enumerate(policy_list):
        x = [i + offset * bar_width for i in x_base]
        y = [get_datapoint(df, sid, p, "tput") for sid in s_list]
        y_norm = [i / j for i, j in zip(y, y_base)]
        y_norm_max = max(y_norm_max, max(y_norm))

        # Check for truncation and store positions
        if yticks is not None:
            max_ytick = abs(float(yticks[-1]))
            for i, (x_pos, y_val) in enumerate(zip(x, y_norm)):
                if y_val > max_ytick:
                    truncated_bars.append((x_pos, max_ytick))

        ax.bar(
            x,
            y_norm,
            width=bar_width,
            color=color_map[p],
            label=labels_map[p],
            edgecolor="white",
            linewidth=0,
            # hatch=[hatch_map.get(sid) for sid in s_list],
        )
        y_scaled = [tput / yscale_factor for tput in y]
        for i, (x_pos, tput) in enumerate(zip(x, y_scaled)):
            ax.text(
                x_pos + 0.01,
                0.23,
                f"{tput:>4.1f}",
                rotation="vertical",
                fontsize=4.3,
                color=label_color_map[p],
                ha="center",
                va="center",
            )

    ax.set_xticks(
        [i + bar_width * (len(policy_list) - 1) / 2 for i in x_base],
        tenants,
    )

    # Make highlighted tenant labels bold for tenants where specified policy has best tput
    if highlight is not None:
        # Find tenants where the specified policy has the best throughput
        best_policy_indices = []
        for idx, sid in enumerate(s_list):
            policy_tputs = {}
            for p in policy_list:
                policy_tputs[p] = get_datapoint(df, sid, p, "tput")
            best_policy = max(policy_tputs, key=policy_tputs.get)
            if best_policy == highlight:
                best_policy_indices.append(idx)

        for tick, idx in zip(ax.get_xticklabels(), range(len(s_list))):
            if idx in best_policy_indices:
                tick.set_color("blue")
                tick.set_weight("bold")
    xmin = x_base[0] + bar_width * (len(policy_list) - 1) / 2 - 0.5
    xmax = x_base[-1] + bar_width * (len(policy_list) - 1) / 2 + 0.5
    ax.set_xlim([xmin, xmax])

    ax.axhline(1, color=color_map["base"], linestyle=":", linewidth=0.5, zorder=1)
    ax.set_ylabel("Normalized tput")

    if yticks is None:
        yticks = [i * 50 for i in range(int(y_norm_max // 50 + 2))]

    if y_norm_max > dense_threshold:
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
        ax.add_patch(
            Rectangle(
                (xmin, dense_threshold),
                xmax - xmin,
                abs(float(yticks[-1])),
                facecolor="0.95",
                zorder=-10,
            )
        )

    ax.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(0.5))
    ax.set_yticks(
        [abs(float(t)) for t in yticks],
        [t if float(t) >= 0 else "" for t in yticks],
    )
    ax.set_ylim(abs(float(yticks[0])), abs(float(yticks[-1])))

    # Add "//" markers for truncated bars
    for x_pos, y_pos in truncated_bars:
        ax.text(
            x_pos + 0.01,
            y_pos - 0.1,
            "//",
            fontsize=4,
            color="black",
            ha="center",
            va="center",
            weight="bold",
            zorder=10,
        )

    if not sep_legend:
        # Create legend including base line
        legend_handles = []
        legend_labels = []
        base_line = mlines.Line2D(
            [],
            [],
            color=color_map["base"],
            linestyle=line_style_map["base"],
            linewidth=LINEWIDTH,
        )
        legend_handles.append(base_line)
        legend_labels.append(labels_map["base"])

        # Add policy bars
        for p in policy_list:
            rect = Rectangle((0, 0), 1, 1, facecolor=color_map[p])
            legend_handles.append(rect)
            legend_labels.append(labels_map[p])

        ax.legend(
            legend_handles,
            legend_labels,
            loc="upper center",
            bbox_to_anchor=(0.5, 1.135),
            ncol=len(policy_list) + 1,
            borderpad=0.05,
            frameon=False,
        )


def plot_resrc(
    ax,
    df: pd.DataFrame,
    s_list: List[str],
    tenants: List[str] | None,
    bar_width=0.27,
):
    ax.spines[["left", "bottom"]].set_visible(False)
    resrc_ratios = {}
    for resrc in ["db_wcu", "db_rcu", "net_bw", "cache_size"]:
        for p in ["hare", "memshare", "drf", "base"]:
            resrc_list = [get_datapoint(df, sid, p, resrc) for sid in s_list]
            total = sum(resrc_list)
            for sid, r in zip(s_list, resrc_list):
                resrc_ratios[sid, resrc, p] = r / total
    x_begin = [0] * 12
    y = []
    for major_offset in range(4):
        for minor_offset in range(3):
            y.append(major_offset + minor_offset * bar_width)
    for sid in s_list:
        x_len = []
        for resrc in ["db_wcu", "db_rcu", "net_bw", "cache_size"]:
            for p in ["hare", "memshare", "drf", "base"]:
                x_len.append(resrc_ratios[sid, resrc, p])
        ax.barh(
            y,
            x_len,
            height=bar_width,
            left=x_begin,
            color=[color_map[p] for p in ["hare", "memshare", "drf", "base"]] * 4,
            edgecolor="white",
            hatch=hatch_map.get(sid),
        )
        x_begin = [i + j for i, j in zip(x_begin, x_len)]
    ax.set_yticks(
        [i + bar_width for i in range(4)], ["DB write", "DB read", "Network", "Cache"]
    )
    ax.tick_params(axis="both", which="both", length=0)
    ax.set_xlim([0, 1])
    ax.set_ylim([-bar_width, 3 + bar_width * 2.5])
    ax.set_xticks([])


def get_cdf(data: List[float]):
    x = sorted(data)
    y = [i / (len(data) - 1) for i in range(len(data))]
    return x, y


def plot_cdf(
    ax,
    df: pd.DataFrame,
    s_list: List[str],
    include_global: bool,
    xticks: List[str] | None,
    dense_threshold: float,
    dense_scale: float,
    sep_legend: bool,
):
    policy_list = (
        ["drf", "memshare", "global", "hare"]
        if include_global
        else ["drf", "memshare", "hare"]
    )
    tput_base = [get_datapoint(df, sid, "base", "tput") for sid in s_list]
    # draw base line
    x, y = get_cdf([1 for _ in range(len(tput_base))])
    ax.plot(
        x,
        y,
        color=color_map["base"],
        linestyle=line_style_map["base"],
        linewidth=LINEWIDTH,
        label=labels_map["base"],
    )

    tput_norm_max = 0
    for p in policy_list:
        tput = [get_datapoint(df, sid, p, "tput") for sid in s_list]
        tput_norm = [i / j for i, j in zip(tput, tput_base)]
        tput_norm_max = max(tput_norm_max, max(tput_norm))
        x, y = get_cdf(tput_norm)
        ax.plot(
            x,
            y,
            color=color_map[p],
            linestyle=line_style_map[p],
            linewidth=LINEWIDTH,
            label=labels_map[p],
        )

    if xticks is None:
        xticks = [i for i in range(int(tput_norm_max + 1))]

    if tput_norm_max > dense_threshold:
        ax.set_xscale(
            "function",
            functions=(
                # lambda y: y,
                # lambda y: y,
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
        ax.add_patch(
            Rectangle(
                (dense_threshold, 0),
                abs(float(xticks[-1])),
                1,
                facecolor="0.95",
                zorder=-10,
            )
        )

    ax.set_xticks(
        [abs(float(t)) for t in xticks],
        [t if float(t) >= 0 else "" for t in xticks],
    )
    ax.xaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(0.5))
    ax.set_xlim(abs(float(xticks[0])), abs(float(xticks[-1])))
    ax.set_xlabel("Normalized Throughput")
    ax.tick_params(axis="y")

    yticks = ["0", ".2", ".4", ".6", ".8", "1"]
    ax.set_yticks([float(t) for t in yticks], yticks)
    ax.set_ylim(0, 1)
    ax.set_ylabel("CDF")
    if not sep_legend:
        legends = ax.legend(
            loc="upper center",
            ncol=1,
            borderpad=0.05,
            frameon=False,
            bbox_to_anchor=(0.18, 1),
            columnspacing=0.5,
            handletextpad=0.3,
            handlelength=1.2,
            fontsize=6,
        )
        for line in legends.get_lines():
            line.set_linewidth(0.8)


def make_legend_pdf(
    data_dir: Path,
    include_global: bool,
    width=SINGLE_COLUMN_WIDTH,
    height=DOUBLE_COLUMN_WIDTH * 0.015,
):
    policy_list = (
        ["base", "drf", "memshare", "global", "hare"]
        if include_global
        else ["base", "drf", "memshare", "hare"]
    )

    pseudo_fig = plt.figure()
    ax = pseudo_fig.add_subplot(111)

    # Create dummy plot elements for legend
    lines = []
    for p in policy_list:
        if p == "base":
            line = ax.plot(
                [],
                [],
                color=color_map[p],
                linestyle=line_style_map[p],
                linewidth=LINEWIDTH,
                label=labels_map[p],
                clip_on=False,
            )[0]
        else:
            # For bar chart policies, create a rectangle patch for legend
            rect = Rectangle((0, 0), 1, 1, facecolor=color_map[p], label=labels_map[p])
            ax.add_patch(rect)
            line = rect
        lines.append(line)

    legend_fig = plt.figure()
    legend_fig.set_size_inches(width, height)
    legend_fig.legend(
        lines,
        [labels_map[p] for p in policy_list],
        loc="center",
        ncol=len(policy_list),
        frameon=False,
        borderpad=0.05,
        handlelength=2.0,
        handletextpad=0.8,
        columnspacing=1.3,
    )
    save_fig(legend_fig, data_dir / "legend.pdf", tight_pad=0)
    plt.close(legend_fig)
    plt.close(pseudo_fig)


def main(
    data_dir: Path,
    yscale: str | None,
    tenants: List[str] | None,
    include_global: bool,
    do_plot_resrc: bool,
    tput_width_scale: float,
    cdf_width_scale: float,
    tput_height_scale: float,
    cdf_height_scale: float,
    ticks: List[str] | None,
    dense_threshold: float,
    dense_scale_tput: float,
    dense_scale_cdf: float,
    sep_legend: bool,
    highlight: str | None,
):
    df = load_data(data_dir)
    s_list = get_sid_list(df)
    if do_plot_resrc:
        fig, (ax_tput, ax_resrc) = build_fig(
            2,
            1,
            DOUBLE_COLUMN_WIDTH * tput_width_scale,
            DOUBLE_COLUMN_WIDTH * tput_height_scale,
        )
    else:
        fig, ax_tput = build_fig(
            1,
            1,
            DOUBLE_COLUMN_WIDTH * tput_width_scale,
            DOUBLE_COLUMN_WIDTH * tput_height_scale,
        )
    plot_tput(
        ax_tput,
        df,
        s_list,
        yscale=yscale,
        tenants=tenants,
        include_global=include_global,
        yticks=ticks,
        dense_threshold=dense_threshold,
        dense_scale=dense_scale_tput,
        sep_legend=sep_legend,
        highlight=highlight,
    )
    if do_plot_resrc:
        plot_resrc(ax_resrc, df, s_list, tenants=tenants)
    save_fig(fig, data_dir / "norm_tput.pdf", tight_pad=0.03)

    fig, ax_cdf = build_fig(
        1,
        1,
        DOUBLE_COLUMN_WIDTH * cdf_width_scale,
        DOUBLE_COLUMN_WIDTH * cdf_height_scale,
    )
    plot_cdf(
        ax_cdf,
        df,
        s_list,
        include_global=include_global,
        xticks=ticks,
        dense_threshold=dense_threshold,
        dense_scale=dense_scale_cdf,
        sep_legend=sep_legend,
    )
    save_fig(fig, data_dir / "norm_tput_cdf.pdf", tight_pad=0.05)

    # Create separate legend PDF if requested
    if sep_legend:
        make_legend_pdf(data_dir, include_global)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir", help="directory for experiment data", type=Path)
    parser.add_argument(
        "--yscale",
        help="scale y-axis ticks",
        type=str,
        choices=["m", "k", "g", "M", "K", "G", "Mi", "Ki", "Gi"],
    )
    parser.add_argument(
        "--tenants",
        help="Names of tenants",
        type=str,
        nargs="+",
    )
    parser.add_argument(
        "--include_global",
        help="include a run of global pooling resource",
        action="store_true",
    )
    parser.add_argument(
        "--plot_resrc",
        help="plot resource allocation",
        action="store_true",
    )
    parser.add_argument(
        "--tput_width_scale",
        help="for the normalized tput plot, scale the width of the figure relative to a double-column figure",
        type=float,
        default=0.7,
    )
    parser.add_argument(
        "--cdf_width_scale",
        help="for the CDF plot, scale the width of the figure relative to a double-column figure",
        type=float,
        default=0.3,
    )
    parser.add_argument(
        "--tput_height_scale",
        help="for the normalized tput plot, scale the height of the figure relative to a double-column figure",
        type=float,
        default=0.2,
    )
    parser.add_argument(
        "--cdf_height_scale",
        help="for the CDF plot, scale the height of the figure relative to a double-column figure",
        type=float,
        default=0.2,
    )
    parser.add_argument(
        "--ticks",
        help="for the normalized tput, show x-axis ticks; negative means showing the tick without label",
        type=str,
        nargs="+",
        default=["0", "1", "2", "3", "4", "5"],
    )
    parser.add_argument(
        "--dense_threshold",
        help="fold the axis into a dense ticks after the threshold",
        type=int,
        default=2,
    )
    parser.add_argument(
        "--dense_scale_tput",
        help="for the normalized tput, fold the axis into a dense ticks in the specified scale",
        type=float,
        default=3,
    )
    parser.add_argument(
        "--dense_scale_cdf",
        help="for the CDF, fold the axis into a dense ticks in the specified scale",
        type=float,
        default=4,
    )
    parser.add_argument("--sep_legend", help="Do not plot legend", action="store_true")
    parser.add_argument(
        "--highlight",
        help="Highlight tenants where the specified policy has the best throughput",
        type=str,
        choices=["drf", "memshare", "global", "hare"],
    )
    args = parser.parse_args()
    args_dict = vars(args)
    args_dict["do_plot_resrc"] = args.plot_resrc
    del args_dict["plot_resrc"]
    main(**args_dict)
