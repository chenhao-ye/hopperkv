import argparse
import logging
import os
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from hopperkv.utils import str_cast_type

from .plot_style import (
    apply_ticks,
    color_map,
    config_auto_ticks,
    decide_scale,
    line_marker_style_map,
    linewidth_map,
    markersize_map,
    scale_factor_map,
    scale_str_map,
    zorder_map,
)
from .plot_util import (
    build_fig_double_col,
    save_fig,
    set_axes,
)


def load_data(data_dir: Path, var_type=float) -> Tuple[List, List[pd.DataFrame], str]:
    var_name = None
    var_val_list = []
    for subdir_str in os.listdir(data_dir):
        subdir = data_dir / subdir_str
        if not subdir.is_dir() or "=" not in subdir_str:
            continue
        curr_var_name, curr_var_val = subdir_str.split("=")
        if var_name is None:
            var_name = curr_var_name
            logging.info(f"Detect variable `{var_name}`")
        else:
            assert var_name == curr_var_name, (
                f"Unexpected variable name: {curr_var_name}"
            )
        var_val_list.append(curr_var_val)

    var_val_list.sort(key=lambda x: str_cast_type(x, var_type, binary_scale=False))
    x, df_list = [], []
    for var_val in var_val_list:
        file_path = data_dir / f"{var_name}={var_val}" / "data.csv"
        if not file_path.exists():
            logging.warning(f"File not found: {file_path}")
            continue
        x.append(str_cast_type(var_val, var_type, binary_scale=False))
        df_list.append(pd.read_csv(file_path))
    return x, df_list, var_name


def get_datapoint(df: pd.DataFrame, sid: str, policy: str, field: str):
    df_filter = df[(df["sid"] == sid) & (df["policy"] == policy)][field]
    assert df_filter.shape[0] == 1, (
        "Unexpected number of matching data points: "
        f"sid={sid}, policy={policy}, field={field}, df_filter={df_filter}, df=\n{df}"
    )
    return df_filter.to_list()[0]


def get_max_val(df_list: List[pd.DataFrame], field: str):
    return max(df[field].max() for df in df_list)


def plot_improve(ax, x: List, df_list: List[pd.DataFrame]):
    y_hare = []
    y_drf = []
    y_memshare = []
    for df in df_list:
        for p, y in [("drf", y_drf), ("memshare", y_memshare), ("hare", y_hare)]:
            min_improve = min(
                get_datapoint(df, sid, p, "tput")
                / get_datapoint(df, sid, "base", "tput")
                - 1
                for sid in ["s0", "s1"]
            )
            y.append(min_improve + 1)
    # ax.axhline(0, color="black", linestyle=":", alpha=0.3, linewidth=0.7)
    y_base = [1] * len(x)
    lines = [
        ax.plot(
            x,
            y,
            line_marker_style_map[(p, "min")],
            label=p,
            color=color_map[(p, "min")],
            zorder=zorder_map[(p, "min")],
            markersize=markersize_map[(p, "min")],
            linewidth=linewidth_map[(p, "min")],
            clip_on=False,
        )[0]
        for p, y in [
            ("base", y_base),
            ("drf", y_drf),
            ("memshare", y_memshare),
            ("hare", y_hare),
        ]
    ]
    apply_ticks(ax, "y", [0, 0.5, 1, 1.5, 2], fontweight="semibold")
    ax.set_ylabel("Min normalized tput", fontweight="semibold")
    ax.spines["left"].set_linewidth(1.5)
    ax.spines["bottom"].set_linewidth(1.5)
    ax.tick_params(axis="both", length=3.5, width=1.5)
    ax.set_facecolor("0.95")
    # ax.legend()
    return lines


def plot_tput(
    ax1,
    ax2,
    x,
    df_list,
):
    max_tput = get_max_val(df_list, "tput")
    yscale, yscale_factor = decide_scale(max_tput)
    yscale_str = scale_str_map[yscale] + " " if yscale else ""

    for sid, ax, tenant_name in [("s0", ax1, "$T_1$"), ("s1", ax2, "$T_2$")]:
        y_max = 0
        for p in ["base", "drf", "memshare", "hare"]:
            y = [get_datapoint(df, sid, p, "tput") / yscale_factor for df in df_list]
            ax.plot(
                x,
                y,
                line_marker_style_map[(p, sid)],
                label=p,
                color=color_map[(p, sid)],
                zorder=zorder_map[(p, sid)],
                markersize=markersize_map[(p, sid)],
                linewidth=linewidth_map[(p, sid)],
                clip_on=False,
            )
            y_max = max(y_max, max(y))

        config_auto_ticks(ax, "y", y_max)
        ax.set_ylabel(f"{tenant_name} ({yscale_str}req/s)")

        # ax.legend()


def plot_tail_lat(ax1, ax2, x, df_list, percentile=999):
    max_tail_lat = get_max_val(df_list, f"p{percentile}")
    yscale, yscale_factor = decide_scale(max_tail_lat)
    for sid, ax in [("s0", ax1), ("s1", ax2)]:
        for p in ["base", "drf", "memshare", "hare"]:
            y = [
                get_datapoint(df, sid, p, f"p{percentile}") / yscale_factor
                for df in df_list
            ]
            ax.plot(
                x,
                y,
                line_marker_style_map[(p, sid)],
                label=p,
                color=color_map[(p, sid)],
                zorder=zorder_map[(p, sid)],
                markersize=markersize_map[(p, sid)],
                linewidth=linewidth_map[(p, sid)],
                clip_on=False,
            )

        apply_ticks(ax, "y", [0, 100, 200, 300])

    ax1.set_ylabel(f"$T_1$ p{percentile} (ms)")
    ax2.set_ylabel(f"$T_2$ p{percentile} (ms)")


def plot_miss_ratio(ax, x, df_list):
    for sid in ["s0", "s1"]:
        for p in ["base", "drf", "memshare", "hare"]:
            y = [get_datapoint(df, sid, p, "miss_ratio") * 100 for df in df_list]
            ax.plot(
                x,
                y,
                line_marker_style_map[(p, sid)],
                label=p,
                color=color_map[(p, sid)],
                zorder=zorder_map[(p, sid)],
                markersize=markersize_map[(p, sid)],
                linewidth=linewidth_map[(p, sid)],
                clip_on=False,
            )

    apply_ticks(ax, "y", [0, 25, 50, 75, 100])
    ax.set_ylabel("Miss ratio (%)")
    # ax.legend()


def plot_resrc(ax, x, df_list, field, sid_list, binary_scale: bool = False) -> str:
    max_val = get_max_val(df_list, field)
    yscale, yscale_factor = decide_scale(max_val, binary_scale)
    yscale_factor = scale_factor_map[yscale]
    base_resrc = None
    for sid in sid_list:
        for p in ["base", "drf", "hare", "memshare"]:
            y = [get_datapoint(df, sid, p, field) / yscale_factor for df in df_list]
            ax.plot(
                x,
                y,
                line_marker_style_map[(p, sid)],
                label=p,
                color=color_map[(p, sid)],
                zorder=zorder_map[(p, sid)],
                markersize=markersize_map[(p, sid)],
                linewidth=linewidth_map[(p, sid)],
                clip_on=False,
            )
            if p == "base":
                base_resrc = max(y)

    yticks = (
        [
            "0",
            f"{base_resrc / 2:.1f}",
            f"{base_resrc:.1f}",
            f"{base_resrc * 3 / 2:.1f}",
            f"{base_resrc * 2:.1f}",
        ]
        if int(base_resrc / 2) * 2.0 != base_resrc
        else [
            "0",
            f"{base_resrc / 2:.0f}",
            f"{base_resrc:.0f}",
            f"{base_resrc * 3 / 2:.0f}",
            f"{base_resrc * 2:.0f}",
        ]
    )

    apply_ticks(ax, "y", yticks)
    # ax.legend()
    return yscale


def plot_cache(ax, x, df_list):
    yscale = plot_resrc(ax, x, df_list, "cache_size", ["s0"], binary_scale=True)
    ax.set_ylabel(f"$T_1$ cache ({scale_str_map[yscale]}B)")


def plot_net(ax, x, df_list):
    yscale = plot_resrc(ax, x, df_list, "net_bw", ["s0"], binary_scale=True)
    ax.set_ylabel(f"$T_1$ network ({scale_str_map[yscale]}B/s)")


def plot_rcu(ax, x, df_list):
    yscale = plot_resrc(ax, x, df_list, "db_rcu", ["s0"])
    ax.set_ylabel(f"$T_1$ DB RU ({f'{scale_str_map[yscale]}' if yscale else ''}/s)")


def plot_wcu(ax, x, df_list):
    yscale = plot_resrc(ax, x, df_list, "db_wcu", ["s0"])
    ax.set_ylabel(f"$T_1$ DB WU ({f'{scale_str_map[yscale]}' if yscale else ''}/s)")


def set_var_ax(ax, x, xscale=None, xtitle_factory=None):
    if xscale is None:
        xscale, xscale_factor = decide_scale(max(x))
        x = [x / xscale_factor for x in x]

    config_auto_ticks(ax, "x", max(x))
    if xtitle_factory is not None:
        ax.set_xlabel(xtitle_factory(xscale))


def main(
    data_dir: Path,
    xlabel: str | None,
    xscale: str | None,
    xticks: List[str] | None,
    xlim: List[float] | None,
):
    x, df_list, var_name = load_data(data_dir)

    if xscale is None:
        xscale, xscale_factor = decide_scale(max(x))
    else:
        xscale_factor = scale_factor_map[xscale]
    x = [x / xscale_factor for x in x]

    fig = build_fig_double_col(2, 5, hw_ratio=0.6, with_ax=False)

    axs = fig.subplot_mosaic(
        [
            [".", "tput1", "lat1", "cache", "rcu"],
            ["improve", "tput2", "lat2", "net", "wcu"],
        ],
        gridspec_kw={
            "top": 0.9,
            "bottom": 0.15,
            "left": 0.05,
            "right": 0.99,
            "wspace": 0.5,
            "hspace": 0.2,
        },
        # position: [left, bottom, width, height]
        per_subplot_kw={"improve": {"position": [0.05, 0.155, 0.14, 0.6]}},
        sharex=True,
    )
    # adjust title location
    axs["improve"].set_title(
        "Target fairness metric",
        loc="center",
        x=0.45,
        y=1.24,
        fontdict={"fontweight": "bold"},
    )
    axs["tput1"].set_title("Throughput", fontdict={"fontweight": "bold"})
    axs["lat1"].set_title("Tail latency", fontdict={"fontweight": "bold"})
    axs["cache"].set_title(" " * 40 + "Resource share", fontdict={"fontweight": "bold"})

    set_axes(list(axs.values()))

    lines = plot_improve(axs["improve"], x, df_list)
    plot_tput(axs["tput1"], axs["tput2"], x, df_list)
    # plot_miss_ratio(ax_mr, x, df_list)
    plot_cache(axs["cache"], x, df_list)
    plot_net(axs["net"], x, df_list)
    plot_rcu(axs["rcu"], x, df_list)
    plot_wcu(axs["wcu"], x, df_list)
    plot_tail_lat(axs["lat1"], axs["lat2"], x, df_list)

    x_min = min(x)
    x_max = max(x)
    if xticks is None:
        xticks = [x_min, (x_min + x_max) / 2, x_max]

    for name in ["improve", "tput2", "lat2", "net", "wcu"]:
        ax = axs[name]
        apply_ticks(ax, "x", xticks, lim=xlim)
        ax.xaxis.set_tick_params(labelbottom=True)
        ax.set_xlabel(xlabel if xlabel is not None else var_name + f" ({xscale})")

    for ax in axs.values():
        ax.tick_params(axis="both", labelsize=6)
        ax.set_xlabel(ax.get_xlabel(), fontsize=6)
        ax.set_ylabel(ax.get_ylabel(), fontsize=6)
        ax.title.set_fontsize(7.5)

    fig.legend(
        handles=lines,
        labels=["Base", "DRF", "MS+DRF", "HARE"],
        frameon=False,
        loc="center",
        ncol=2,
        columnspacing=1.2,
        bbox_to_anchor=(0.105, 0.84),
        fontsize=6,
        fancybox=False,
        # markerscale=1.3,
        # handletextpad=0.4,
    )
    # ax_improve.set_xticklabels(ax_improve.get_xticks(), fontweight='semibold')

    save_fig(fig, data_dir / "perf_resrc.pdf", tight_pad=None)

    # make_legend(["base", "drf", "hare", "s0", "s1", "min"], data_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir", help="Directory for experiment data", type=Path)
    parser.add_argument(
        "--xlabel",
        help="label of x-axis",
        type=str,
    )
    parser.add_argument(
        "--xscale",
        help="scale x-axis ticks",
        type=str,
        choices=["m", "k", "g", "M", "K", "G", "Mi", "Ki", "Gi"],
    )
    parser.add_argument(
        "--xticks",
        help="x-axis ticks",
        type=str,
        nargs="+",
    )
    parser.add_argument(
        "--xlim",
        help="min and max of x-ticks (default is xticks min and max)",
        type=float,
        nargs=2,
    )

    args = parser.parse_args()
    main(**vars(args))
