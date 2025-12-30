from collections.abc import Iterable
from pathlib import Path
from typing import List

import matplotlib
import matplotlib.layout_engine
import matplotlib.pyplot as plt

from .plot_style import (
    DOUBLE_COLUMN_WIDTH,
    LEGEND_MARKER_SIZE,
    SINGLE_COLUMN_WIDTH,
    color_map,
    labels_map,
    line_marker_style_map,
)


def build_fig_no_ax(total_width, total_height, **kwargs):
    fig = plt.figure()
    fig.set_size_inches(total_width, total_height)
    return fig


def set_axes(axes):
    for ax in axes:
        ax.spines[["right", "top"]].set_visible(False)


def build_fig(nrows, ncols, total_width, total_height, **kwargs):
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, **kwargs)
    fig.set_size_inches(total_width, total_height)

    def recursive_config_ax(axes):
        if isinstance(axes, Iterable):
            for ax in axes:
                recursive_config_ax(ax)
        else:
            axes.spines[["right", "top"]].set_visible(False)

    recursive_config_ax(axes)

    return fig, axes


def build_fig_single_col(nrows, ncols, hw_ratio=1.0, with_ax=True, **kwargs):
    subplot_width = SINGLE_COLUMN_WIDTH / ncols
    subplot_height = subplot_width * hw_ratio
    return (
        build_fig(nrows, ncols, SINGLE_COLUMN_WIDTH, subplot_height * nrows, **kwargs)
        if with_ax
        else build_fig_no_ax(SINGLE_COLUMN_WIDTH, subplot_height * nrows, **kwargs)
    )


def build_fig_double_col(nrows, ncols, hw_ratio=1.0, with_ax=True, **kwargs):
    subplot_width = DOUBLE_COLUMN_WIDTH / ncols
    subplot_height = subplot_width * hw_ratio
    return (
        build_fig(nrows, ncols, DOUBLE_COLUMN_WIDTH, subplot_height * nrows, **kwargs)
        if with_ax
        else build_fig_no_ax(DOUBLE_COLUMN_WIDTH, subplot_height * nrows, **kwargs)
    )


def save_fig(fig, fig_path, tight_pad=0.1):
    if tight_pad is not None:
        fig.set_layout_engine(matplotlib.layout_engine.TightLayoutEngine(pad=tight_pad))
    fig.savefig(fig_path)
    print(f"Save figure to {fig_path}")
    if str(fig_path).endswith(".pdf"):
        png_path = str(fig_path).replace(".pdf", ".png")
        fig.savefig(png_path, dpi=300)
        print(f"Save another png copy to {png_path}")


def make_legend(
    keys: List[str],
    data_dir: Path,
    width=DOUBLE_COLUMN_WIDTH,
    height=DOUBLE_COLUMN_WIDTH * 0.015,
    line_markers: List[str] | None = None,
    colors: List[str] | None = None,
    labels: List[str] | None = None,
    ncol=None,
    fontsize=None,
    columnspacing=1.3,
    borderpad=0.05,
    handlelength=2.0,
    handletextpad=0.8,
):
    if ncol is None:
        ncol = len(keys)
    pseudo_fig = plt.figure()
    ax = pseudo_fig.add_subplot(111)

    if line_markers is None:
        line_markers = [line_marker_style_map[k] for k in keys]
    if colors is None:
        colors = [color_map[k] for k in keys]
    if labels is None:
        labels = [labels_map[k] for k in keys]
    lines = [
        ax.plot(
            [],
            [],
            lm,
            color=c,
            markersize=LEGEND_MARKER_SIZE,
            label=lb,
            clip_on=False,
        )[0]
        for lm, c, lb in zip(line_markers, colors, labels)
    ]

    legend_fig = plt.figure()
    legend_fig.set_size_inches(width, height)
    legend_fig.legend(
        lines,
        [labels_map[k] for k in keys],
        loc="center",
        ncol=ncol,
        fontsize=fontsize,
        frameon=False,
        columnspacing=columnspacing,
        labelspacing=0.4,
        borderpad=borderpad,
        handlelength=handlelength,
        handletextpad=handletextpad,
    )
    save_fig(legend_fig, data_dir / "legend.pdf", tight_pad=0)
    plt.close(legend_fig)
