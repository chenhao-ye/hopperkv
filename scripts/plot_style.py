import math
from typing import List, Tuple

import matplotlib
import matplotlib.pyplot as plt

matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42
matplotlib.rcParams["font.size"] = 7
matplotlib.rcParams["hatch.linewidth"] = 0.3

plt.rcParams["xtick.major.pad"] = "2"
plt.rcParams["ytick.major.pad"] = "2"
plt.rcParams["xtick.major.size"] = "3"
plt.rcParams["ytick.major.size"] = "3"
plt.rcParams["xtick.minor.size"] = "1.5"
plt.rcParams["ytick.minor.size"] = "1.5"
plt.rcParams["axes.labelpad"] = "1"

# based on USENIX template, unit: inch
DOUBLE_COLUMN_WIDTH = 7
COLUMN_SEP = 0.33
SINGLE_COLUMN_WIDTH = (DOUBLE_COLUMN_WIDTH - COLUMN_SEP) / 2

plt.rcParams.update({"mathtext.default": "regular"})

MARKER_SIZE = 2.5
LEGEND_MARKER_SIZE = 5
LINEWIDTH = 1

color_map = {
    "s0": "0",
    "s1": "0",
    "min": "0",
    "base": "0.2",
    "drf": "0.5",
    "hare": "0",
    "global": "0.7",
    "memshare": "0.8",
}

color_map.update(
    {
        (p, s): color_map[p]
        for p in ["base", "drf", "hare", "memshare"]
        for s in ["s0", "s1", "min"]
    }
)

line_style_map = {
    "base": ":",
    "drf": "--",
    "hare": "-",
    "memshare": "-",
    "global": "-.",
}

tenant_marker_style_map = {
    "min": "",
    "s0": "",
    "s1": "",
    "s2": "",
    "s3": "",
}

policy_marker_style_map = {"base": ":", "drf": "--x", "hare": "-o", "memshare": "-v"}

hatch_map = {
    "min": None,  # solid
    "s0": "/////",
    "s1": "\\\\\\\\\\",
    "s2": "....",
    "s3": "xxxx",
    "s4": "oo",
    "s5": "oo////",
    "s6": "oo\\\\\\\\",
    "s7": "ooxxxx",
}

# hybrid of line and marker
line_marker_style_map = tenant_marker_style_map.copy()
line_marker_style_map.update(policy_marker_style_map)
line_marker_style_map.update(
    {
        (p, s): s_style + p_style
        for p, p_style in policy_marker_style_map.items()
        for s, s_style in tenant_marker_style_map.items()
    }
)

zorder_map = {k: 10 + float(v) for k, v in color_map.items()}

# different line width and marker size for better readability when there are overlaps
# line layout in the front (larger zorder) should be thinner/smaller
linewidth_map = {
    k: LINEWIDTH if v == "0.8" else LINEWIDTH * 1.1 if v == "0.5" else LINEWIDTH * 1.3
    for k, v in color_map.items()
}

markersize_map = {
    k: (
        MARKER_SIZE
        if v == "0.8"
        else MARKER_SIZE * 1.1
        if v == "0.5"
        else MARKER_SIZE * 1.3
    )
    for k, v in color_map.items()
}
for k, v in line_marker_style_map.items():
    if "o" in v:
        markersize_map[k] = MARKER_SIZE * 1.3

labels_map = {
    "base": "Base",
    "drf": "DRF",
    "hare": "HARE",
    "memshare": "MS+DRF",
    "global": "NonPart",
    "min": "Min",
    **{f"s{i}": f"$T_{{{i + 1}}}$" for i in range(16)},
    "cache_size": "Cache",
    "db_rcu": "DB Read",
    "db_wcu": "DB Write",
    "net_bw": "Network",
}

scale_factor_map = {
    "": 1,
    "Ki": 1024,
    "Mi": 1024 * 1024,
    "Gi": 1024 * 1024 * 1024,
    "Ti": 1024 * 1024 * 1024 * 1024,
    "Pi": 1024 * 1024 * 1024 * 1024 * 1024,
    "K": 1000,
    "M": 1000_000,
    "G": 1000_000_000,
    "T": 1000_000_000_000,
    "P": 1000_000_000_000_000,
    "k": 1000,
    "m": 1000_000,
    "g": 1000_000_000,
    "t": 1000_000_000_000,
    "p": 1000_000_000_000_000,
}

scale_str_map = {
    "": "",
    # "Ki": "K",
    # "Mi": "M",
    # "Gi": "G",
    # "Ti": "T",
    # "Pi": "P",
    "Ki": "K",
    "Mi": "M",
    "Gi": "G",
    "Ti": "T",
    "Pi": "P",
    "K": "K",
    "M": "M",
    "G": "G",
    "T": "T",
    "P": "P",
}


def decide_scale(max_raw_val: float | int, binary_scale=False) -> Tuple[str, int]:
    for scale in (
        ["Pi", "Ti", "Gi", "Mi", "Ki"] if binary_scale else ["P", "T", "G", "M", "K"]
    ):
        if max_raw_val > scale_factor_map[scale]:
            return scale, scale_factor_map[scale]
    return "", 1


def decide_ticks(max_val: float | int):
    """
    The general rule of making ticks is:
    - avoid too long tick text (<= 3 chars)
    - ticks should be some easy-to-read numbers (e.g., 200, 100)
    """

    def gen_ticks(tick, max_val):
        num_ticks = math.ceil(max_val / tick)
        assert num_ticks * tick >= max_val
        return [i * tick for i in range(num_ticks + 1)]

    if max_val == 0:  # special cases
        return [0, 1]

    if max_val > 20:
        ticks = decide_ticks(max_val / 10)
        return [t * 10 for t in ticks]
    if max_val < 16 and max_val > 12:
        return gen_ticks(4, max_val)
    if max_val < 12 and max_val > 10:
        return gen_ticks(4, max_val)
    if max_val >= 10:
        return gen_ticks(5, max_val)
    if max_val >= 5:
        return gen_ticks(2, max_val)
    if max_val > 2:
        return gen_ticks(1, max_val)
    ticks = decide_ticks(max_val * 10)
    return [t / 10 for t in ticks]


def apply_ticks(
    ax,
    which: str,
    ticks: List[float | int | str],
    lim: List[float] | None = None,
    lim_margin=0,
    **kwargs,
):
    assert which in {"x", "y"}
    if lim is None:
        min_tick = float(ticks[0])
        max_tick = float(ticks[-1])
        margin = lim_margin * (max_tick - min_tick)
        lim = min_tick - margin, max_tick + margin
    if which == "x":
        ax.set_xticks([float(t) for t in ticks], [str(t) for t in ticks], **kwargs)
        ax.set_xlim(lim)
    else:
        ax.set_yticks([float(t) for t in ticks], [str(t) for t in ticks], **kwargs)
        ax.set_ylim(lim)


def config_auto_ticks(ax, which: str, max_val: float | int):
    ticks = decide_ticks(max_val)
    apply_ticks(ax, which, ticks)
    return ticks
