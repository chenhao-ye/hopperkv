from typing import Dict, Tuple

import pandas as pd
from hdrh.histogram import HdrHistogram


def merge_latency_hist(df_details) -> Dict[Tuple[int, int], HdrHistogram]:
    # aggregate latency across clients
    df_lat_hist_blob = df_details[["sid", "elapsed", "lat_hist_blob"]]
    dict_lat_hist_blob = (
        df_lat_hist_blob.groupby(["sid", "elapsed"])["lat_hist_blob"]
        .apply(list)
        .to_dict()
    )
    latency_dict = {}
    # first merge by (sid, elapsed)
    for (sid, elapsed), lat_hist_blob_list in dict_lat_hist_blob.items():
        lat_hist = None
        for lat_hist_blob in lat_hist_blob_list:
            if lat_hist is None:
                lat_hist = HdrHistogram.decode(lat_hist_blob)
            else:
                lat_hist.decode_and_add(lat_hist_blob)
        latency_dict[(sid, elapsed)] = lat_hist
    return latency_dict


def extract_latency_distrib_per_epoch(
    latency_dict: Dict[Tuple[int, int], HdrHistogram],
):
    latency_list = []
    for (sid, elapsed), lat_hist in latency_dict.items():
        latency_list.append(
            {
                "sid": sid,
                "elapsed": elapsed,
                "lat_mean": lat_hist.get_mean_value(),
                "lat_min": lat_hist.get_min_value(),
                "lat_max": lat_hist.get_max_value(),
                "p10": lat_hist.get_value_at_percentile(10),
                "p20": lat_hist.get_value_at_percentile(20),
                "p30": lat_hist.get_value_at_percentile(30),
                "p40": lat_hist.get_value_at_percentile(40),
                "p50": lat_hist.get_value_at_percentile(50),
                "p60": lat_hist.get_value_at_percentile(60),
                "p70": lat_hist.get_value_at_percentile(70),
                "p80": lat_hist.get_value_at_percentile(80),
                "p90": lat_hist.get_value_at_percentile(90),
                "p99": lat_hist.get_value_at_percentile(99),
                "p999": lat_hist.get_value_at_percentile(99.9),
            }
        )
    df_latency_summary = pd.DataFrame.from_records(
        latency_list, index=["sid", "elapsed"]
    )
    return df_latency_summary


# this will corrupt the input latency_dict; must be called last
def extract_latency_distrib_within_window(
    latency_dict: Dict[Tuple[int, int], HdrHistogram],
    min_elapsed: int,  # inclusive boundary
    max_elapsed: int,  # exclusive boundary
):
    sid_hist_map = {}
    for (sid, elapsed), lat_hist in latency_dict.items():
        if min_elapsed <= elapsed and elapsed < max_elapsed:
            if sid not in sid_hist_map:
                sid_hist_map[sid] = HdrHistogram(
                    lowest_trackable_value=lat_hist.lowest_trackable_value,
                    highest_trackable_value=lat_hist.highest_trackable_value,
                    significant_figures=lat_hist.significant_figures,
                    word_size=lat_hist.word_size,
                    b64_wrap=lat_hist.b64_wrap,
                )
            sid_hist_map[sid].add(lat_hist)

    latency_list = []
    for sid, lat_hist in sid_hist_map.items():
        latency_list.append(
            {
                "sid": sid,
                "ts_min": min_elapsed,
                "ts_max": max_elapsed,
                "lat_mean": lat_hist.get_mean_value(),
                "lat_min": lat_hist.get_min_value(),
                "lat_max": lat_hist.get_max_value(),
                "p10": lat_hist.get_value_at_percentile(10),
                "p20": lat_hist.get_value_at_percentile(20),
                "p30": lat_hist.get_value_at_percentile(30),
                "p40": lat_hist.get_value_at_percentile(40),
                "p50": lat_hist.get_value_at_percentile(50),
                "p60": lat_hist.get_value_at_percentile(60),
                "p70": lat_hist.get_value_at_percentile(70),
                "p80": lat_hist.get_value_at_percentile(80),
                "p90": lat_hist.get_value_at_percentile(90),
                "p99": lat_hist.get_value_at_percentile(99),
                "p999": lat_hist.get_value_at_percentile(99.9),
            }
        )
    df_latency_window = pd.DataFrame.from_records(latency_list, index="sid")
    return df_latency_window
