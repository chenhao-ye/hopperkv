"""
Experiment Data management
"""

import argparse
import logging
import os
from pathlib import Path
from typing import List

import pandas as pd

from driver.utils import run_cmd

from .merge_latency import extract_latency_distrib_per_epoch, merge_latency_hist


def merge_data(
    data_dir: Path,
    data_fname: str = "data.csv",
    lat_hist_fname: str = "lat_hist.csv",
    summary_fname: str = "data_summary.csv",
    details_fname: str = "data_details.csv",
):
    df_details = None
    sid_list = []
    num_epoch = -1
    num_clients = -1
    for sid in os.listdir(data_dir):
        s_path = data_dir / sid
        if not os.path.isdir(s_path):
            # logging.warning(f"Skip unexpected file `{s_path}`...")
            continue
        sid_list.append(sid)
        num_clients_for_curr_server = 0

        merged_cid_list = []

        for cid in os.listdir(s_path):
            c_path = s_path / cid
            if not os.path.isdir(c_path):
                continue
            data_path = c_path / data_fname
            lat_hist_path = c_path / lat_hist_fname
            if not (os.path.isfile(data_path) and os.path.isfile(lat_hist_path)):
                continue
            merged_cid_list.append(cid)
            num_clients_for_curr_server += 1
            df_data = pd.read_csv(data_path)
            df_lat_hist = pd.read_csv(lat_hist_path)
            df = df_data.join(
                df_lat_hist.set_index("elapsed"), on="elapsed", how="outer"
            )
            df["sid"] = sid
            df["cid"] = cid
            num_epoch_for_curr_client = df.shape[0]
            if num_epoch < 0:
                num_epoch = num_epoch_for_curr_client
                logging.info(f"Detect {num_epoch} epochs")
            else:
                if num_epoch != num_epoch_for_curr_client:
                    logging.warning(
                        f"Number of epochs mismatch for client {sid}/{cid}..."
                    )
                    num_epoch = max(num_epoch, num_epoch_for_curr_client)
            df_details = (
                df
                if df_details is None
                else pd.concat([df_details, df], ignore_index=True)
            )
        if num_clients < 0:
            num_clients = num_clients_for_curr_server
            logging.info(f"Detect {num_clients} clients")
        else:
            if num_clients != num_clients_for_curr_server:
                logging.warning(f"Number of clients mismatch for server {sid}...")

        merged_cid_list.sort(key=lambda x: int(x[1:]))
        logging.info(
            f"Merge {len(merged_cid_list)} data directories from {s_path}: "
            f"{', '.join(merged_cid_list)}"
        )

    assert df_details is not None

    # validate no missing data points
    all_elapsed = df_details["elapsed"].unique()
    for sid in sid_list:
        for t in all_elapsed:
            if not ((df_details["sid"] == sid) & (df_details["elapsed"] == t)).any():
                logging.warning(f"Missing data point: sid={sid}, elapsed={t}")

    # dump the detailed big table
    df_details.to_csv(data_dir / details_fname, index=False)
    logging.info(f"Save merged details to `{data_dir / details_fname}`")

    # aggregate throughput across clients
    df_tput = df_details[["sid", "elapsed", "tput"]]
    df_tput.set_index(["sid", "elapsed"], inplace=True)
    df_tput_summary = df_tput.groupby(["sid", "elapsed"]).sum()

    # aggregate latency across clients
    latency_dict = merge_latency_hist(df_details)
    df_latency_summary = extract_latency_distrib_per_epoch(latency_dict)

    # join throughput and latency summary
    df_summary = df_tput_summary.join(
        df_latency_summary, on=["sid", "elapsed"], how="outer", validate="one_to_one"
    )
    df_summary.to_csv(data_dir / summary_fname)
    logging.info(f"Save merged summary to `{data_dir / summary_fname}`")

    return df_summary, df_details


def pull_remote_data(remote_clients: List[str], remote_path: Path, data_dir: Path):
    remote_data_dir = data_dir if data_dir.is_absolute() else remote_path / data_dir
    for sid, ssh_hostname in enumerate(remote_clients):
        assert ssh_hostname is not None
        run_cmd(
            [
                "scp",
                "-r",
                f"{ssh_hostname}:{remote_data_dir}/s{sid}",
                f"{data_dir}/",
            ]
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir", help="Directory for experiment data", type=Path)
    parser.add_argument(
        "--remote_clients",
        help="Remote clients (hostname set in ~/.ssh/config) to pull data from",
        nargs="+",
        type=str,
    )
    parser.add_argument(
        "--remote_path",
        help="Path on the remote machine that contains HopperKV top-level directory",
        type=Path,
        required=False,
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    if args.remote_clients:
        assert args.remote_path
        pull_remote_data(args.remote_clients, args.remote_path, args.data_dir)
    merge_data(args.data_dir)
