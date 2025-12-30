"""
This file is mostly an entry point for `run.py` from another script to attach and launch clients.
"""

import argparse
import json
import logging

from .launch import launch_clients
from .utils import check_rc


def main(launch_clients_args: str):
    args_dict = json.loads(launch_clients_args)
    c_list = launch_clients(**args_dict)
    exit_code = 0
    sid = args_dict["sid"]
    for cid, c in enumerate(c_list):
        rc = c.wait()
        check_rc(
            rc,
            err_msg=f"Client s{sid}/c{cid} exits unexpectedly with code {rc}",
            err_panic=False,
        )
        if rc != 0:
            exit_code = 1
    exit(exit_code)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "launch_clients_args",
        help="JSON-serialized arguments for `launch_clients`",
        type=str,
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    main(args.launch_clients_args)
