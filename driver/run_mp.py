"""
similar to run.py, but with multiple policies (mp)
"""

import argparse
import logging
from pathlib import Path
from typing import Dict, List

import pandas as pd

from .run import add_parser_args, preprocess_args
from .run import main as run_main
from .utils import prepare_data_dir


def load_alloc_results(data_dir: Path, policy: str, elapsed: int):
    df = pd.read_csv(data_dir / "alloc.csv")
    df_alloc = df[(df["elapsed"] == elapsed) & (df["policy"] == policy)]
    num_servers = df_alloc.shape[0]
    results = []
    for sid in range(num_servers):
        row_dicts = df_alloc[df_alloc["sid"] == sid].to_dict("records")
        assert len(row_dicts) == 1, (
            f"Multiple rows for filter policy={policy}, elapsed={elapsed}, sid={sid}: {row_dicts}"
        )
        row_dict = row_dicts[0]
        results.append(
            ",".join(
                (
                    str(row_dict["cache_size"]),
                    str(row_dict["db_rcu"]),
                    str(row_dict["db_wcu"]),
                    str(row_dict["net_bw"]),
                )
            )
        )
    return results


def main(
    mode: str,
    args_dict: Dict,
    mrc_salt_config: List[str] | None,
    include_global: bool,
    global_load_ckpt_paths: List[str] | None,
    global_tables: List[str] | None,
    skip_policies: List[str] | None,
    skip_cleanup: bool,
    policy_tables: List[str] | None,
):
    prepare_data_dir(args_dict["data_dir"], cleanup=not skip_cleanup)

    rejected_args = [
        ("dump_ckpt_paths", None),
        ("init_resrcs", None),
        ("skip_alloc", False),
        ("skip_apply", False),
        ("global_pool", False),
    ]
    required_args = ["alloc_sched"]

    for arg, expected in rejected_args:
        if args_dict.get(arg) != expected:
            raise ValueError(f"{arg} is not supported")
    for arg in required_args:
        if args_dict.get(arg) is None:
            raise ValueError(f"{arg} is required")

    args_base = args_dict.copy()
    args_drf = args_dict.copy()
    args_hare = args_dict.copy()
    args_memshare = args_dict.copy()
    args_global = args_dict.copy()

    if policy_tables is not None:
        num_policies = 4 - len(skip_policies)
        if include_global:
            num_policies += 1
        num_servers = len(args_dict["workloads"])
        expected_num_tables = num_policies * num_servers
        if expected_num_tables != len(policy_tables):
            raise ValueError(
                "Incorrect number of policy_tables: "
                f"expected={expected_num_tables}; "
                f"received={len(policy_tables)}"
            )

        if "base" not in skip_policies:
            args_base["tables"], policy_tables = (
                policy_tables[:num_servers],
                policy_tables[num_servers:],
            )
            logging.info(f"Base uses tables: {args_base['tables']}")
        if "drf" not in skip_policies:
            args_drf["tables"], policy_tables = (
                policy_tables[:num_servers],
                policy_tables[num_servers:],
            )
            logging.info(f"DRF uses tables: {args_drf['tables']}")
        if "hare" not in skip_policies:
            args_hare["tables"], policy_tables = (
                policy_tables[:num_servers],
                policy_tables[num_servers:],
            )
            logging.info(f"HARE uses tables: {args_hare['tables']}")
        if "memshare" not in skip_policies:
            args_memshare["tables"], policy_tables = (
                policy_tables[:num_servers],
                policy_tables[num_servers:],
            )
            logging.info(f"Memshare uses tables: {args_memshare['tables']}")
        if include_global:
            args_global["tables"], policy_tables = (
                policy_tables[:num_servers],
                policy_tables[num_servers:],
            )
            logging.info(f"Global uses tables: {args_global['tables']}")
        assert len(policy_tables) == 0

    mrc_salt_dict = {}
    if mrc_salt_config:
        for s in mrc_salt_config:
            policy, mrc_salt = s.split(":", 1)
            mrc_salt_dict[policy] = mrc_salt

    for policy, args in [
        ("base", args_base),
        ("drf", args_drf),
        ("hare", args_hare),
        ("memshare", args_memshare),
        ("global", args_global),
    ]:
        args.update(
            {
                "data_dir": f"{args_dict['data_dir']}/{policy}",
                "exper_namespace": policy,
            }
        )
        mrc_salt_modified = mrc_salt_dict.get(policy)
        if mrc_salt_modified is not None:
            args["mrc_salt"] = mrc_salt_modified

    drf_config = {
        "policy": "drf",
        "harvest": False,
        "conserving": True,
        "memshare": False,
    }
    hare_config = {
        "policy": "hare",
        "harvest": True,
        "conserving": True,
        "memshare": False,
    }
    memshare_config = {
        "policy": "memshare",
        "harvest": False,
        "conserving": True,
        "memshare": True,
    }

    if mode == "pipeline":
        assert args_dict["alloc_sched_rep"] is None
        assert len(args_dict["alloc_sched"]) == 1, (
            "`alloc_sched` must be a single timestamp"
        )
        args_base.update(
            {
                "skip_alloc": False,
                "skip_apply": True,
                "alloc_configs": [drf_config, hare_config, memshare_config],
            }
        )
        for args in [args_drf, args_hare, args_memshare]:
            args.update(
                {
                    "skip_alloc": True,
                    "skip_apply": True,
                    "alloc_configs": [],
                }
            )
    else:
        assert mode == "parallel"
        args_base
        args_base.update(
            {
                "skip_alloc": False,
                "skip_apply": True,
                "alloc_configs": [drf_config, hare_config, memshare_config],
            }
        )
        args_drf.update(
            {
                "skip_alloc": False,
                "skip_apply": False,
                "alloc_configs": [drf_config],
            }
        )
        args_hare.update(
            {
                "skip_alloc": False,
                "skip_apply": False,
                "alloc_configs": [hare_config],
            }
        )
        args_memshare.update(
            {
                "skip_alloc": False,
                "skip_apply": False,
                "alloc_configs": [memshare_config],
            }
        )

    if "base" in skip_policies:
        # if base is skipped, all its dependencies must be skipped
        assert "drf" in skip_policies
        assert "hare" in skip_policies
        assert "memshare" in skip_policies

    if "base" not in skip_policies:
        logging.info("Start to run baseline...")
        args_base = preprocess_args(args_base)
        run_main(**args_base)

        # load allocation decision and apply
        alloc_ts = args_base["alloc_sched"][0]
        logging.info(f"Use allocation decision made at ts={alloc_ts}")

    if "drf" not in skip_policies:  # run DRF
        args_drf["init_resrcs"] = load_alloc_results(
            args_base["data_dir"], "drf", alloc_ts
        )
        args_drf = preprocess_args(args_drf)
        logging.info("Start to run DRF...")
        run_main(**args_drf)

    if "hare" not in skip_policies:  # run HARE
        args_hare["init_resrcs"] = load_alloc_results(
            args_base["data_dir"], "hare", alloc_ts
        )
        args_hare = preprocess_args(args_hare)
        logging.info("Start to run HARE...")
        run_main(**args_hare)

    if "memshare" not in skip_policies:  # run Memshare
        args_memshare["init_resrcs"] = load_alloc_results(
            args_base["data_dir"], "memshare", alloc_ts
        )
        args_memshare = preprocess_args(args_memshare)
        logging.info("Start to run Memshare...")
        run_main(**args_memshare)

    if include_global and "global" not in skip_policies:
        logging.info("Start to run Global...")
        args_global.update(
            {
                "skip_alloc": True,
                "skip_apply": True,
                "alloc_configs": [],
                "global_pool": True,
                "skip_load_ckpt_check": True,
                "num_preload": 0,  # disable preload
                "load_cache_image_paths": None,
            }
        )
        if global_load_ckpt_paths is not None:
            args_global["load_ckpt_paths"] = global_load_ckpt_paths
        if global_tables is not None:
            args_global["tables"] = global_tables
        args_global = preprocess_args(args_global)
        run_main(**args_global)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    parser = argparse.ArgumentParser(
        description="Run an experiment with multiple policies"
    )
    parser.add_argument(
        "mode",
        help="mode to run policies; "
        "`pipeline` will run policy `base` and load (the first) allocation to run other policies; "
        "`parallel` will run all policies individually",
        choices=["pipeline", "parallel"],
    )
    parser.add_argument(
        "--mrc_salt_config",
        help="Overwrite mrc_salt for certain policies, formatted as `policy:mrc_salt`",
        nargs="+",
        required=False,
    )
    parser.add_argument(
        "--include_global",
        help="include another policy run of global pooling resource",
        action="store_true",
    )
    parser.add_argument(
        "--global_load_ckpt_paths",
        help="a list of path to load checkpoint for the global pooling experiment",
        type=str,
        nargs="+",
        required=False,
    )
    parser.add_argument(
        "--global_tables",
        help="a list of tables to run the global pooling experiment",
        type=str,
        nargs="+",
        required=False,
    )
    parser.add_argument(
        "--skip_policies",
        help="skip a set of polices",
        choices=["base", "drf", "hare", "memshare", "global"],
        nargs="*",
        default=[],
        required=False,
    )
    parser.add_argument(
        "--skip_cleanup",
        help="skip clean up data directories (the policy-specific directory will still be cleaned up on-demand)",
        action="store_true",
    )
    # for trace-replay workload, each policy run will modify the table.
    # To ensure a correct replay, each run should use a new table.
    parser.add_argument(
        "--policy_tables",
        help="Use different tables for different policies (will overwrite the given --tables)",
        nargs="+",
        required=False,
    )

    add_parser_args(parser)
    args = parser.parse_args()
    args_dict = vars(args)
    mode = args.mode
    mrc_salt_config = args.mrc_salt_config
    include_global = args.include_global
    global_load_ckpt_paths = args.global_load_ckpt_paths
    global_tables = args.global_tables
    skip_policies = args.skip_policies
    skip_cleanup = args.skip_cleanup
    policy_tables = args.policy_tables
    del args_dict["mode"]
    del args_dict["mrc_salt_config"]
    del args_dict["include_global"]
    del args_dict["global_load_ckpt_paths"]
    del args_dict["global_tables"]
    del args_dict["skip_policies"]
    del args_dict["skip_cleanup"]
    args_dict.pop("policy_tables", None)
    main(
        mode,
        args_dict,
        mrc_salt_config,
        include_global,
        global_load_ckpt_paths,
        global_tables,
        skip_policies,
        skip_cleanup,
        policy_tables,
    )
