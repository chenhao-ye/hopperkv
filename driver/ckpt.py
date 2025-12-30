import json
import logging
import os
import shutil
import time
from pathlib import Path
from typing import List

from hopperkv.alloc.resrc import ResrcTuple
from hopperkv.hopper_redis import HopperRedis

from .client.workload import DynamicWorkload, StaticWorkload, Workload
from .utils import prepare_data_dir


def check_load_ckpts(
    load_ckpt_paths: List[str | None] | None,
    workloads: List[DynamicWorkload],
    init_resrc_list: List[ResrcTuple] | None,
):
    if load_ckpt_paths is None:
        return
    if init_resrc_list is None:
        init_resrc_list = [None] * len(load_ckpt_paths)
    assert len(load_ckpt_paths) == len(workloads)
    for load_ckpt_path, workload, init_resrc in zip(
        load_ckpt_paths, workloads, init_resrc_list
    ):
        assert os.path.isfile(f"{load_ckpt_path}/dump.rdb"), (
            f"{load_ckpt_path}/dump.rdb not found"
        )
        assert os.path.isfile(f"{load_ckpt_path}/dump.ghc"), (
            f"{load_ckpt_path}/dump.ghc not found"
        )
        assert os.path.isfile(f"{load_ckpt_path}/ckpt.json"), (
            f"{load_ckpt_path}/ckpt.json not found"
        )
        with open(f"{load_ckpt_path}/ckpt.json") as f_ckpt:
            ckpt_info = json.load(f_ckpt)
        ckpt_workload = StaticWorkload.from_string(ckpt_info["workload"])
        # there is certain limits on the checkpoint-compatibility
        # key_size and val_size must match
        assert ckpt_workload.key_size == workload.first.key_size
        assert ckpt_workload.val_size == workload.first.val_size
        # recovered checkpoint must not contain any keys not in the current one
        assert ckpt_workload.num_keys <= workload.first.num_keys

        if init_resrc is not None:
            ckpt_mem_size = ckpt_info["mem_stats"]["total.allocated"]
            if ckpt_mem_size < init_resrc.cache_size * 0.95:
                logging.warning(
                    f"Checkpoint data ({ckpt_mem_size}B) is smaller than "
                    f"the specified cache size ({init_resrc.cache_size}B)"
                )


def dump_ckpts(
    redis_connections: List[HopperRedis],
    dump_ckpt_paths: List[str] | None,
    workloads: List[Workload | str],
    data_dir: Path,
):
    if dump_ckpt_paths is None:
        return
    logging.info("Start to take checkpoints...")
    mem_stats_list = [r.memory_stats() for r in redis_connections]
    last_save_time_list = [r.exec("LASTSAVE") for r in redis_connections]
    for r in redis_connections:
        r.exec("BGSAVE")
        r.exec("HOPPER.GHOST.SAVE")
    for sid, r in enumerate(redis_connections):
        while r.exec("LASTSAVE") == last_save_time_list[sid]:
            time.sleep(1)
    logging.info("All servers have completed RDB dump")

    for sid, (workload, mem_stats, dump_ckpt_path) in enumerate(
        zip(workloads, mem_stats_list, dump_ckpt_paths)
    ):
        s_path = f"{data_dir}/s{sid}"
        assert s_path != dump_ckpt_path
        prepare_data_dir(dump_ckpt_path, cleanup=True)
        # save dump.rdb and dump.ghc
        for ckpt_type in ["rdb", "ghc"]:
            ckpt_file_path = f"{s_path}/dump.{ckpt_type}"
            if not os.path.isfile(ckpt_file_path):
                raise ValueError(f"Fail to checkpoint: {ckpt_file_path} not found")
            shutil.move(ckpt_file_path, dump_ckpt_path)

        ckpt_info = {
            "workload": str(workload.last)
            if isinstance(workload, DynamicWorkload)
            else str(workload),
            "mem_stats": mem_stats,
        }
        with open(f"{dump_ckpt_path}/ckpt.json", "w") as f_ckpt:
            json.dump(ckpt_info, f_ckpt, indent=2)
        logging.info(f"Checkpoint s{sid} to {dump_ckpt_path}")
