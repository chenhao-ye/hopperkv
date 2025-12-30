# This script aims to study the miss ratio curves estimation based on keys count.

import csv
import json
import logging
import os
import signal
import subprocess
import time

from driver.client.workload import StaticWorkload, UnifDistrib
from driver.launch import (
    launch_preload_fill,
    launch_preload_warmup,
    launch_servers,
)
from driver.utils import check_rc, prepare_data_dir, run_cmd
from hopperkv.hopper_redis import HopperRedis


def cleanup_redis():
    run_cmd(["sudo", "killall", "-9", "redis-server"], err_panic=False, silent=True)


def flatten_dict(d, key_prefix="", sep=":"):
    new_d = {}
    for k, v in d.items():
        if type(v) is dict:
            new_d.update(flatten_dict(v, key_prefix=k + sep))
        else:
            new_d[key_prefix + k] = v
    return new_d


def shutdown_server(s: subprocess.Popen):
    s.send_signal(signal.SIGINT)
    rc = s.wait()
    check_rc(
        rc,
        err_msg=f"Server exits unexpectedly with code {rc}",
        err_panic=False,
    )


# populate the database to the exact number of keys
def populate_keys(
    port: int,
    num_keys: int,
    key_size: int,
    val_size: int,
    data_dir: str,
):
    c_list = launch_preload_fill(
        num_preload=64,
        workload=StaticWorkload(
            num_keys=num_keys,
            key_size=key_size,
            val_size=val_size,
        ),
        port=port,
        sid=0,
        data_dir=data_dir,
        batch_size=0,
        verbose=True,
    )
    for c in c_list:
        c.wait()


def warmup_keys(
    port: int,
    num_keys: int,
    key_size: int,
    val_size: int,
    data_dir: str,
    duration: int,
    wait: bool,
):
    c_list = launch_preload_warmup(
        num_preload=64,
        workload=StaticWorkload(
            num_keys=num_keys,
            key_size=key_size,
            val_size=val_size,
            distrib=UnifDistrib(),
            write_ratio=0,
        ),
        port=port,
        sid=0,
        batch_size=0,
        duration=duration,
        data_dir=data_dir,
        verbose=True,
    )
    if wait:
        for c in c_list:
            c.wait()
    else:
        return c_list


def enforce_memory(r: HopperRedis, mem_size: int):
    r.exec("CONFIG", "SET", "MAXMEMORY", str(mem_size))


def propose_ticks(r: HopperRedis, tick, min_tick, max_tick):
    r.set_ghost_range(tick=tick, min_tick=min_tick, max_tick=max_tick)
    stat_dict = r.stats()  # must enable report_raw_ticks
    return stat_dict.get("ghost.ticks_keys_count")


def get_data_subdir(
    data_dir: str, num_keys: int, key_size: int, val_size: int, force_memory: int = 0
):
    return f"{data_dir}/n{num_keys}k{key_size}v{val_size}{f'f{force_memory}' if force_memory > 0 else ''}"


def profile_memory(
    port: int,
    num_keys: int,
    key_size: int,
    val_size: int,
    data_dir: str,
    force_memory: int = 0,
):
    s = launch_servers(ports=[port], data_dir=data_dir, cleanup=False)[0]
    r = HopperRedis(host="localhost", port=port)
    r.wait_ready(silent=True)
    r.set_config("dynamo.mock", key_size, val_size)
    # r.set_defrag()

    # tick = num_keys // 16
    # r.set_ghost_range(tick, tick, tick * 16)
    # ticks = propose_ticks(r, tick, tick, tick * 16)

    for _ in range(5):
        populate_keys(port, num_keys, key_size, val_size, data_dir)

    if force_memory > 0:
        enforce_memory(r, force_memory)
        time.sleep(0.5)

    c_list = warmup_keys(port, num_keys, key_size, val_size, data_dir, 10, wait=False)
    time.sleep(8)

    mem_stats = flatten_dict(r.memory_stats())
    mem_stats["key_size"] = key_size
    mem_stats["val_size"] = val_size
    mem_stats["num_keys"] = num_keys

    for c in c_list:
        c.wait()

    shutdown_server(s)

    with open(f"{data_dir}/stats.json", "w") as f_stats:
        json.dump(mem_stats, f_stats, indent=2)

    assert mem_stats["keys.count"] == num_keys or force_memory > 0, (
        f"Mismatch keys count: keys.count={mem_stats['keys.count']}, expected={num_keys}"
    )

    return mem_stats


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    cleanup_redis()
    data_dir = "results/profile_mrc"
    prepare_data_dir(data_dir, cleanup=True)
    key_size = 16
    val_size = 4000
    # force_memory = 0
    force_memory = 4 * 1024 * 1024

    pids = []
    port_offset = 0
    data_subdirs = []

    for i in range(16):
        num_keys = 400 * 1024 * 1024 // (key_size + val_size) * (i + 1) // 16
        data_subdir = get_data_subdir(
            data_dir, num_keys, key_size, val_size, force_memory
        )
        data_subdirs.append(data_subdir)
        pid = os.fork()
        if pid == 0:
            logging.info(
                f"Start profiling: num_keys={num_keys}, "
                f"key_size={key_size}, val_size={val_size}"
            )
            mem_stats = profile_memory(
                port=7000 + port_offset,
                num_keys=num_keys,
                key_size=key_size,
                val_size=val_size,
                data_dir=data_subdir,
                force_memory=force_memory,
            )
            logging.info(
                f"Finish profiling: num_keys={num_keys}, "
                f"key_size={key_size}, val_size={val_size}"
            )
            exit(0)
        port_offset += 1
        pids.append(pid)

    for pid in pids:
        _, rc = os.waitpid(pid, 0)
        if rc != 0:
            logging.warning(f"Child process {pid} exits with code {rc}")

    stats = []

    for data_subdir in data_subdirs:
        with open(f"{data_subdir}/stats.json", "r") as f:
            stats.append(json.load(f))

    with open(f"{data_dir}/stats_details.csv", "w") as f:
        writer = csv.DictWriter(f, fieldnames=stats[0].keys())
        writer.writeheader()
        writer.writerows(stats)
    logging.info(f"Write detailed stats to {data_dir}/stats_details.csv")
