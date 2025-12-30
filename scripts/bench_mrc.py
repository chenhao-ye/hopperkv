# This script aims to study the miss ratio curves estimation.

import csv
import json
import logging
import os
import signal
from typing import Dict, List, Tuple

from driver.client.workload import SeqDistrib, StaticWorkload, UnifDistrib
from driver.launch import launch_clients, launch_servers
from driver.utils import prepare_data_dir, run_cmd
from hopperkv.alloc.resrc import ResrcStat
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


def get_stats_snapshot(r: HopperRedis) -> Tuple[List[float], List[float], Dict]:
    stats_res = r.stats()
    ghost_ticks = stats_res["ghost.ticks"]
    resrc_stat = ResrcStat(
        **{
            k.replace(".", "_"): v
            for k, v in stats_res.items()
            if k
            in {
                "ghost.hit_cnt",
                "ghost.miss_cnt",
                "req_cnt",
                "hit_cnt",
                "miss_cnt",
                "db_rcu_consump_if_miss",
                "net_bw_consump_if_miss",
                "net_bw_consump_if_hit",
                "db_rcu_consump",
                "db_wcu_consump",
                "net_bw_consump",
            }
        }
    )
    mem_stats = flatten_dict(r.memory_stats())
    return ghost_ticks, resrc_stat, mem_stats


def run_client_preheat(
    port: int,
    workload: StaticWorkload,
    seed: int,
    data_dir: str,
):
    wl_preheat = workload.copy()
    wl_preheat.distrib = SeqDistrib()
    c = launch_clients(
        num_clients=1,
        host="localhost",
        ports=[port],
        sid=0,
        workload=wl_preheat,
        count=workload.num_keys,
        data_dir=data_dir,
        batch_size=0,
        seed=seed,
    )[0]
    c.wait()


def run_client_measured(
    port: int,
    workload: StaticWorkload,
    seed: int,
    data_dir: str,
):
    c = launch_clients(
        num_clients=1,
        host="localhost",
        ports=[port],
        sid=0,
        workload=workload,
        count=workload.num_keys * 10,
        data_dir=data_dir,
        batch_size=0,
        seed=seed,
    )[0]
    c.wait()


def run_mrc(
    workload: StaticWorkload,
    seed: int,
    data_dir: str,
    port: int,
):
    s = launch_servers(ports=[port], data_dir=f"{data_dir}/mrc")[0]

    # connect to the server ourself to set config and collect more status
    r = HopperRedis(host="localhost", port=port)
    r.wait_ready(silent=True)
    r.set_config("dynamo.mock", workload.key_size, workload.val_size)
    tick = num_keys // 16
    r.set_ghost_range(tick, tick, tick * 16)
    # r.set_defrag()

    run_client_preheat(port, workload, seed, f"{data_dir}/preheat/mrc")
    _, last_snapshot, prev_mem_stats = get_stats_snapshot(r)

    run_client_measured(port, workload, seed, f"{data_dir}/mrc")
    ghost_ticks, curr_snapshot, curr_mem_stats = get_stats_snapshot(r)

    resrc_stat = curr_snapshot - last_snapshot
    ghost_miss_ratios = [
        mc / (hc + mc)
        for hc, mc in zip(resrc_stat.ghost_hit_cnt, resrc_stat.ghost_miss_cnt)
    ]

    s.send_signal(signal.SIGINT)
    s.wait()
    logging.info("Finish miss ratio curve profiling")

    with open(f"{data_dir}/mrc/prev_stats.json", "w") as f:
        json.dump(prev_mem_stats, f)
    with open(f"{data_dir}/mrc/post_stats.json", "w") as f:
        json.dump(curr_mem_stats, f)

    return ghost_ticks, ghost_miss_ratios


def run_miss_ratio_on_size(
    cache_size: int,
    workload: StaticWorkload,
    seed: int,
    data_dir: str,
    port: int,
) -> float:
    s = launch_servers(
        ports=[port], data_dir=f"{data_dir}/cache_{cache_size}", cleanup=False
    )[0]

    # connect to the server ourself to set config and collect more status
    r = HopperRedis(host="localhost", port=port)
    r.wait_ready(silent=True)
    r.set_config("dynamo.mock", workload.key_size, workload.val_size)
    r.set_cache_size(cache_size)
    # these should not matter, but set anyway
    tick = num_keys // 16
    r.set_ghost_range(tick, tick, tick * 16)
    # r.set_defrag()

    run_client_preheat(
        port,
        workload,
        seed,
        f"{data_dir}/preheat/cache_{cache_size}",
    )
    _, last_snapshot, prev_mem_stats = get_stats_snapshot(r)

    run_client_measured(
        port,
        workload,
        seed,
        f"{data_dir}/cache_{cache_size}",
    )
    _, curr_snapshot, curr_mem_stats = get_stats_snapshot(r)

    resrc_stat = curr_snapshot - last_snapshot
    miss_ratio = resrc_stat.miss_cnt / (resrc_stat.hit_cnt + resrc_stat.miss_cnt)

    s.send_signal(signal.SIGINT)
    s.wait()

    with open(f"{data_dir}/cache_{cache_size}/prev_stats.json", "w") as f:
        json.dump(prev_mem_stats, f)
    with open(f"{data_dir}/cache_{cache_size}/post_stats.json", "w") as f:
        json.dump(curr_mem_stats, f)
    with open(f"{data_dir}/cache_{cache_size}/miss_ratio.txt", "w") as f:
        f.write(f"{miss_ratio:.6f}")
    logging.info(f"Finish miss ratio measurement at size={cache_size}")

    return miss_ratio


def run(
    workload: StaticWorkload,
    seed: int,
    data_dir: str,
):
    prepare_data_dir(data_dir, cleanup=True)
    ghost_ticks, ghost_miss_ratios = run_mrc(workload, seed, data_dir, port=7000)
    # remove the first tick, which is the startup memory size
    ghost_ticks = ghost_ticks[1:]
    ghost_miss_ratios = ghost_miss_ratios[1:]

    next_port = 7001
    real_miss_ratios = []
    pids = []
    for tick in ghost_ticks:
        next_port += 1
        pid: int = os.fork()
        if pid == 0:
            run_miss_ratio_on_size(
                tick,
                workload,
                seed,
                data_dir,
                port=next_port,
            )
            exit(0)
        else:
            pids.append(pid)
            logging.info(
                f"Run miss ratio measurement with cache_size={tick} at process {pid}"
            )

    for pid in pids:
        _, rc = os.waitpid(pid, 0)
        if rc != 0:
            logging.error(f"Child process {pid} exits with rc={rc}")

    stats = []

    for cache_size in ghost_ticks:
        with open(f"{data_dir}/cache_{cache_size}/miss_ratio.txt", "r") as f:
            real_miss_ratio = float(f.read())
            real_miss_ratios.append(real_miss_ratio)
        with open(f"{data_dir}/cache_{cache_size}/prev_stats.json", "r") as f:
            prev_stats = json.load(f)
            prev_stats["tag"] = "prev"
            stats.append(prev_stats)
        with open(f"{data_dir}/cache_{cache_size}/post_stats.json", "r") as f:
            post_stats = json.load(f)
            post_stats["tag"] = "post"
            stats.append(post_stats)

    with open(f"{data_dir}/stats_details.csv", "w") as f:
        writer = csv.DictWriter(f, fieldnames=stats[0].keys())
        writer.writeheader()
        writer.writerows(stats)
    logging.info(f"Write detailed stats to {data_dir}/stats_details.csv")

    return ghost_ticks, ghost_miss_ratios, real_miss_ratios


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    cleanup_redis()
    num_keys = 1024 * 10
    key_size = 16
    val_size = 4000
    ghost_ticks, ghost_miss_ratios, real_miss_ratios = run(
        workload=StaticWorkload(
            num_keys=num_keys,
            key_size=key_size,
            val_size=val_size,
            write_ratio=0,
            distrib=UnifDistrib(),
        ),
        seed=537,
        data_dir=f"results/bench_mrc/n{num_keys}k{key_size}v{val_size}",
    )
    print(f"ghost_ticks: [{', '.join([f'{t // 1000}K' for t in ghost_ticks])}]")
    print(
        f"ghost_miss_ratios: [{', '.join([f'{mr * 100:.1f}' for mr in ghost_miss_ratios])}]"
    )
    print(
        f"real_miss_ratios:  [{', '.join([f'{mr * 100:.1f}' for mr in real_miss_ratios])}]"
    )
