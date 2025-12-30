"""
This script provides a handy way to launch an experiment by specifying
tenants' configuration from commandline.
"""

import argparse
import concurrent.futures
import json
import logging
import signal
import subprocess
import time
from pathlib import Path
from typing import Dict, List
from uuid import uuid4

from hopperkv.alloc import engine
from hopperkv.alloc.controller import (
    init_alloc,
    poll_prev_snapshots,
    post_alloc_apply,
    pre_alloc_poll,
    run_alloc,
)
from hopperkv.alloc.resrc import ResrcTuple
from hopperkv.hopper_redis import HopperRedis

from .ckpt import check_load_ckpts, dump_ckpts
from .client.workload import DynamicWorkload, ImageLoadWorkload, TraceReplayWorkload
from .launch import (
    isolate_proc_cpus,
    launch_clients,
    launch_preload_fill,
    launch_preload_load,
    launch_preload_warmup,
    launch_remote_clients,
    launch_servers,
)
from .utils import check_rc, prepare_data_dir, run_cmd


def dump_servers_stats(
    redis_connections: List[HopperRedis], data_dir: Path, fname: str
):
    # post-experiment stats dump
    stats = {}
    for sid, r in enumerate(redis_connections):
        stats[sid] = {
            "HOPPER.STATS": r.stats(),
            "HOPPER.RESRC": r.get_resrc(),
            "HOPPER.CONFIG": r.get_config(),
            "MEMORY_STATS": r.memory_stats(),
        }
    with open(data_dir / f"{fname}.json", "w") as f_stats:
        json.dump(stats, f_stats, indent=2)


def config_ticks(
    redis_connections: List[HopperRedis],
    req_size_list: List[int] | None,
    base_resrc: ResrcTuple,
    num_ticks_list: List[int] | None,  # how many ticks to cover the ghost cache range
    max_cache_scale: int,  # default: scale cache to 4 times at most
    delta_frac=64,  # fraction of base cache size as cache delta
):
    num_servers = len(redis_connections)
    assert len(req_size_list) == num_servers
    # parameter: we take step size as 1/64 of total cache
    if num_ticks_list is None:
        num_ticks_list = [64] * num_servers

    max_ghost_cache_size = base_resrc.cache_size * min(max_cache_scale, num_servers)

    for r, s, num_ticks in zip(redis_connections, req_size_list, num_ticks_list):
        tick = int(max_ghost_cache_size / num_ticks / s)
        max_tick = tick * (num_ticks + 1)
        # note the unit of ghost range is #keys
        r.set_ghost_range(tick=tick, min_tick=tick, max_tick=max_tick)

    cache_delta = int(base_resrc.cache_size / delta_frac)
    engine.set_cache_delta(cache_delta)
    # some manually set limit to ensure stable systems
    engine.set_min_cache_size(10 * 1024 * 1024)
    engine.set_min_db_rcu(base_resrc.db_rcu / 100)
    engine.set_min_db_wcu(base_resrc.db_wcu / 100)
    engine.set_min_net_bw(base_resrc.net_bw / 100)


# wrapper that decides whether to launch client remotely or locally
def launch_clients_remote_or_local(
    remote_client: str | None, remote_path: str | None, **kwargs
):
    if remote_client is None:
        assert remote_path is None
        return launch_clients(**kwargs)
    else:
        assert remote_path is not None
        return [
            launch_remote_clients(
                remote_client=remote_client, remote_path=remote_path, **kwargs
            )
        ]


def wait_clients(c_list: List[subprocess.Popen], is_remote: bool, num_servers: int):
    for c_idx, c in enumerate(c_list):
        rc = c.wait()
        if is_remote:
            sid = c_idx  # each server has one ssh proxy that launches all clients on the remote machine
            check_rc(
                rc,
                err_msg=f"Server s{sid} has at least one client exit unexpectedly",
                err_panic=False,
            )
        else:
            num_client_per_server = len(c_list) // num_servers
            cid = c_idx % num_client_per_server
            sid = c_idx // num_client_per_server
            check_rc(
                rc,
                err_msg=f"Client s{sid}/c{cid} exits unexpectedly with code {rc}",
                err_panic=False,
            )


def shutdown_servers(s_list: List[subprocess.Popen]):
    for sid, s in enumerate(s_list):
        s.send_signal(signal.SIGINT)
        rc = s.wait()
        check_rc(
            rc,
            err_msg=f"Server s{sid} exits unexpectedly with code {rc}",
            err_panic=False,
        )


def main(
    host: str,
    tables: List[str],
    remote_clients: List[str] | None,
    remote_path: str | None,
    workloads: List[DynamicWorkload | TraceReplayWorkload],
    num_clients: int,
    async_queue_depth: int,
    duration: int,
    preheat_duration: int,
    batch_size: int,
    data_dir: Path,
    base_resrc: ResrcTuple,
    init_resrcs: List[str] | None,
    alloc_sched: List[int],
    alloc_stat_window: int,
    report_freq: int,
    verbose: bool,
    check: bool,
    mock_dynamo: bool,
    isolate_cpu: bool,
    global_pool: bool,
    num_preload: int,
    preload_batch_size: int | None,
    ghost_num_ticks: List[int] | None,
    ghost_hint_kv_sizes: List[int] | None,
    ghost_max_cache_scale: int,
    boost: bool,
    gradual: bool,
    alloc_apply_threshold: float,
    mrc_salt: float,
    smooth_window: int,
    load_ckpt_paths: List[str] | None,
    dump_ckpt_paths: List[str] | None,
    load_cache_image_paths: List[str] | None,
    load_mock_image_paths: List[str] | None,
    skip_alloc: bool,
    skip_apply: bool,
    trace_max_timestamp: float,
    trace_max_line: int,
    trace_queue_size: int,
    # args below are only set by run_mp, never by cmdline args
    ## do not check whether the loaded ckpt parameters match; usually only used with global_pool
    skip_load_ckpt_check: bool = False,
    ## namespace for spdlog logger to avoid conflict
    exper_namespace: str | None = None,
    ## tries these policies on every alloctaion; if not skip_apply, only the last will be applied
    alloc_configs: List[Dict] = [
        {"policy": "hare", "harvest": True, "conserving": True, "memshare": False}
    ],
):
    assert not (boost and gradual)
    # pre-process input arguments
    num_servers = len(workloads)
    assert len(tables) == num_servers, f"{len(tables)=} not match {num_servers=}"

    is_remote = remote_clients is not None
    if is_remote:
        assert len(remote_clients) == num_servers
    else:
        remote_clients = [None] * num_servers

    if global_pool:
        # no allocation shall ever happen
        assert not alloc_configs or not alloc_sched or skip_alloc

    init_resrc_list: List[ResrcTuple] = (
        [base_resrc] * num_servers
        if init_resrcs is None
        else [ResrcTuple.from_str(init_resrc_str) for init_resrc_str in init_resrcs]
    )
    assert len(init_resrc_list) == num_servers

    # now start to launch servers and clients
    port_list = [6400 + i for i in range(num_servers)]
    # create a list of random passwords
    pword_list = [f"s{sid}:{uuid4()}" for sid in range(num_servers)]

    assert load_ckpt_paths is None or len(load_ckpt_paths) == num_servers
    assert dump_ckpt_paths is None or len(dump_ckpt_paths) == num_servers
    # load_ckpt and load_preheat_image are incompatible
    assert load_ckpt_paths is None or load_cache_image_paths is None

    # Start actual work
    if not skip_load_ckpt_check:
        check_load_ckpts(load_ckpt_paths, workloads, init_resrc_list)

    s_list = launch_servers(
        ports=port_list,
        pwords=pword_list,
        data_dir=data_dir,
        load_ckpt_paths=load_ckpt_paths,
    )

    # pin servers to dedicated CPUs
    if isolate_cpu:
        isolate_proc_cpus(s_list)

    # the redis servers are launched on the same machine as this script, so we
    # only connect to them through localhost
    redis_connections = [
        HopperRedis(host="localhost", port=port, password=pword, verbose=True)
        for (port, pword) in zip(port_list, pword_list)
    ]

    # for sending commands to Redis in parallel
    executor = concurrent.futures.ThreadPoolExecutor()

    if load_mock_image_paths is None:
        load_mock_image_paths = [None] * num_servers
    else:
        load_mock_image_paths = [str(Path(p).absolute()) for p in load_mock_image_paths]

    # wait for redis servers ready to accept TCP
    # then set baseline resources and table
    for r, table in zip(redis_connections, tables):
        r.wait_ready(silent=True)
        r.set_table(table)

    fut_list = []
    for r, workload, load_mock_image_path in zip(
        redis_connections, workloads, load_mock_image_paths
    ):
        if mock_dynamo:
            if load_mock_image_path is not None:
                # if using global_pool, every Redis server must load all data
                load_mock_image_path_list = (
                    load_mock_image_paths if global_pool else [load_mock_image_path]
                )
                # this is very costly, so we do it in parallel
                fut_list.append(
                    executor.submit(
                        r.set_config, "dynamo.mock", "image", *load_mock_image_path_list
                    )
                )
            else:
                assert isinstance(workload, DynamicWorkload)
                k, v = workload.first.key_size, workload.first.val_size
                r.set_config("dynamo.mock", "format", k, v)
        else:
            r.set_config("dynamo.mock", "disable")

    for fut in fut_list:
        fut.result()

    # validate policy.alloc_total_net_bw is consistent
    policy_alloc_total_net_bw = None
    for r in redis_connections:
        r_policy = r.get_config()["policy.alloc_total_net_bw"]
        if policy_alloc_total_net_bw is None:
            policy_alloc_total_net_bw = r_policy
        else:
            assert r_policy == policy_alloc_total_net_bw
    assert policy_alloc_total_net_bw is not None
    if engine.get_policy_alloc_total_net_bw() != policy_alloc_total_net_bw:
        logging.warning(
            "RedisModule and Allocator have different policies for alloc_total_net_bw; "
            f"force Allocator to set policy to {policy_alloc_total_net_bw}"
        )
        engine.set_policy_alloc_total_net_bw(policy_alloc_total_net_bw)

    logger_prefix = f"{exper_namespace}:" if exper_namespace is not None else ""
    engine.setup_logger(
        logger_name=logger_prefix + "alloc_trace",
        log_filename=data_dir / "alloc_trace.log",
        log_level="trace",
    )

    # configure ghost cache ticks; if no special info available, assume kv-pair is 200 B
    if ghost_hint_kv_sizes is None:
        ghost_hint_kv_sizes = [
            workload.first.req_size if isinstance(workload, DynamicWorkload) else 200
            for workload in workloads
        ]

    config_ticks(
        redis_connections,
        req_size_list=ghost_hint_kv_sizes,
        base_resrc=base_resrc,
        num_ticks_list=ghost_num_ticks,
        max_cache_scale=ghost_max_cache_scale,
    )

    # then launch clients
    c_list = []
    for sid, (port, workload, remote_client, pword) in enumerate(
        zip(port_list, workloads, remote_clients, pword_list)
    ):
        clients = launch_clients_remote_or_local(
            remote_client=remote_client,
            remote_path=remote_path,
            num_clients=num_clients,
            workload=str(workload),
            data_dir=str(data_dir),
            batch_size=batch_size,
            duration=duration,
            preheat_duration=preheat_duration,
            host=host,
            ports=[port] if not global_pool else port_list,
            sid=sid,
            verbose=verbose,
            check=check,
            passwords=[pword] if not global_pool else pword_list,
            shard_shift=sid,
            async_queue_depth=async_queue_depth,
            freq=report_freq,
            trace_max_timestamp=trace_max_timestamp,
            trace_max_line=trace_max_line,
            trace_queue_size=trace_queue_size,
        )
        c_list.extend(clients)

    if num_preload > 0:
        # set cache size first... so preload won't cause OOM
        for r, init_resrc in zip(redis_connections, init_resrc_list):
            init_cache_size = init_resrc.to_tuple()[0]
            r.set_resrc(init_cache_size, -1, -1, -1)

        if load_cache_image_paths is None:
            # if load_ckpt_paths specified, only do warmup
            preload_type = "fill" if load_ckpt_paths is None else "warmup"
        else:
            preload_type = "load"

        if global_pool:
            # global_pool does not need warmup/load; the checkpoint can be used as-is
            assert preload_type == "fill", (
                "global_pool is incompatible with preload_warmup or preload_load"
            )

        logging.info("Start preload...")

        if preload_batch_size is None:
            preload_batch_size = batch_size

        p_list = []
        if preload_type == "load":
            for sid, (port, load_cache_image_paths, pword) in enumerate(
                zip(port_list, load_cache_image_paths, pword_list)
            ):
                preload_procs = launch_preload_load(
                    num_preload=num_preload,
                    workload=ImageLoadWorkload(load_cache_image_paths),
                    port=port,
                    sid=sid,
                    batch_size=preload_batch_size,
                    data_dir=data_dir,
                    password=pword,
                )
                p_list.extend(preload_procs)
        else:  # fill or warmup
            for sid, (port, workload, pword) in enumerate(
                zip(port_list, workloads, pword_list)
            ):
                assert isinstance(workload, DynamicWorkload)
                preload_args = {
                    "num_preload": num_preload,
                    "workload": workload.first,
                    "port": port,
                    "sid": sid,
                    "batch_size": preload_batch_size,
                    "data_dir": data_dir,
                    "password": pword,
                }
                preload_procs = (
                    launch_preload_fill(**preload_args)
                    if preload_type == "fill"
                    else launch_preload_warmup(
                        duration=preheat_duration, **preload_args
                    )
                )
                p_list.extend(preload_procs)
        for p in p_list:
            rc = p.wait()
            check_rc(rc, err_msg=f"Preload exits unexpectedly with code {rc}")
        logging.info("Preload completed.")

    # enforce resource limits
    for r, init_resrc in zip(redis_connections, init_resrc_list):
        r.set_resrc(*init_resrc.to_tuple())
    for r, init_resrc in zip(redis_connections, init_resrc_list):
        threshold = max(init_resrc.cache_size, 10 * 1024 * 1024) * 1.05
        r.wait_memory_lower_than(threshold)

    # wait for all servers' rate limiter to refresh
    time.sleep(1)

    # pre-experiment stats dump
    dump_servers_stats(redis_connections, data_dir, "pre_stats")

    tenants = init_alloc(
        redis_connections=redis_connections,
        base_resrc=base_resrc,
        init_resrc_list=init_resrc_list,
        mrc_salt=mrc_salt,
        smooth_window=smooth_window,
    )

    t0 = time.time()
    if global_pool:
        while True:
            ready_count = redis_connections[0].barrier_count()
            if ready_count >= num_clients * num_servers:
                break
            logging.info(f"Wait for clients to be ready; {ready_count=}")
            # poll to check not process in c_list has exits
            for c in c_list:
                if c.poll() is not None:
                    raise RuntimeError(
                        "A client process exited before all clients were ready"
                    )
            time.sleep(1)
    else:
        for sid, r in enumerate(redis_connections):
            while True:
                ready_count = r.barrier_count()
                if ready_count >= num_clients:
                    break
                logging.info(f"Wait for clients of s{sid} to be ready; {ready_count=}")
                for c in c_list:
                    if c.poll() is not None:
                        raise RuntimeError(
                            "A client process exited before all clients were ready"
                        )
                time.sleep(1)
    logging.info(f"All clients are ready after {time.time() - t0:g} seconds")

    if global_pool:
        redis_connections[0].barrier_signal()
    else:
        for r in redis_connections:
            r.barrier_signal()

    if preheat_duration > 0:
        time.sleep(preheat_duration)

    t0: float = time.time()
    # convert relative timestamps into absolute timestamps
    alloc_ts_list = [t0 + ts for ts in alloc_sched if ts < duration]

    # post-preheat stats dump
    dump_servers_stats(redis_connections, data_dir, "preheat_stats")

    # start run allocator
    with open(data_dir / "alloc.csv", "w") as f_alloc:
        f_alloc.write("policy,elapsed,sid,cache_size,db_rcu,db_wcu,net_bw\n")
        if init_resrcs is None:
            # save allocation as the baseline
            for sid in range(num_servers):
                f_alloc.write(
                    f"base,0,{sid},"
                    f"{base_resrc.cache_size:d},{base_resrc.db_rcu:.2f},"
                    f"{base_resrc.db_wcu:.2f},{base_resrc.net_bw:.0f}\n"
                )
        else:
            # initialized resources are pre-configured (likely from another
            # run's decision on the policy)
            # will account into the effective policy's allocation (i.e., the
            # last policy)
            if alloc_configs:
                for sid, init_resrc in enumerate(init_resrc_list):
                    f_alloc.write(
                        f"{alloc_configs[-1]['policy']},0,{sid},"
                        f"{init_resrc.cache_size:d},{init_resrc.db_rcu:.2f},"
                        f"{init_resrc.db_wcu:.2f},{init_resrc.net_bw:.0f}\n"
                    )

        if alloc_ts_list:
            poll_prev_snapshots(tenants)

        # an allocation involves three timestamps:
        # - stat_ts: alloc_ts - alloc_stat_window
        # - alloc_ts: the time to run allocation algorithm
        # - ddl_ts: next alloc_ts; any resource relocation must be completed by
        #     this deadline; for the last allocation, it's experiment duration
        stat_ts_list = [ts - alloc_stat_window for ts in alloc_ts_list]
        next_stat_ts_list = stat_ts_list[1:] + [t0 + duration]
        ddl_ts_list = alloc_ts_list[1:] + [t0 + duration]

        stat_done = False
        for stat_ts, alloc_ts, next_stat_ts, ddl_ts in zip(
            stat_ts_list, alloc_ts_list, next_stat_ts_list, ddl_ts_list
        ):
            assert stat_ts < alloc_ts and alloc_ts < ddl_ts
            if not stat_done:
                # not done by the previous iteration
                # check if necessary to make a pre-poll
                now_ts = time.time()
                sleep_time = stat_ts - now_ts
                if sleep_time < 0:
                    # allow minor miss
                    if sleep_time < -1:
                        logging.warning(
                            "Insufficient statics collection time: "
                            f"should be done at stat_ts={stat_ts - t0:g}, "
                            f"now_ts={now_ts - t0:g}"
                        )
                else:
                    time.sleep(sleep_time)
                poll_prev_snapshots(tenants)
            else:
                stat_done = False  # reset

            now_ts = time.time()
            sleep_time = alloc_ts - now_ts
            if sleep_time < 0:
                logging.error(f"Miss allocation timestamp @{alloc_ts - t0:g}")
                continue
            time.sleep(sleep_time)
            elapsed: int = int(alloc_ts - t0)

            # pre-alloc stats dump
            dump_servers_stats(redis_connections, data_dir, f"alloc_stats@{elapsed}")

            is_ready = pre_alloc_poll(
                tenants=tenants,
                view_filename=data_dir / f"alloc_view@{elapsed}.json",
            )
            if skip_alloc:  # only collect stats without running allocation algorithm
                continue
            if not is_ready:
                for sid in range(len(s_list)):
                    # format: policy,elapsed,sid,cache_size,db_rcu,db_wcu,net_bw
                    f_alloc.write(f"NA,{elapsed:d},{sid},NA,NA,NA,NA\n")
                continue
            assert len(alloc_configs) > 0
            for alloc_config in alloc_configs:
                improve_ratio, alloc_results = run_alloc(
                    tenants,
                    alloc_config["harvest"],
                    alloc_config["conserving"],
                    alloc_config["memshare"],
                )
                for sid, resrc in enumerate(alloc_results):
                    f_alloc.write(
                        f"{alloc_config['policy']},{elapsed:d},{sid},"
                        f"{resrc.cache_size:d},{resrc.db_rcu:.2f},"
                        f"{resrc.db_wcu:.2f},{resrc.net_bw:.0f}\n"
                    )
            if skip_apply:  # only run allocation algorithm without applying decision
                continue

            if alloc_apply_threshold > 0:
                curr_improve_ratio = min(t.estimate_improve_ratio() for t in tenants)
                if improve_ratio < curr_improve_ratio + alloc_apply_threshold:
                    logging.info(
                        f"[@{int(time.time() - t0):d}] "
                        "Skip applying allocation decision due to "
                        "insufficient gain over the current allocation: "
                        f"{curr_improve_ratio * 100:.1f}% -> "
                        f"{improve_ratio * 100:.1f}%"
                    )
                    continue
                else:
                    logging.info(
                        f"[@{int(time.time() - t0):d}] "
                        "Apply allocation decision with significant gain: "
                        f"{curr_improve_ratio * 100:.1f}% -> "
                        f"{improve_ratio * 100:.1f}%"
                    )
            # we next_stat_ts <= alloc_tx, we don't need another stat
            need_next_stat = next_stat_ts > alloc_ts
            stat_done = post_alloc_apply(
                tenants,
                alloc_results,
                boost,
                gradual,
                stat_ts=next_stat_ts if need_next_stat else float("inf"),
                ddl_ts=ddl_ts,
            )
            if not need_next_stat:
                stat_done = True

    wait_clients(c_list, is_remote, num_servers)

    # post-experiment stats dump
    dump_servers_stats(redis_connections, data_dir, "post_stats")

    dump_ckpts(redis_connections, dump_ckpt_paths, workloads, data_dir)

    shutdown_servers(s_list)


def add_parser_args(parser: argparse.ArgumentParser):
    parser.add_argument(
        "workloads",
        help="a list of synthetic or trace-replay workloads. "
        "A synthetic workload is formatted as semicolon-separated strings "
        "`[static_workload][@until_ts]` (until_ts omitted for non-stop workload); "
        "each static_workload is comma-separated strings `arg=value` requiring "
        "following arguments: num_keys (n), key_size (k), val_size (v), "
        "write_ratio (w), distrib (d); `distrib` must be formatted as `seq`, `unif`, "
        "`zipf:$theta`, or `scan:$theta:$max_range`. "
        "A trace-replay workload formatted as `TRACE:[replay_mode]:[trace_filepath]`; "
        "replay_mode can be `timestamp` (submit requests based on the timestamps in "
        "the trace) or `loop` (submit requests in a closed-loop); trace_filepath is "
        "path to a csv file with columns `key,val_size`",
        nargs="+",
        type=str,
    )
    parser.add_argument(
        "--tables",
        help="a list of tables (each corresponds to a server)",
        nargs="+",
        type=str,
        default=[],
    )
    parser.add_argument(
        "-a",
        "--host",
        help="the address for clients to connect (i.e., this machine's address)",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--remote_clients",
        help="a list of ssh host names (set in ~/.ssh/config) that behave as clients (one for each tenant)",
        nargs="+",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--remote_path",
        help="path on the remote machine that contains HopperKV top-level directory",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-t",
        "--duration",
        help="Run time in seconds",
        type=int,
        required=True,
    )
    parser.add_argument(
        "--preheat_duration",
        help="Preheat run time in seconds",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--num_clients",
        help="number of clients for each Redis server (and thus each tenant)",
        type=int,
        default=8,
    )
    parser.add_argument(
        "--async_queue_depth",
        help="Use async interface with given queue depth; 0 = sync mode",
        type=int,
        default=16,
    )
    parser.add_argument(
        "--batch_size",
        help="Number of requests in a batch; 0 = disable batching",
        type=int,
        default=0,
    )
    parser.add_argument(
        "-d",
        "--data_dir",
        help="directory for experiment data",
        type=str,
        required=True,
    )
    parser.add_argument(
        "-b",
        "--base_resrc",
        help="baseline resources <cache_size> <db_rcu> <db_wcu> <net_bw>; support suffix M/m/K/k",
        nargs=4,
        required=True,
        type=str,
    )
    parser.add_argument(
        "-i",
        "--init_resrcs",
        help="a list of resources for every tenant, each formatted as "
        "`<cache_size>,<db_rcu>,<db_wcu>,<net_bw>`; "
        "support suffix M/m/K/k; if not specify, will use base_resrc",
        nargs="*",
        type=str,
    )
    parser.add_argument(
        "--alloc_sched",
        help="schedule to perform allocation; formatted as a list of timestamp in seconds, e.g., 5 10 15",
        nargs="+",
        type=int,
        default=[],
    )
    parser.add_argument(
        "--alloc_sched_rep",
        help="repeated schedule to perform allocation; formatted as `<begin_ts>:<freq>; will overwrite --alloc_sched once provided",
        type=str,
    )
    parser.add_argument(
        "--alloc_stat_window",
        help="time window in seconds to collect statics for allocation",
        type=int,
        default=15,
    )
    parser.add_argument(
        "--report_freq",
        help="frequency of clients reporting performance metrics",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--verbose",
        help="Print logs to stdout",
        action="store_true",
    )
    parser.add_argument(
        "--check",
        help="Check correctness of `GET` results",
        action="store_true",
    )
    parser.add_argument(
        "--mock_dynamo",
        help="Use mocked storage backend instead of real DynamoDB",
        action="store_true",
    )
    parser.add_argument(
        "--isolate_cpu",
        help="Pin servers to disjointed CPUs for isolation",
        action="store_true",
    )
    parser.add_argument(
        "--global_pool",
        help="Any tenants can talk to any servers, "
        "but tenants' working set are still disjointed though sharding; "
        "in this mode, no allocation shall happen: "
        "all tenants shall just compete resources by themselves",
        action="store_true",
    )
    parser.add_argument(
        "--num_preload",
        help="number of preload clients for each Redis server (and thus each tenant); 0 means no preload",
        type=int,
        default=16,
    )
    parser.add_argument(
        "--preload_batch_size",
        help="Number of requests in a batch for preload; default to be same as --batch_size",
        type=int,
    )
    parser.add_argument(
        "--ghost_num_ticks",
        help="Number of ticks to cover the ghost cache range for every tenant; default: 64",
        nargs="+",
        type=int,
    )
    parser.add_argument(
        "--ghost_hint_kv_sizes",
        help="A hint of average key-value pair size in bytes for every tenant; used to "
        "estimate ghost cache size range; if not specified, use the first workload if "
        "synthetic OR assume 200 bytes for trace replay",
        nargs="+",
        type=int,
    )
    parser.add_argument(
        "--ghost_max_cache_scale",
        help="Max cache scale over the baseline cache size for ghost cache",
        default=4,
        type=float,
    )
    parser.add_argument(
        "--boost",
        help="Boost db_rcu/net_bw after allocation until that tenant's cache is fully populated, which can speedup convergence",
        action="store_true",
    )
    parser.add_argument(
        "--gradual",
        help="Gradually apply allocation decision; this reduces the impact of cold cache",
        action="store_true",
    )
    parser.add_argument(
        "--alloc_apply_threshold",
        help="Only apply an allocation decision if the improvement ratio difference (compared to the current allocation) is above this threshold; support '%' suffix",
        type=str,
        default="0%",
    )
    parser.add_argument(
        "--mrc_salt",
        help="Increase the miss ratio curve by `salt`; this enables the system more resilient to miss ratio estimation inaccuracy when the tenant's miss ratio is low; support '%' suffix",
        type=str,
        default="0%",
    )
    parser.add_argument(
        "--smooth_window",
        help="Smooth stat by aggregating over the past few epochs",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--load_ckpt_paths",
        help="A list of path to load checkpoint for every Redis server",
        type=str,
        nargs="+",
        required=False,
    )
    parser.add_argument(
        "--dump_ckpt_paths",
        help="A list of path to dump checkpoint of every Redis server",
        type=str,
        nargs="+",
        required=False,
    )
    # when using trace, use cache images for prehead
    parser.add_argument(
        "--load_cache_image_paths",  # **/cache_image.csv
        help="A list of path to load preheat image for every Redis server",
        type=str,
        nargs="+",
        required=False,
    )
    # when using mock with trace, we return dummy value based on the mock image
    # if --global_pool, will load all images to every Redis server
    parser.add_argument(
        "--load_mock_image_paths",  # **/persist_image.csv
        help="A list of path to load mock image for every Redis server",
        type=str,
        nargs="+",
        required=False,
    )
    parser.add_argument(
        "--skip_alloc",
        help="Only collect and dump stats without actually do allocation in `alloc_sched`",
        action="store_true",
    )
    parser.add_argument(
        "--skip_apply",
        help="Only run allocation algorithm without applying decision in `alloc_sched`; ignored if skip_alloc is set",
        action="store_true",
    )
    parser.add_argument(
        "--trace_max_timestamp",
        help="Maximum timestamp for trace replay (stop processing beyond this)",
        type=float,
        default=float("inf"),
    )
    parser.add_argument(
        "--trace_max_line",
        help="Maximum line number for trace replay (stop processing beyond this)",
        type=float,
        default=float("inf"),
    )
    parser.add_argument(
        "--trace_queue_size",
        help="Queue size for trace replay workload",
        type=int,
        default=1_000_000,
    )
    return parser


def preprocess_args(args_dict: Dict) -> Dict:
    data_dir = Path(args_dict["data_dir"])
    # should be either both None or both not None
    assert (args_dict["remote_clients"] is None) == (args_dict["remote_path"] is None)
    if args_dict["remote_clients"] is not None:
        # clean up remote clients directories before calling prepare_data_dir
        # because it's possible the remote client is localhost
        for remote_client in args_dict["remote_clients"]:
            remote_data_dir = (
                data_dir
                if data_dir.is_absolute()
                else Path(args_dict["remote_path"]) / data_dir
            )
            cmds = [
                ["rm", "-rf", str(remote_data_dir)],
                ["sudo", "killall", "-9", "python3"],
                ["sudo", "killall", "-9", "uv"],
            ]
            for cmd in cmds:
                run_cmd(
                    ["ssh", "-o", "StrictHostKeyChecking=no", remote_client] + cmd,
                    err_panic=False,
                    silent=True,
                )

    prepare_data_dir(data_dir, cleanup=True)
    with open(data_dir / "config.json", "w") as f_config:
        json.dump(args_dict, f_config, indent=2)

    args_dict["data_dir"] = data_dir
    args_dict["base_resrc"] = ResrcTuple.from_str_tuple(tuple(args_dict["base_resrc"]))
    args_dict["workloads"] = [
        TraceReplayWorkload.from_string(workload_str)
        if workload_str.startswith("TRACE:")
        else DynamicWorkload.from_string(workload_str)
        for workload_str in args_dict["workloads"]
    ]
    if args_dict["alloc_sched_rep"] is not None:
        begin_ts, freq = args_dict["alloc_sched_rep"].split(":")
        args_dict["alloc_sched"] = list(
            range(int(begin_ts), args_dict["duration"], int(freq))
        )
    del args_dict["alloc_sched_rep"]
    threshold_s = args_dict["alloc_apply_threshold"]
    args_dict["alloc_apply_threshold"] = (
        float(threshold_s[:-1]) / 100
        if threshold_s.endswith("%")
        else float(threshold_s)
    )
    mrc_salt_s = args_dict["mrc_salt"]
    args_dict["mrc_salt"] = (
        float(mrc_salt_s[:-1]) / 100 if mrc_salt_s.endswith("%") else float(mrc_salt_s)
    )
    assert 0 <= args_dict["mrc_salt"] and args_dict["mrc_salt"] <= 1
    return args_dict


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    parser = argparse.ArgumentParser(
        description="Run an experiment with specified client workloads and allocator configurations"
    )
    add_parser_args(parser)
    args_dict = preprocess_args(vars(parser.parse_args()))

    try:
        main(**args_dict)
    except Exception as e:
        logging.info("Perform cleanup upon failure...")
        run_cmd(["sudo", "killall", "-9", "redis-server"], err_panic=False)
        raise e
