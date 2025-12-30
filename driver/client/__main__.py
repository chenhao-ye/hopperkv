import argparse
import asyncio
import json
import logging
import random
import time
from pathlib import Path
from typing import List

from hopperkv.hopper_redis import HopperRedis

from ..utils import prepare_data_dir
from .adaptor import HopperRedisAdaptor
from .epoch import EpochMgr, LatencyHistMgr
from .workload import (
    DynamicWorkload,
    ReqGenEngine,
    TraceReplayGenEngine,
    TraceReplayWorkload,
)
from .workload.kv_format import KvFormatParams, check_quick, make_val

# global variables to avoid passing around in all functions
g_verbose: bool = False
g_check: bool = False
g_name: str = ""


def check_data_integrity(format_params: KvFormatParams, offset: int, v: str):
    expected_v = make_val(offset, format_params)
    if not check_quick(expected_v, v):
        raise ValueError(f"Miss match `GET` result: expected={expected_v}, actual={v}")


def preheat_sync(
    r_adaptor: HopperRedisAdaptor,
    req_gen: ReqGenEngine,
    preheat_duration: int,
):
    # for now, only do sync version of preheat
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < preheat_duration:
        req = req_gen.make_req()
        if req is None:
            break
        for k, v, offset in req.to_tuples():
            v_ret = r_adaptor.do_work(k, v, offset, None)
            if g_check and v_ret is not None:  # `GET` request
                check_data_integrity(req_gen.req_builder.format_params, offset, v_ret)


async def preheat_async(
    r_adaptor: HopperRedisAdaptor,
    req_gen: ReqGenEngine,
    preheat_duration: int,
    async_queue_depth: int,
):
    t0: float = time.perf_counter()

    async def preheat_task():
        while time.perf_counter() - t0 < preheat_duration:
            req = req_gen.make_req()
            if req is None:
                break
            for k, v, offset in req.to_tuples():
                v_ret = await r_adaptor.do_work_async(k, v, offset, None)
                if g_check and v_ret is not None:  # `GET` request
                    check_data_integrity(
                        req_gen.req_builder.format_params, offset, v_ret
                    )

    tasks = [asyncio.create_task(preheat_task()) for _ in range(async_queue_depth)]
    for t in tasks:
        await t


def run_sync(
    r_adaptor: HopperRedisAdaptor,
    req_gen: ReqGenEngine,
    epoch_mgr: EpochMgr,
    latency_hist_mgr: LatencyHistMgr,
    begin_ts: float,
) -> bool:
    elapsed = -1  # must be set in the loop
    epoch_done = False
    req_gen_done = False
    if isinstance(req_gen, TraceReplayGenEngine):
        req_gen.reset_begin_ts(begin_ts)
    while not epoch_done and not req_gen_done:
        req = req_gen.make_req()
        if req is None:
            break
        for k, v, offset in req.to_tuples():
            v_ret = r_adaptor.do_work(k, v, offset, latency_hist_mgr)
            if g_check and v_ret is not None:  # non-batch `GET` request
                check_data_integrity(req_gen.req_builder.format_params, offset, v_ret)
            epoch_mgr.add_ops(1)

        elapsed = time.perf_counter() - begin_ts
        epoch_done = epoch_mgr.refresh(elapsed)
        req_gen_done = req_gen.is_done(elapsed)
    logging.info(f"{g_name}@{req_gen}: tput={epoch_mgr.report_tput(elapsed):g} req/s")
    return epoch_done


async def run_async(
    r_adaptor: HopperRedisAdaptor,
    req_gen: ReqGenEngine,
    async_queue_depth: int,
    epoch_mgr: EpochMgr,
    latency_hist_mgr: LatencyHistMgr,
    begin_ts: float,
) -> bool:
    epoch_done = False
    req_gen_done = False
    elapsed = -1  # must be set in the loop

    if isinstance(req_gen, TraceReplayGenEngine):
        req_gen.reset_begin_ts(begin_ts)

    async def run_task(task_id):
        nonlocal epoch_done
        nonlocal req_gen_done
        nonlocal elapsed
        while not epoch_done and not req_gen_done:
            req = req_gen.make_req()
            if req is None:
                break
            for k, v, offset in req.to_tuples():
                v_ret = await r_adaptor.do_work_async(k, v, offset, latency_hist_mgr)
                if g_check and v_ret is not None:  # `GET` request
                    check_data_integrity(
                        req_gen.req_builder.format_params, offset, v_ret
                    )
                epoch_mgr.add_ops(1)

            if task_id == 0:  # this task is responsible for refresh epoch
                elapsed = time.perf_counter() - begin_ts
                epoch_done = epoch_mgr.refresh(elapsed)
                req_gen_done = req_gen.is_done(elapsed)

    tasks = [
        asyncio.create_task(run_task(task_id)) for task_id in range(async_queue_depth)
    ]
    for t in tasks:
        await t
    logging.info(f"{g_name}@{req_gen}: tput={epoch_mgr.report_tput(elapsed):g} req/s")

    return epoch_done


def main_sync(
    r_adaptor: HopperRedisAdaptor,
    req_gen_list: List[ReqGenEngine],
    epoch_mgr: EpochMgr,
    latency_hist_mgr: LatencyHistMgr,
    preheat_duration: int,
):
    if preheat_duration > 0:
        logging.info("Start to preheat...")
        preheat_sync(
            r_adaptor=r_adaptor,
            req_gen=req_gen_list[0],
            preheat_duration=preheat_duration,
        )
        logging.info("Preheat completed.")
    begin_ts = time.perf_counter()
    for req_gen in req_gen_list:
        epoch_done = run_sync(
            r_adaptor=r_adaptor,
            req_gen=req_gen,
            epoch_mgr=epoch_mgr,
            latency_hist_mgr=latency_hist_mgr,
            begin_ts=begin_ts,
        )
        if epoch_done:
            break
    for r in r_adaptor.r_list:
        r.close()


async def main_async(
    r_adaptor: HopperRedisAdaptor,
    req_gen_list: List[ReqGenEngine],
    epoch_mgr: EpochMgr,
    latency_hist_mgr: LatencyHistMgr,
    preheat_duration: int,
    async_queue_depth: int,
):
    assert async_queue_depth > 0
    if preheat_duration > 0:
        logging.info("Start to preheat...")
        await preheat_async(
            r_adaptor=r_adaptor,
            req_gen=req_gen_list[0],
            preheat_duration=preheat_duration,
            async_queue_depth=async_queue_depth,
        )
        logging.info("Preheat completed.")
    begin_ts = time.perf_counter()
    for req_gen in req_gen_list:
        epoch_done = await run_async(
            r_adaptor=r_adaptor,
            req_gen=req_gen,
            async_queue_depth=async_queue_depth,
            epoch_mgr=epoch_mgr,
            latency_hist_mgr=latency_hist_mgr,
            begin_ts=begin_ts,
        )
        if epoch_done:
            break
    for r in r_adaptor.r_list:
        await r.close_async()
        r.close()


def main(
    workload: DynamicWorkload | TraceReplayWorkload,
    host: str,
    ports: List[int],
    passwords: List[str] | None,
    batch_size: int,
    count: int | None,
    duration: int,
    preheat_duration: int,
    trace_shard_idx: int,
    trace_num_shards: int,
    data_dir: Path,
    async_queue_depth: int | None,
    freq: int,
    verbose: bool,
    check: bool,
    name: str,
    shard_shift: int,
    seed: int | None,
    trace_max_timestamp: float,
    trace_max_line: int,
    trace_queue_size: int,
):
    # make these variables into global variables; avoid passing around
    global g_verbose
    global g_check
    global g_name
    g_verbose = verbose
    g_check = check
    g_name = name

    if seed is not None:
        random.seed(seed)
    else:
        # force a different seed for different instances of clients
        random.seed(time.time_ns() ^ abs(hash(data_dir)) ^ abs(hash(name)))

    if passwords is None:
        passwords = [None] * len(ports)
    assert len(passwords) == len(ports)

    r_adaptor = HopperRedisAdaptor(
        r_list=[
            HopperRedis(
                enable_async=async_queue_depth is not None,
                host=host,
                port=port,
                password=password,
            )
            for port, password in zip(ports, passwords)
        ],
        batch_size=batch_size,
        shard_shift=shard_shift,
        verbose=verbose,
    )

    logging.info("Start to build request generator...")
    # this can take a while if there is a scan workload
    req_gen_list = (
        workload.build_req_gen(
            trace_shard_idx,
            trace_num_shards,
            max_timestamp=trace_max_timestamp,
            max_line=trace_max_line,
            queue_size=trace_queue_size,
        )
        if isinstance(workload, TraceReplayWorkload)
        else workload.build_req_gen()
    )
    logging.info("Request generator built.")

    with (
        open(data_dir / "data.csv", "w") as f_data,
        open(data_dir / "lat_hist.csv", "w") as f_hist,
    ):
        # the latency in data.csv is only for reference, not actually used for
        # analysis; the real latency distribution is based on the merged
        # lat_hist.csv aggregated across clients; latency unit is us
        f_data.write(
            "timestamp,elapsed,tput,lat_mean,lat_min,lat_max,"
            "p10,p20,p30,p40,p50,p60,p70,p80,p90,p99,p999\n"
        )
        f_hist.write("elapsed,lat_hist_blob\n")

        num_hist = min(60, int((duration + freq - 1) / freq)) if duration > 0 else 60
        latency_hist_mgr = LatencyHistMgr(num_hist, f_hist, freq)
        epoch_mgr = EpochMgr(latency_hist_mgr, f_data, freq, count, duration)

        # wait for run.py to signal start
        logging.info("Wait for the signal to start.")
        r_adaptor.wait_for_signal()
        logging.info("Start to run workload.")

        if async_queue_depth is None or async_queue_depth == 0:
            logging.info("Run in sync mode")
            main_sync(
                r_adaptor=r_adaptor,
                req_gen_list=req_gen_list,
                epoch_mgr=epoch_mgr,
                latency_hist_mgr=latency_hist_mgr,
                preheat_duration=preheat_duration,
            )
        else:
            logging.info(f"Run in async mode with queue depth {async_queue_depth}")
            # batch is not supported in async mode
            asyncio.run(
                main_async(
                    r_adaptor=r_adaptor,
                    req_gen_list=req_gen_list,
                    epoch_mgr=epoch_mgr,
                    latency_hist_mgr=latency_hist_mgr,
                    preheat_duration=preheat_duration,
                    async_queue_depth=async_queue_depth,
                )
            )

        # flush all in-memory histograms
        latency_hist_mgr.flush_until(epoch_mgr.epoch)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # no longer use `localhost` as default argument; force specifying
    parser.add_argument(
        "workload",
        help="a synthetic or trace-replay workload. "
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
        type=str,
    )
    parser.add_argument("-a", "--host", help="host address", type=str)
    parser.add_argument(
        "-p",
        "--ports",
        help="a list of Redis server ports",  # should only be len>1 for global cache
        nargs="+",
        type=int,
        required=True,
    )
    parser.add_argument(
        "--passwords", help="Redis passwords", nargs="+", type=str, required=False
    )
    parser.add_argument(
        "-b",
        "--batch_size",
        help="Number of requests in a batch; 0 = disable batching; not supported in "
        "async mode",
        type=int,
        default=0,
    )
    parser.add_argument(
        "-c",
        "--count",
        help="Number of ops; None means no limit",
        type=int,
        required=False,
    )
    parser.add_argument(
        "-t",
        "--duration",
        help="Run time in seconds; 0 means no limit",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--preheat_duration",
        help="Preheat run time in seconds",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--trace_shard_idx",
        help="Shard index for trace replay (0-based); ignored for non-replay workload",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--trace_num_shards",
        help="Total number of shards for trace replay; ignored for non-replay workload",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--data_dir",
        help="Directory to write measurement data",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--async_queue_depth",
        help="Use async interface with given queue depth; 0 = sync mode",
        type=int,
        required=False,
    )
    parser.add_argument(
        "-q",
        "--freq",
        help="Data reporting frequency in seconds",
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
        help="Check correction of `GET` results",
        action="store_true",
    )
    parser.add_argument(
        "--name",
        help="Name of this client to show in log",
        type=str,
        default="",
    )
    parser.add_argument(
        "--shard_shift",
        help="Parameter to control data sharding",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--seed",
        help="Random seed; useful to study miss ratio curves where a deterministic "
        "input flow are necessary",
        type=int,
        required=False,
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

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )

    args_dict = vars(args)
    data_dir = Path(args.data_dir)

    prepare_data_dir(data_dir)
    with open(data_dir / "config.json", "w") as f_config:
        json.dump(args_dict, f_config, indent=2)

    is_trace = args.workload.startswith("TRACE:")
    if is_trace:
        # preheat is not compatible with trace replay
        assert args.preheat_duration == 0

    args_dict["workload"] = (
        TraceReplayWorkload.from_string(args.workload)
        if is_trace
        else DynamicWorkload.from_string(args.workload)
    )
    args_dict["data_dir"] = data_dir

    main(**args_dict)
