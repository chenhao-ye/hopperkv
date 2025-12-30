import argparse
import json
import logging
import time

from hopperkv.hopper_redis import HopperRedis

from .workload import ImageLoadWorkload, StaticWorkload
from .workload.synthetic_workload import OffsetReqBuilder


def do_preload(
    r: HopperRedis, k: str, v: str, batch: bool = False, verbose: bool = False
):
    assert v is not None
    if batch:
        r.set_cache_only_batch(k, v)
    else:
        res = r.set_cache_only(k, v)
        if verbose:
            logging.debug(
                f"DONE: HOPPER.SETC {k} {v if len(v) < 16 else f'{v[:16]}...'} -> {res}"
            )


def main_fill(
    r: HopperRedis,
    workload: StaticWorkload,
    stride: int,
    stride_shift: int,
    batch_size: int,
    verbose: bool,
    no_reverse: bool,
):
    assert stride_shift < stride

    # since we want to manage offset by ourself, we cannot use req_gen
    req_builder = OffsetReqBuilder(workload)

    begin_offset = 0
    while begin_offset % stride != stride_shift:
        begin_offset += 1

    offset_iter = range(begin_offset, workload.num_keys, stride)
    if not no_reverse:
        offset_iter = reversed(offset_iter)

    t0: float = time.perf_counter()
    cnt = 0
    for offset in offset_iter:
        assert offset % stride == stride_shift
        req = req_builder.make_req(offset)
        if req is None:
            break
        for k, v, _ in req.to_tuples():
            do_preload(r=r, k=k, v=v, batch=batch_size > 0, verbose=verbose)
            cnt += 1
            if batch_size > 0 and cnt % batch_size == 0:
                r.exec_batch_flush()
    t1 = time.perf_counter()
    r.close()
    logging.info(
        f"Complete preload-fill [{workload},{stride},{stride_shift}] "
        f"in {t1 - t0:g} seconds (reverse={not no_reverse})"
    )


def main_warmup(
    r: HopperRedis,
    workload: StaticWorkload,
    duration: int,
    batch_size: int,
    verbose: bool,
):
    req_gen = workload.build_req_gen()[0]
    t0 = time.perf_counter()
    cnt = 0
    while time.perf_counter() - t0 < duration:
        req = req_gen.make_req()
        if req is None:
            break
        for k, v, _ in req.to_tuples():
            do_preload(r=r, k=k, v=v, batch=batch_size > 0, verbose=verbose)
            cnt += 1
            if batch_size > 0 and cnt % batch_size == 0:
                r.exec_batch_flush()
    r.close()
    logging.info(f"Complete preload-warmup [{req_gen}] for {duration} seconds")


def main_load(
    r: HopperRedis,
    workload: ImageLoadWorkload,
    stride: int,
    stride_shift: int,
    batch_size: int,
    verbose: bool,
):
    assert stride_shift < stride

    req_gen = workload.build_req_gen(
        image_shard_idx=stride_shift, image_num_shards=stride
    )[0]
    t0 = time.perf_counter()
    cnt = 0
    while not req_gen.is_done():
        req = req_gen.make_req()
        if req is None:
            break
        for k, v, _ in req.to_tuples():
            do_preload(r=r, k=k, v=v, batch=batch_size > 0, verbose=verbose)
            cnt += 1
            if batch_size > 0 and cnt % batch_size == 0:
                r.exec_batch_flush()
    t1 = time.perf_counter()
    r.close()
    logging.info(f"Complete preload-load [{req_gen}] for {t1 - t0:g} seconds")


def main(
    mode: str,
    host: str,
    port: int,
    workload: StaticWorkload,
    duration: int | None,
    password: str | None,
    stride: int,
    stride_shift: int,
    batch_size: int,
    verbose: bool,
    no_reverse: bool,
):
    r = HopperRedis(enable_async=False, host=host, port=port, password=password)
    r.wait_ready()
    workload.write_ratio = 1

    if mode == "fill":
        if workload is not None:
            logging.warning("Ignore `workload` in fill mode")
        if duration is not None:
            logging.warning("Ignore `duration` in fill mode")
        main_fill(
            r=r,
            workload=workload,
            stride=stride,
            stride_shift=stride_shift,
            batch_size=batch_size,
            verbose=verbose,
            no_reverse=no_reverse,
        )
    elif mode == "warmup":
        main_warmup(
            r=r,
            workload=workload,
            duration=duration,
            batch_size=batch_size,
            verbose=verbose,
        )
    elif mode == "load":
        main_load(
            r=r,
            workload=workload,
            stride=stride,
            stride_shift=stride_shift,
            batch_size=batch_size,
            verbose=verbose,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        help="either `fill`, `warmup`, `load`; fill mode will load data sequentially "
        "(default in reverse order so that small offset keys in more recent); "
        "warmup mode access data following the given workload (commonly used with RDB "
        "checkpoint recovery); load mode will load data from cache_image.csv",
        choices=["fill", "warmup", "load"],
    )
    parser.add_argument(
        "workload",
        help="Workload formatted as a comma-separated string `arg=value` (fill/warmup) "
        "OR `IMAGE:[image_path]` (load); "
        "`write_ratio` and `distrib` are ignored in fill mode",
        type=str,
    )
    # this field should really be `localhost` only
    parser.add_argument(
        "-a", "--host", help="host address", type=str, default="localhost"
    )
    parser.add_argument(
        "-p",
        "--port",
        help="Redis server port",
        type=int,
        required=True,
    )
    parser.add_argument(
        "-t",
        "--duration",
        help="Duration in seconds; ignored in fill/load mode; required in warmup mode",
        type=int,
        required=False,
    )
    parser.add_argument("--password", help="Redis password", type=str, required=False)
    parser.add_argument(
        "--batch_size",
        help="Number of requests in a batch; 0 = disable batching",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--stride",
        help="load data based on the key offset with the given stride; "
        "ignored in warmup mode",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--stride_shift",
        help="only load data that match `offset %% stride == stride_shift`; "
        "ignored in warmup mode",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--verbose",
        help="Print logs to stdout",
        action="store_true",
    )
    parser.add_argument(
        "--no_reverse",
        help="By default, will load data in reverse order (useful for Zipfian, where "
        "smaller offset is hotter and should be more fresh in LRU list); if set, will "
        "disable reverse loading; ignored in warmup/load mode",
        action="store_true",
    )

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    logging.info(json.dumps(vars(args), indent=2))

    arg_dict = vars(args)
    arg_dict["workload"] = (
        StaticWorkload.from_string(args.workload)
        if args.mode != "load"
        else ImageLoadWorkload.from_string(args.workload)
    )

    main(**arg_dict)
