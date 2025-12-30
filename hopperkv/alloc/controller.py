import json
import logging
import time
from pathlib import Path
from typing import List, Tuple

from ..hopper_redis import HopperRedis
from .engine import Allocator, ResrcVec
from .resrc import ResrcTuple
from .tenant import Tenant


def init_alloc(
    *,
    redis_connections: List[HopperRedis],
    base_resrc: ResrcTuple,
    init_resrc_list: List[ResrcTuple],
    mrc_salt: float = 0,
    smooth_window: int = 1,
) -> List[Tenant]:
    tenants: List[Tenant] = [
        Tenant(
            tid=tid,
            r=r,
            base_resrc=base_resrc,
            init_resrc=init_resrc,
            mrc_salt=mrc_salt,
            smooth_window=smooth_window,
        )
        for tid, (r, init_resrc) in enumerate(zip(redis_connections, init_resrc_list))
    ]
    return tenants


def poll_prev_snapshots(tenants: List[Tenant]):
    for t in tenants:
        t.poll_prev_snapshot()


def poll_post_snapshots(tenants: List[Tenant]) -> bool:
    is_ready = True
    for t in tenants:
        try:
            t.poll_post_snapshot()
        except ValueError:
            is_ready = False
    return is_ready


def pre_alloc_poll(tenants: List[Tenant], view_filename: Path | str | None) -> bool:
    is_ready = poll_post_snapshots(tenants)
    if not is_ready:
        logging.warning(
            "Terminate allocation because at least one tenant has made no progress"
        )
        return False

    if view_filename is not None:
        with open(view_filename, "w") as f_view:
            json.dump([t.dump() for t in tenants], f_view, indent=2)
    return True


def post_alloc_apply(
    tenants: List[Tenant],
    alloc_results: List[ResrcTuple],
    boost: bool,
    gradual: bool,
    # if beyond this ts, poll a stat; inf means ignored (for direct apply)
    stat_ts: float = float("inf"),
    # try best to return before the deadline timestamp; inf means ignored (for direct apply)
    ddl_ts: float = float("inf"),
) -> bool:  # return whether stat has been polled
    assert not (boost and gradual)
    if not boost and not gradual:
        direct_apply(tenants, alloc_results)
        return False
    else:
        if boost:
            return boost_apply(tenants, alloc_results, stat_ts=stat_ts, ddl_ts=ddl_ts)
        elif gradual:
            return gradual_apply(tenants, alloc_results, stat_ts=stat_ts, ddl_ts=ddl_ts)


def direct_apply(tenants: List[Tenant], alloc_results: List[ResrcTuple]):
    for t, resrc in zip(tenants, alloc_results):
        logging.info(f"Tenant-{t.tid}: {resrc}")
        t.apply_resrc(resrc)


def boost_apply(
    tenants: List[Tenant],
    alloc_results: List[ResrcTuple],
    stat_ts: float,  # if beyond this ts, poll a stat
    ddl_ts: float,  # try best to return before the deadline timestamp
    poll_freq: float = 1,
) -> bool:
    stat_done = False
    pending_tenants = []
    for t, resrc in zip(tenants, alloc_results):
        logging.info(f"Tenant-{t.tid}: {resrc}")
        is_applied = t.try_apply_resrc_with_boost(resrc)
        if not is_applied:
            pending_tenants.append(t)

    if not pending_tenants:
        return stat_done
    begin_ts = time.time()
    while pending_tenants:
        now_ts = time.time()
        if now_ts + poll_freq > ddl_ts:
            # will reach timeout if continue
            logging.info(
                f"Boosting incompleted due to timeout after {now_ts - begin_ts:g} seconds"
            )
            for t in pending_tenants:
                t.apply_last_pending_resrc()
            return stat_done
        if not stat_done and now_ts > stat_ts:
            poll_prev_snapshots(tenants)
            stat_done = True
        time.sleep(poll_freq)
        still_pending = []
        for t in pending_tenants:
            if t.is_cache_warm():
                t.apply_next_pending_resrc()
            else:
                still_pending.append(t)
        pending_tenants = still_pending

    logging.info(f"Complete boosting after {now_ts - begin_ts:g} seconds")
    return stat_done


def gradual_apply(
    tenants: List[Tenant],
    alloc_results: List[ResrcTuple],
    stat_ts: float,  # if beyond this ts, poll a stat
    ddl_ts: float,  # try best to return before the deadline timestamp
    max_cache_reloc_each: int = 16 * 1024 * 1024,
    poll_freq: float = 0.5,
) -> bool:
    assert stat_ts <= ddl_ts or stat_ts == float("inf")
    stat_done = False
    max_cache_delta = max(
        abs(resrc.cache_size - t.curr_alloc_resrc.cache_size)
        for t, resrc in zip(tenants, alloc_results)
    )
    num_rounds = int(max_cache_delta / max_cache_reloc_each) + 1
    for t, resrc in zip(tenants, alloc_results):
        delta_resrc = resrc - t.curr_alloc_resrc
        for round in range(num_rounds - 1):
            t.add_pending_resrc(
                t.curr_alloc_resrc + delta_resrc * ((round + 1) / num_rounds)
            )
        t.add_pending_resrc(resrc)

    # apply first round (does not need to wait for cache warm)
    for t in tenants:
        t.apply_next_pending_resrc()

    if num_rounds == 1:
        return stat_done

    begin_ts = time.time()
    for _ in range(num_rounds - 1):
        pending_tenants = [t for t in tenants if not t.is_cache_warm()]
        while pending_tenants:
            now_ts = time.time()
            if now_ts + poll_freq > ddl_ts:
                logging.info(
                    "Gradual allocation incompleted due to timeout "
                    f"after {now_ts - begin_ts:g} seconds"
                )
                for t in tenants:
                    t.clear_pending_resrc()
                return stat_done
            if not stat_done and now_ts > stat_ts:
                poll_prev_snapshots(tenants)
                stat_done = True
            time.sleep(poll_freq)
            pending_tenants = [t for t in pending_tenants if not t.is_cache_warm()]
        # wait for all cache warm before apply the next round
        for t in tenants:
            t.apply_next_pending_resrc()
    logging.info(
        f"Complete gradual resource relocation after {time.time() - begin_ts:g} seconds"
    )
    return stat_done


def run_alloc(
    tenants: List[Tenant],
    policy_alloc_harvest: bool,
    policy_alloc_conserving: bool,
    policy_alloc_memshare: bool,
) -> Tuple[float, List[ResrcTuple]]:
    allocator = Allocator(
        policy_alloc_harvest, policy_alloc_conserving, policy_alloc_memshare
    )
    for t in tenants:
        allocator.add_tenant(
            t.demand_if_miss, t.base_resrc.to_vec(), t.mrc, t.net_bw_alpha
        )

    improve_ratio = allocator.do_alloc()
    alloc_results: List[ResrcVec] = allocator.get_alloc_result()
    return improve_ratio, [ResrcTuple.from_vec(resrc) for resrc in alloc_results]
