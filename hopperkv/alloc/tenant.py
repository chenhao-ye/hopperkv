import functools
import logging
import operator
import time
from collections import deque
from typing import Dict, List

from ..hopper_redis import HopperRedis
from .engine import (
    MissRatioCurve,
    StatelessResrcVec,
    get_min_cache_size,
    get_min_db_rcu,
    get_min_db_wcu,
    get_min_net_bw,
)
from .resrc import ResrcStat, ResrcTuple


class Tenant:
    def __init__(
        self,
        *,
        tid: int,
        r: HopperRedis,
        base_resrc: ResrcTuple,
        init_resrc: ResrcTuple,
        # add to miss ratio, making it more resilient to estimation inaccuracy
        mrc_salt: float = 0,
        # use the past few epochs's aggregated stats
        smooth_window: int = 1,
    ):
        self.tid = tid
        self.r = r
        self.base_resrc = base_resrc
        self.prev_snapshot = ResrcStat()
        self.epoch_stat_window: deque[ResrcStat] = deque()
        self.mrc: MissRatioCurve | None = None
        self.ghost_ticks: List[int] | None = None
        self.ghost_miss_ratios: List[float] | None = None
        self.demand_if_miss = None
        # alpha is defined as: if for a miss, the consumption is d; then for a
        # miss, the consumption is (1 - alpha) * d
        # in other words, alpha is defined as:
        #   1 - (consumption_if_miss / consumption_is_hit)
        self.net_bw_alpha = None
        # current allocated resources (used for boosting decision)
        self.curr_alloc_resrc: ResrcTuple = init_resrc
        # when enabling boost or gradual, the allocation decision is considered
        # pending
        self.pending_resrc_queue: deque[ResrcTuple] = deque()
        self.mrc_salt = mrc_salt
        self.smooth_window = smooth_window

    def __str__(self) -> str:
        return f"Tenant-{self.tid}"

    def estimate_tput(self, resrc: ResrcTuple) -> float:
        mr = self.mrc.get_miss_ratio(resrc.cache_size)
        rcu_demand, wcu_demand, net_demand = self.demand_if_miss.to_tuple()
        rcu_tput = (
            resrc.db_rcu / rcu_demand / mr
            if rcu_demand != 0 and mr != 0
            else float("inf")
        )
        wcu_tput = resrc.db_wcu / wcu_demand if wcu_demand != 0 else float("inf")
        net_tput = (
            (
                resrc.net_bw
                / net_demand
                / (1 - self.net_bw_alpha + self.net_bw_alpha * mr)
            )
            if net_demand != 0
            else float("inf")
        )
        return min(rcu_tput, wcu_tput, net_tput)

    def estimate_improve_ratio(self) -> float:
        base_tput = self.estimate_tput(self.base_resrc)
        curr_tput = self.estimate_tput(self.curr_alloc_resrc)
        return curr_tput / base_tput - 1

    def poll(self) -> ResrcStat:
        stats_res = self.r.stats()
        # logging.debug(f"{self}: Stats={stats_res}")
        self.ghost_ticks = stats_res["ghost.ticks"]
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
        resrc_stat.timestamp = time.perf_counter()
        return resrc_stat

    def poll_prev_snapshot(self):
        self.prev_snapshot = self.poll()

    def poll_post_snapshot(self):
        # by default, will replace prev_snapshot with the current one, so it's
        # legal to continually call poll_post_snapshot
        curr_snapshot = self.poll()
        epoch_stat = curr_snapshot - self.prev_snapshot
        self.prev_snapshot = curr_snapshot

        if len(self.epoch_stat_window) >= self.smooth_window:
            self.epoch_stat_window.popleft()
        self.epoch_stat_window.append(epoch_stat)
        sum_stat = functools.reduce(operator.add, self.epoch_stat_window)

        assert self.ghost_ticks is not None
        assert sum_stat.is_valid(), str(sum_stat)

        self.ghost_miss_ratios = [
            min(mc / (hc + mc) + self.mrc_salt, 1)
            for hc, mc in zip(sum_stat.ghost_hit_cnt, sum_stat.ghost_miss_cnt)
        ]
        # over the time, len(ghost_ticks) may grow if the working set grows
        # but len(ghost_miss_ratios) should grow together
        assert len(self.ghost_ticks) == len(self.ghost_miss_ratios)
        self.mrc = MissRatioCurve(self.ghost_ticks, self.ghost_miss_ratios)
        self.demand_if_miss = StatelessResrcVec(
            sum_stat.db_rcu_consump_if_miss / sum_stat.req_cnt,
            sum_stat.db_wcu_consump / sum_stat.req_cnt,
            sum_stat.net_bw_consump_if_miss / sum_stat.req_cnt,
        )
        self.net_bw_alpha = 1 - (
            sum_stat.net_bw_consump_if_hit / sum_stat.net_bw_consump_if_miss
        )

    def try_apply_resrc_with_boost(self, resrc: ResrcTuple) -> bool:
        assert not self.pending_resrc_queue  # must no pending
        resrc = self._enforce_min_resrc(resrc)
        if resrc.cache_size <= self.curr_alloc_resrc.cache_size:  # no need to boost
            self.apply_resrc(resrc)
            return True
        self.add_pending_resrc(resrc, check_enforce=False)
        self.apply_resrc(
            ResrcTuple(
                resrc.cache_size,
                max(resrc.db_rcu, self.base_resrc.db_rcu),
                resrc.db_wcu,  # no boost on db_wcu
                max(resrc.net_bw, self.base_resrc.net_bw),
            )
        )
        logging.info(
            f"Tenant-{self.tid}: Boost allocation: {self.curr_alloc_resrc}, "
            f"pending=[{', '.join([str(resrc) for resrc in self.pending_resrc_queue])}]"
        )
        return False

    def apply_next_pending_resrc(self):
        assert self.pending_resrc_queue
        next_pending_resrc = self.pending_resrc_queue.popleft()
        self.apply_resrc(next_pending_resrc)
        logging.info(
            f"Tenant-{self.tid}: Apply pending allocation: {next_pending_resrc}"
        )

    def apply_last_pending_resrc(self):
        # apply the last pending allocation and clear the pending queue
        assert self.pending_resrc_queue
        last_pending_resrc = self.pending_resrc_queue.pop()
        self.apply_resrc(last_pending_resrc)
        logging.info(
            f"Tenant-{self.tid}: Apply last pending allocation: {last_pending_resrc}"
        )
        self.clear_pending_resrc()

    def clear_pending_resrc(self):
        self.pending_resrc_queue.clear()

    def add_pending_resrc(self, resrc: ResrcTuple, check_enforce: bool = True) -> bool:
        if check_enforce:
            resrc = self._enforce_min_resrc(resrc)
        self.pending_resrc_queue.append(resrc)
        # logging.info(f"Tenant-{self.tid}: Add pending allocation: {resrc}")

    def is_cache_warm(self, threshold=0.97) -> bool:
        curr_mem = self.r.memory_stats()["total.allocated"]
        is_populated = curr_mem > self.curr_alloc_resrc.cache_size * threshold
        if is_populated:
            return True
        is_working_set_fit = self.mrc.get_miss_ratio(curr_mem) < 0.01
        return is_working_set_fit

    def dump(self) -> Dict:  # as a json-friendly json
        return {
            "tid": self.tid,
            "base_resrc": self.base_resrc.to_tuple(),
            "prev_snapshot": self.prev_snapshot.dump(),
            "epoch_stat": functools.reduce(operator.add, self.epoch_stat_window).dump(),
            # "epoch_stat_window": [
            #     epoch_stat.dump() for epoch_stat in self.epoch_stat_window
            # ],
            "ghost_ticks": self.ghost_ticks,
            "ghost_miss_ratios": self.ghost_miss_ratios,
            "demand_if_miss": (
                self.demand_if_miss.to_tuple()
                if self.demand_if_miss is not None
                else None
            ),
            "net_bw_alpha": self.net_bw_alpha,
            "curr_alloc_resrc": self.curr_alloc_resrc.to_tuple(),
            "pending_resrc_queue": [
                resrc.to_tuple() for resrc in self.pending_resrc_queue
            ],
        }

    def apply_resrc(self, resrc: ResrcTuple) -> bool:
        self.curr_alloc_resrc = resrc
        self.r.set_resrc(*resrc.to_tuple())

    def _enforce_min_resrc(self, resrc: ResrcTuple) -> ResrcTuple:
        cache_size, db_rcu, db_wcu, net_bw = resrc.to_tuple()
        min_cache_size = get_min_cache_size()
        if cache_size < min_cache_size:
            logging.debug(
                f"{self}: <cache_size> allocated={cache_size:g} is lower than "
                f"min_threshold={min_cache_size}; will enforce the threshold"
            )
            cache_size = min_cache_size

        min_db_rcu = get_min_db_rcu()
        if db_rcu < min_db_rcu:
            logging.debug(
                f"{self}: <db_rcu> allocated={db_rcu:g} is lower than "
                f"min_threshold={min_db_rcu}; will enforce the threshold"
            )
            db_rcu = min_db_rcu

        min_db_wcu = get_min_db_wcu()
        if db_wcu < min_db_wcu:
            logging.debug(
                f"{self}: <db_wcu> allocated={db_wcu:g} is lower than "
                f"min_threshold={min_db_wcu}; will enforce the threshold"
            )
            db_wcu = min_db_wcu

        min_net_bw = get_min_net_bw()
        if net_bw < min_net_bw:
            logging.debug(
                f"{self}: <net_bw> allocated={net_bw:g} is lower than "
                f"min_threshold={min_net_bw}; will enforce the threshold"
            )
            net_bw = min_net_bw
        return ResrcTuple(cache_size, db_rcu, db_wcu, net_bw)
