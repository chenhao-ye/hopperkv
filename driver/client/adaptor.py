import logging
import time
from typing import List

from hopperkv.hopper_redis import HopperRedis

from .epoch import LatencyHistMgr


class HopperRedisAdaptor:
    def __init__(
        self,
        r_list: List[HopperRedis],
        batch_size: int,
        shard_shift: int,
        verbose: bool,
    ):
        self.r_list = r_list
        self.batch_size = batch_size
        self.batch_cnts: List[int] = [0 for _ in r_list]
        self.shard_shift = shard_shift
        self.verbose = verbose

    def get_idx(self, offset: int) -> int:
        return (offset + self.shard_shift) % len(self.r_list)

    def wait_for_signal(self):
        # always use the first redis instance to wait for signal
        self.r_list[0].barrier_wait()

    def do_work(
        self,
        k: str,
        v: str | None,
        offset: int,
        latency_hist_mgr: LatencyHistMgr | None,
    ) -> str | None:
        r_idx = self.get_idx(offset)
        r = self.r_list[r_idx]
        ret = None  # only non-batch SET will return value to caller
        if self.batch_size > 0:  # only enqueue into pipeline
            if v is None:
                r.get_batch(k)
            else:
                r.set_batch(k, v)
            self.batch_cnts[r_idx] += 1
            if self.batch_cnts[r_idx] % self.batch_size == 0:
                t_begin = time.perf_counter()
                r.exec_batch_flush()
                t_end = time.perf_counter()
                if latency_hist_mgr is not None:
                    latency_hist_mgr.record_latency((t_end - t_begin) * 1000_000)
        else:
            t_begin = time.perf_counter()
            if v is None:
                ret = r.get(k)  # will return this to caller
                if self.verbose:
                    logging.debug(f"DONE: HOPPER.GET {k} -> {self._digest(ret)}")
            else:
                _ret = r.set(k, v)  # no to return back to caller
                if self.verbose:
                    logging.debug(f"DONE: HOPPER.SET {k} {self._digest(v)} -> {_ret}")
            t_end = time.perf_counter()
            if latency_hist_mgr is not None:
                latency_hist_mgr.record_latency((t_end - t_begin) * 1000_000)
        return ret

    async def do_work_async(
        self,
        k: str,
        v: str | None,
        offset: int,
        latency_hist_mgr: LatencyHistMgr | None,
    ):
        r = self.r_list[self.get_idx(offset)]
        t_begin = time.perf_counter()
        ret = None
        if v is None:
            ret = await r.get_async(k)
            if self.verbose:
                logging.debug(f"<async> DONE: HOPPER.GET {k} -> {self._digest(ret)}")
        else:
            _ret = await r.set_async(k, v)
            if self.verbose:
                logging.debug(
                    f"<async> DONE: HOPPER.SET {k} {self._digest(v)} -> {_ret}"
                )
        t_end: float = time.perf_counter()
        if latency_hist_mgr is not None:
            latency_hist_mgr.record_latency((t_end - t_begin) * 1000_000)
        return ret

    @staticmethod
    def _digest(v: str, maxlen: int = 16) -> str:
        return f"{v if len(v) < maxlen else f'{v[:maxlen]}...'}"
