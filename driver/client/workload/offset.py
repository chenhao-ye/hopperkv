import math
import random
from bisect import bisect_left
from typing import List

import xxhash


class OffsetMgr:
    def __init__(self) -> None:
        pass

    def get_offset(self) -> int:
        raise NotImplementedError()


class WorkingSetOffsetMgr(OffsetMgr):
    """with finite working set"""

    def __init__(self, ws_size: int) -> None:
        super().__init__()
        self.ws_size = ws_size

    def set_ws_size(self, ws_size: int) -> None:
        # NOTE: all subclass must support to set size at runtime (i.e., after
        # calling get_offset)
        self.ws_size = ws_size

    def get_offset(self) -> int:
        raise NotImplementedError()


class SeqOffsetMgr(WorkingSetOffsetMgr):
    def __init__(self, ws_size: int) -> None:
        super().__init__(ws_size)
        self.next_offset = 0

    def get_offset(self) -> int:
        offset = self.next_offset % self.ws_size
        self.next_offset += 1
        return offset


class UnifRndOffsetMgr(WorkingSetOffsetMgr):
    def __init__(self, ws_size: int) -> None:
        super().__init__(ws_size)

    # set_ws_size can just inherent from WorkingSetOffsetMgr

    def get_offset(self) -> int:
        return random.randrange(self.ws_size)


# code is ported from [DBx1000](https://github.com/yxymit/DBx1000).
# here we twist the code a little bit: the original code produces offset
# ranging from 1 to n. Here we make it from 0 to n - 1.
class ZipfGtor:
    def __init__(self, n: int, theta: float) -> None:
        self.n = n
        self.theta = theta
        self.denom = self.zeta(n, theta)
        self.eta = (1 - math.pow(2.0 / self.n, 1 - self.theta)) / (
            1 - self.zeta(2, theta) / self.denom
        )
        self.alpha = 1 / (1 - self.theta)

    def zeta(self, n: int, theta: float) -> float:
        return sum(math.pow(1.0 / i, theta) for i in range(1, n + 1))

    def zipf(self) -> int:
        u = random.random()
        uz = u * self.denom
        if uz < 1:
            return 0
        if uz < 1 + math.pow(0.5, self.theta):
            return 1
        return int(self.n * math.pow(self.eta * u - self.eta + 1, self.alpha))


class ZipfRndOffsetMgr(WorkingSetOffsetMgr):
    def __init__(self, ws_size: int, theta: float) -> None:
        super().__init__(ws_size)
        self.theta = theta
        self._make_zipf_gtor()

    def _make_zipf_gtor(self) -> None:
        # zipf generator must be remade after setting working set size or theta
        self.zipf_gtor = ZipfGtor(self.ws_size, self.theta)

    def set_ws_size(self, ws_size: int) -> None:
        self.ws_size = ws_size
        self._make_zipf_gtor()

    def set_theta(self, theta: int) -> None:
        self.theta = theta
        self._make_zipf_gtor()

    def get_offset(self) -> int:
        return self.zipf_gtor.zipf()


# to avoid redundant creation of sorted range to save memory
# assume the same sort_func!
_scan_sorted_range_cache = {}


class ScanRangeOffsetMgr(ZipfRndOffsetMgr):
    # this is designed for YCSB-E workload
    def __init__(self, ws_size: int, theta: float, max_range: int) -> None:
        super().__init__(ws_size, theta)
        self.max_range = max_range
        self.sort_func = lambda x: xxhash.xxh32_intdigest(
            x.to_bytes(8, byteorder="big")
        )
        cached_sorted_range = _scan_sorted_range_cache.get(ws_size)
        if cached_sorted_range is None:
            cached_sorted_range = sorted(range(ws_size), key=self.sort_func)
            _scan_sorted_range_cache[ws_size] = cached_sorted_range
        self.sorted_range = cached_sorted_range

    def set_ws_size(self, ws_size: int) -> None:
        super().set_ws_size(ws_size)
        self.sorted_range = sorted(range(ws_size), key=self.sort_func)

    def _scan(self, begin_offset, size):
        # for millions of keys, this can take a few microseconds
        begin_idx = bisect_left(
            self.sorted_range, self.sort_func(begin_offset), key=self.sort_func
        )
        while self.sorted_range[begin_idx] < begin_offset and self.sort_func(
            self.sorted_range[begin_idx]
        ) == self.sort_func(begin_offset):
            # very unlikely: hash collision
            begin_idx += 1
        return [
            self.sorted_range[(begin_idx + i) % len(self.sorted_range)]
            for i in range(size)
        ]

    def get_offset(self) -> List[int]:
        scan_size = random.randint(1, self.max_range)
        offset_begin = self.zipf_gtor.zipf()
        return self._scan(offset_begin, scan_size)
