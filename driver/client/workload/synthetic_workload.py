import random
from dataclasses import dataclass
from typing import List

from hopperkv.utils import str_cast_type

from .base import Req, ReqGenEngine, Workload
from .distrib import BaseDistrib, ScanDistrib, SeqDistrib, UnifDistrib, ZipfDistrib
from .kv_format import KvFormatParams, get_format_params, make_key, make_val
from .offset import (
    OffsetMgr,
    ScanRangeOffsetMgr,
    SeqOffsetMgr,
    UnifRndOffsetMgr,
    ZipfRndOffsetMgr,
)


@dataclass
class StaticWorkload(Workload):
    key_size: int
    val_size: int
    num_keys: int
    write_ratio: float | None = None
    distrib: BaseDistrib | None = None

    @property
    def req_size(self):
        return self.key_size + self.val_size

    def copy(self):
        return StaticWorkload(
            key_size=self.key_size,
            val_size=self.val_size,
            num_keys=self.num_keys,
            write_ratio=self.write_ratio,
            distrib=self.distrib,
        )

    @classmethod
    def from_string(cls, s: str, allow_dup: bool = False):
        args = {}
        for field in s.split(","):
            field = field.strip()
            k, v = field.split("=", 1)
            if k in {"n", "num_keys"}:
                assert allow_dup or "num_keys" not in args
                args["num_keys"] = int(str_cast_type(v, float, binary_scale=False))
            elif k in {"k", "key_size"}:
                assert allow_dup or "key_size" not in args
                args["key_size"] = int(v)
            elif k in {"v", "val_size"}:
                assert allow_dup or "val_size" not in args
                args["val_size"] = int(v)
            elif k in {"w", "write_ratio"}:
                assert allow_dup or "write_ratio" not in args
                args["write_ratio"] = float(v)
            elif k in {"d", "distrib"}:
                assert allow_dup or "distrib" not in args
                if v == "seq":
                    args["distrib"] = SeqDistrib()
                elif v == "unif":
                    args["distrib"] = UnifDistrib()
                elif v.startswith("zipf:"):
                    args["distrib"] = ZipfDistrib(float(v[5:]))
                elif v.startswith("scan:"):
                    _, theta, max_range = v.split(":")
                    args["distrib"] = ScanDistrib(float(theta), int(max_range))
                else:
                    raise ValueError(f"Unknown distrib: {v}")
            else:
                raise ValueError(f"Unknown field: {k}={v}")
        return cls(**args)

    def __str__(self):
        fields = [f"k={self.key_size}", f"v={self.val_size}", f"n={self.num_keys}"]
        if self.write_ratio is not None:
            fields.append(f"w={self.write_ratio}")
        if self.distrib is not None:
            fields.append(f"d={self.distrib}")
        return ",".join(fields)

    def build_req_gen(self) -> List[ReqGenEngine]:
        return [OffsetReqGenEngine(self, 0)]


class OffsetReqBuilder:
    def __init__(self, workload: StaticWorkload):
        self.workload = workload
        self.format_params: KvFormatParams = get_format_params(
            workload.key_size, workload.val_size
        )

    def make_req(self, offset: int | List[int]) -> Req:
        """return a tuple of (k, v) where v is None for read"""
        is_write = random.random() < self.workload.write_ratio
        if isinstance(offset, int):
            return Req(
                make_key(offset, self.format_params),
                make_val(offset, self.format_params) if is_write else None,
                offset,
            )
        else:
            # for scan, only read is currently supported
            # write is still an point operation
            if is_write:
                # pick the first key
                return Req(
                    make_key(offset[0], self.format_params),
                    make_val(offset[0], self.format_params),
                    offset[0],
                )
            else:
                # scan read
                return Req(
                    [make_key(o, self.format_params) for o in offset],
                    None,
                    offset,
                )

    def __str__(self):
        return str(self.workload)


class OffsetReqGenEngine(ReqGenEngine):
    """wrapper over OffsetReqBuilder with offset management"""

    def __init__(self, workload: StaticWorkload, until_elapsed: int):
        self.req_builder = OffsetReqBuilder(workload)
        self.offset_mgr: OffsetMgr = self.build_offset_mgr(workload)
        self.until_elapsed = until_elapsed

    @staticmethod
    def build_offset_mgr(workload: StaticWorkload) -> OffsetMgr:
        if isinstance(workload.distrib, SeqDistrib):
            return SeqOffsetMgr(workload.num_keys)
        if isinstance(workload.distrib, UnifDistrib):
            return UnifRndOffsetMgr(workload.num_keys)
        elif isinstance(workload.distrib, ZipfDistrib):
            return ZipfRndOffsetMgr(workload.num_keys, workload.distrib.theta)
        elif isinstance(workload.distrib, ScanDistrib):
            return ScanRangeOffsetMgr(
                workload.num_keys, workload.distrib.theta, workload.distrib.max_range
            )
        else:
            raise ValueError(f"Unrecognized distribution: {workload.distrib}")

    def make_req(self) -> Req:
        """return a tuple of (k, v) where v is None for read"""
        offset = self.offset_mgr.get_offset()
        return self.req_builder.make_req(offset)

    def is_done(self, elapsed: float) -> bool:
        return self.until_elapsed > 0 and elapsed >= self.until_elapsed

    def __str__(self):
        return str(self.req_builder)


@dataclass
class DynamicWorkload(Workload):
    @dataclass
    class WorkloadSchedule:
        until_time: int  # <= 0 for unlimited
        workload: StaticWorkload

    schedule: List[WorkloadSchedule]

    @classmethod
    def from_string(cls, s: str):
        """
        formatted as `n=xxx,k=xxx,v=xxx,w=xxx,d=xxx@t;...`
        `t` may be omitted, default to 0
        allow prefix `~` to denote copy the previous static workload (and then
        overwrite some parameters)
        """
        schedule = []
        for sub_sched in s.split(";"):
            sub_sched = sub_sched.strip()
            split_res = sub_sched.split("@", 1)
            wl_str, until_time_str = (
                (split_res[0], "0") if len(split_res) == 1 else split_res
            )
            until_time = (
                int(until_time_str[:-3]) * 60
                if until_time_str.endswith("min")
                else (
                    int(until_time_str[:-3])
                    if until_time_str.endswith("sec")
                    else int(until_time_str)
                )
            )
            if wl_str.startswith("~"):  # clone from previous workload
                wl = StaticWorkload.from_string(
                    f"{schedule[-1].workload},{wl_str[1:]}", allow_dup=True
                )
            else:
                wl = StaticWorkload.from_string(wl_str)
            schedule.append(cls.WorkloadSchedule(until_time, wl))
        return cls(schedule)

    def __str__(self):
        return ";".join(
            [f"{wl_sched.workload}@{wl_sched.until_time}" for wl_sched in self.schedule]
        )

    def build_req_gen(self) -> List[ReqGenEngine]:
        return [
            OffsetReqGenEngine(wl_sched.workload, wl_sched.until_time)
            for wl_sched in self.schedule
        ]

    @property
    def first(self) -> StaticWorkload:
        return self.schedule[0].workload

    @property
    def last(self) -> StaticWorkload:
        return self.schedule[-1].workload
