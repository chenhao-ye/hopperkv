from dataclasses import asdict, dataclass
from typing import List, Tuple

from ..utils import str_cast_type
from .engine import ResrcVec


@dataclass
class ResrcTuple:
    cache_size: int
    db_rcu: float
    db_wcu: float
    net_bw: float

    def __sub__(self, other: "ResrcTuple"):
        return ResrcTuple(
            cache_size=self.cache_size - other.cache_size,
            db_rcu=self.db_rcu - other.db_rcu,
            db_wcu=self.db_wcu - other.db_wcu,
            net_bw=self.net_bw - other.net_bw,
        )

    def __str__(self) -> str:
        return (
            f"{{ cache_size={self.to_str_human_readable(self.cache_size)}B, "
            f"db_rcu={self.db_rcu:.2f}, db_wcu={self.db_wcu:.2f}, "
            f"net_bw={self.to_str_human_readable(self.net_bw)}B/s }}"
        )

    def __mul__(self, factor: float) -> "ResrcTuple":
        return ResrcTuple(
            cache_size=int(self.cache_size * factor),
            db_rcu=self.db_rcu * factor,
            db_wcu=self.db_wcu * factor,
            net_bw=self.net_bw * factor,
        )

    def __add__(self, other: "ResrcTuple") -> "ResrcTuple":
        return ResrcTuple(
            cache_size=self.cache_size + other.cache_size,
            db_rcu=self.db_rcu + other.db_rcu,
            db_wcu=self.db_wcu + other.db_wcu,
            net_bw=self.net_bw + other.net_bw,
        )

    def to_tuple(self) -> Tuple[int, float, float, float]:
        return self.cache_size, self.db_rcu, self.db_wcu, self.net_bw

    @classmethod
    def from_vec(cls, resrc: ResrcVec) -> "ResrcTuple":
        return cls(*resrc.to_tuple())

    def to_vec(self) -> ResrcVec:
        return ResrcVec(self.to_tuple())

    @classmethod
    def from_str_tuple(cls, resrc_str_tuple: Tuple[str, str, str, str]) -> "ResrcTuple":
        cache_size_str, db_rcu_str, db_wcu_str, net_bw_str = resrc_str_tuple
        return cls(
            str_cast_type(cache_size_str, int, binary_scale=True),
            str_cast_type(db_rcu_str, float, binary_scale=False),
            str_cast_type(db_wcu_str, float, binary_scale=False),
            str_cast_type(net_bw_str, float, binary_scale=True),
        )

    @classmethod
    def from_str(cls, resrc_str: str) -> "ResrcTuple":
        return cls.from_str_tuple(resrc_str.split(","))

    @staticmethod
    def to_str_human_readable(v: float | int) -> str:
        if v >= 1024 * 1024:
            return f"{v / 1024 / 1024:.2f}M"
        if v >= 1024:
            return f"{v / 1024:.2f}K"
        return f"{v}"


@dataclass
class ResrcStat:
    timestamp: float | None = None
    duration: float | None = None
    ghost_hit_cnt: List[int] = None
    ghost_miss_cnt: List[int] = None
    req_cnt: int = 0
    hit_cnt: int = 0
    miss_cnt: int = 0
    db_rcu_consump_if_miss: int = 0
    net_bw_consump_if_miss: int = 0
    net_bw_consump_if_hit: int = 0
    db_rcu_consump: float = 0
    db_wcu_consump: float = 0
    net_bw_consump: float = 0

    @staticmethod
    def zip_pad(lhs, rhs):
        if len(lhs) != len(rhs):
            max_len = max(len(lhs), len(rhs))
            lhs = lhs if len(lhs) == max_len else lhs + [lhs[-1]] * (max_len - len(lhs))
            rhs = rhs if len(rhs) == max_len else rhs + [rhs[-1]] * (max_len - len(rhs))
        return zip(lhs, rhs)

    def __add__(self, other: "ResrcStat") -> "ResrcStat":
        return ResrcStat(
            timestamp=None,
            duration=(
                self.duration + other.duration
                if self.duration is not None and other.duration is not None
                else None
            ),
            ghost_hit_cnt=(
                [
                    lhs + rhs
                    for lhs, rhs in ResrcStat.zip_pad(
                        self.ghost_hit_cnt, other.ghost_hit_cnt
                    )
                ]
                if self.ghost_hit_cnt is not None and other.ghost_hit_cnt is not None
                else None
            ),
            ghost_miss_cnt=(
                [
                    lhs + rhs
                    for lhs, rhs in ResrcStat.zip_pad(
                        self.ghost_miss_cnt, other.ghost_miss_cnt
                    )
                ]
                if self.ghost_miss_cnt is not None and other.ghost_miss_cnt is not None
                else None
            ),
            req_cnt=self.req_cnt + other.req_cnt,
            hit_cnt=self.hit_cnt + other.hit_cnt,
            miss_cnt=self.miss_cnt + other.miss_cnt,
            db_rcu_consump_if_miss=self.db_rcu_consump_if_miss
            + other.db_rcu_consump_if_miss,
            net_bw_consump_if_miss=self.net_bw_consump_if_miss
            + other.net_bw_consump_if_miss,
            net_bw_consump_if_hit=self.net_bw_consump_if_hit
            + other.net_bw_consump_if_hit,
            db_rcu_consump=self.db_rcu_consump + other.db_rcu_consump,
            db_wcu_consump=self.db_wcu_consump + other.db_wcu_consump,
            net_bw_consump=self.net_bw_consump + other.net_bw_consump,
        )

    def __sub__(self, other: "ResrcStat") -> "ResrcStat":
        return ResrcStat(
            timestamp=None,
            duration=(
                self.timestamp - other.timestamp
                if self.timestamp is not None and other.timestamp is not None
                else None
            ),
            ghost_hit_cnt=(
                [
                    lhs - rhs
                    for lhs, rhs in ResrcStat.zip_pad(
                        self.ghost_hit_cnt, other.ghost_hit_cnt
                    )
                ]
                if self.ghost_hit_cnt is not None and other.ghost_hit_cnt is not None
                else None
            ),
            ghost_miss_cnt=(
                [
                    lhs - rhs
                    for lhs, rhs in ResrcStat.zip_pad(
                        self.ghost_miss_cnt, other.ghost_miss_cnt
                    )
                ]
                if self.ghost_miss_cnt is not None and other.ghost_miss_cnt is not None
                else None
            ),
            req_cnt=self.req_cnt - other.req_cnt,
            hit_cnt=self.hit_cnt - other.hit_cnt,
            miss_cnt=self.miss_cnt - other.miss_cnt,
            db_rcu_consump_if_miss=self.db_rcu_consump_if_miss
            - other.db_rcu_consump_if_miss,
            net_bw_consump_if_miss=self.net_bw_consump_if_miss
            - other.net_bw_consump_if_miss,
            net_bw_consump_if_hit=self.net_bw_consump_if_hit
            - other.net_bw_consump_if_hit,
            db_rcu_consump=self.db_rcu_consump - other.db_rcu_consump,
            db_wcu_consump=self.db_wcu_consump - other.db_wcu_consump,
            net_bw_consump=self.net_bw_consump - other.net_bw_consump,
        )

    def is_valid(self) -> bool:
        return (
            self.ghost_hit_cnt is not None
            and self.ghost_miss_cnt is not None
            and self.ghost_hit_cnt[0] + self.ghost_miss_cnt[0] > 0
        )

    def dump(self):
        return asdict(self)
