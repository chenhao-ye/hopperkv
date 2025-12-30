from dataclasses import dataclass
from typing import List, Tuple


@dataclass
class Req:
    key: List[str] | str
    val: List[str] | str | None
    offset: List[int] | int

    @property
    def is_single(self) -> bool:
        return isinstance(self.key, str)

    @property
    def is_write(self) -> bool:
        return self.val is not None

    def to_tuples(self) -> List[Tuple[str, str | None, int]]:
        if self.is_single:
            return [(self.key, self.val, self.offset)]
        else:
            if self.val is None:
                return [(k, None, o) for k, o in zip(self.key, self.offset)]
            else:
                return list(zip(self.key, self.val, self.val))


class ReqGenEngine:
    def make_req(self) -> Req | None:
        raise NotImplementedError()

    def is_done(self, elapsed: float) -> bool:
        raise NotImplementedError()


class Workload:
    def build_req_gen(self) -> List[ReqGenEngine]:
        raise NotImplementedError
