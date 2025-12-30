from dataclasses import dataclass


@dataclass
class BaseDistrib:
    def __str__(self):
        raise NotImplementedError()


@dataclass
class SeqDistrib(BaseDistrib):
    def __str__(self):
        return "seq"


@dataclass
class UnifDistrib(BaseDistrib):
    def __str__(self):
        return "unif"


@dataclass
class ZipfDistrib(BaseDistrib):
    theta: float

    def __str__(self):
        return f"zipf:{self.theta}"


@dataclass
class ScanDistrib(BaseDistrib):
    theta: float
    max_range: int  # uniformly pick a range size from 1 to max_range

    def __str__(self):
        return f"scan:{self.theta}:{self.max_range}"
