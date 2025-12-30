# proxy for import
from pathlib import Path

from .hare_alloc_engine import (
    Allocator,
    MissRatioCurve,
    ResrcVec,
    StatelessResrcVec,
    config_logger,
    get_cache_delta,
    get_min_cache_size,
    get_min_db_rcu,
    get_min_db_wcu,
    get_min_net_bw,
    get_policy_alloc_total_net_bw,
    set_cache_delta,
    set_min_cache_size,
    set_min_db_rcu,
    set_min_db_wcu,
    set_min_net_bw,
    set_policy_alloc_total_net_bw,
)


def setup_logger(
    logger_name: str,
    log_filename: Path | str,
    log_level: str = "trace",
):
    # should only be called once for each logger name
    assert log_level in ["trace", "debug", "info", "warn", "error", "critical"]
    config_logger(logger_name, str(log_filename), log_level)


__all__ = [
    "Allocator",
    "MissRatioCurve",
    "ResrcVec",
    "StatelessResrcVec",
    "config_logger",
    "get_cache_delta",
    "get_min_cache_size",
    "get_min_db_rcu",
    "get_min_db_wcu",
    "get_min_net_bw",
    "get_policy_alloc_total_net_bw",
    "set_cache_delta",
    "set_min_cache_size",
    "set_min_db_rcu",
    "set_min_db_wcu",
    "set_min_net_bw",
    "set_policy_alloc_total_net_bw",
]
