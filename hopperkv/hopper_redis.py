import logging
import time
from typing import Dict, List

import redis
from redis import asyncio as redis_async


def _resp_to_dict(resp: List[str]):
    return dict(zip(resp[::2], resp[1::2]))


class HopperRedis:
    """Redis wrapper with customized HOPPER commands"""

    def __init__(self, enable_async: bool = False, verbose: bool = False, **kwargs):
        ## sync version:
        self.r = redis.Redis(**kwargs, decode_responses=True)
        self.r_async = (
            redis_async.Redis(**kwargs, decode_responses=True) if enable_async else None
        )
        self.verbose = verbose
        self.pipe = self.r.pipeline(transaction=False)

    def exec(self, *args):
        if self.verbose:
            logging.debug(f"Exec: {' '.join(args)}")
        return self.r.execute_command(*args)

    async def exec_async(self, *args):
        assert self.r_async is not None
        return await self.r_async.execute_command(*args)

    def exec_batch_add(self, *args):
        logging.debug(f"Exec batch: {self._digest(' '.join(args))}")
        self.pipe.execute_command(*args)

    def exec_batch_flush(self):
        logging.debug("Flush exec batch")
        return self.pipe.execute()

    def get(self, key: str):
        return self.exec("HOPPER.GET", key)

    async def get_async(self, key: str):
        return await self.exec_async("HOPPER.GET", key)

    def get_batch(self, key: str):
        self.exec_batch_add("HOPPER.GET", key)

    def set(self, key: str, val: str):
        return self.exec("HOPPER.SET", key, val)

    async def set_async(self, key: str, val: str):
        return await self.exec_async("HOPPER.SET", key, val)

    def set_batch(self, key: str, val: str):
        self.exec_batch_add("HOPPER.SET", key, val)

    def set_cache_only(self, key: str, val: str):
        return self.exec("HOPPER.SETC", key, val)

    async def set_cache_only_async(self, key: str, val: str):
        return await self.exec_async("HOPPER.SETC", key, val)

    def set_cache_only_batch(self, key: str, val: str):
        self.exec_batch_add("HOPPER.SETC", key, val)

    def load(self, image_path: str):
        return self.exec("HOPPER.LOAD", image_path)

    def stats(self) -> Dict:
        return _resp_to_dict(self.exec("HOPPER.STATS"))

    def get_resrc(self):
        (cache_size, db_rcu, db_wcu, net_bw) = self.exec("HOPPER.RESRC.GET")
        return int(cache_size), float(db_rcu), float(db_wcu), float(net_bw)

    def set_resrc(self, cache_size: int, db_rcu: float, db_wcu: float, net_bw: float):
        self.exec(
            "HOPPER.RESRC.SET", f"{cache_size}", f"{db_rcu}", f"{db_wcu}", f"{net_bw}"
        )

    def get_config(self) -> Dict:
        return _resp_to_dict(self.exec("HOPPER.CONFIG.GET"))

    def set_config(self, field: str, *args):
        self.exec("HOPPER.CONFIG.SET", field, *[str(arg) for arg in args])

    def set_table(self, table: str):
        logging.info(f"Switch to table <{table}>")
        self.set_config("dynamo.table", table)

    def set_ghost_range(self, tick: int, min_tick: int, max_tick: int):
        self.set_config("ghost.range", tick, min_tick, max_tick)

    def enable_admit_write(self):
        self.set_config("cache.admit_write", "true")

    def disable_admit_write(self):
        self.set_config("cache.admit_write", "false")

    # legacy wrapper; to be deprecated
    def set_cache_size(self, cache_size: float):
        self.r.config_set("MAXMEMORY", f"{cache_size:d}")

    def set_defrag(self, active: bool = True):
        self.r.config_set("activedefrag", "yes" if active else "no")

    def memory_stats(self, *args, **kwargs):
        return self.r.memory_stats(*args, **kwargs)

    def ping(self):
        return self.r.ping()

    def barrier_wait(self):
        self.exec("HOPPER.BARRIER.WAIT")

    def barrier_signal(self):
        self.exec("HOPPER.BARRIER.SIGNAL")

    def barrier_count(self):
        return self.exec("HOPPER.BARRIER.COUNT")

    def wait_ready(self, silent: bool = False):
        while True:
            try:
                self.r.ping()
                break
            except redis.exceptions.ConnectionError:
                time.sleep(1)
                if not silent:
                    logging.info("WAIT: Redis server is not ready...")
        if not silent:
            logging.info("READY: Redis server is ready!")

    def wait_memory_lower_than(self, threshold: int, silent: bool = False):
        while True:
            curr_memory = self.memory_stats()["total.allocated"]
            if curr_memory <= threshold:
                break
            time.sleep(1)
            if not silent:
                logging.info(
                    f"WAIT: Redis memory usage ({curr_memory / 1024 / 1024:.2f}MB) "
                    f"> threshold ({threshold / 1024 / 1024:.2f}MB)..."
                )
        if not silent:
            logging.info(
                f"READY: Redis memory usage ({curr_memory / 1024 / 1024:.2f}MB) "
                f"<= threshold ({threshold / 1024 / 1024:.2f}MB)"
            )

    def close(self):
        self.r.close()

    async def close_async(self):
        assert self.r_async is not None
        await self.r_async.close()

    # Redis native SET/GET
    def get_native(self, key: str):
        return self.r.get(key)

    def set_native(self, key: str, val: str):
        return self.r.set(key, val)

    def _digest(self, s: str) -> str:
        return f"{s if len(s) < 64 else f'{s[:64]}...'}"


def wait_redis_ready(**kwargs):
    r = HopperRedis(enable_async=False, **kwargs)
    r.wait_ready()
    r.close()
