import json
import logging
import shlex
import shutil
import subprocess
import time
from pathlib import Path
from typing import List

import psutil

from .client.workload import Workload
from .env import CLIENT_MOD_PATH, CLIENT_PRELOAD_MOD_PATH, REDIS_MODULE_PATH
from .utils import prepare_data_dir, run_cmd


def cleanup_redis():
    # clean up redis image before start server
    ## no images should ever be made anymore
    # run_cmd(["rm", "-rf", "images"], err_panic=False, silent=True)
    run_cmd(["sudo", "killall", "-9", "redis-server"], err_panic=False, silent=True)


def launch_servers(
    ports=List[int],
    server_path: Path | str | None = None,
    module_path: Path | str | None = None,
    data_dir: Path | str | None = None,
    pwords: List[str] | None = None,
    load_ckpt_paths: List[str | None] | None = None,
    cleanup: bool = True,
) -> List[subprocess.Popen]:
    if cleanup:
        cleanup_redis()
    else:
        # at least kill any processes that may occupy the ports
        for port in ports:
            run_cmd(
                ["sudo", "fuser", "-k", f"{port}/tcp"], err_panic=False, silent=True
            )

    if server_path is None:
        server_path = shutil.which("redis-server")  # assume `make install` for Redis
        assert server_path is not None, "Redis server not found in PATH"

    if module_path is None:
        module_path = REDIS_MODULE_PATH
        assert module_path.exists(), f"Module path {module_path} does not exist"

    if data_dir is None:
        data_dir = Path(".")

    s_list = []
    if pwords is None:
        pwords = [None] * len(ports)
    if load_ckpt_paths is None:
        load_ckpt_paths = [None] * len(ports)

    for sid, (port, pw, load_ckpt_path) in enumerate(
        zip(ports, pwords, load_ckpt_paths)
    ):
        # start a server with:
        # - LRU replacement policy
        # - disable RDB persistence
        # - disable AOF persistence
        args = [f"{server_path}"]
        args.extend(["--loadmodule", f"{module_path}"])
        args.extend(["--port", f"{port}"])
        args.extend(["--maxmemory-policy", "allkeys-lru"])
        args.extend(["--save", ""])
        args.extend(["--appendonly", "no"])
        if pw is not None:
            args.extend(["--requirepass", pw])
        s_data_dir = Path(data_dir) / f"s{sid}"
        prepare_data_dir(s_data_dir, cleanup=True)
        if load_ckpt_path is not None:
            for ckpt_type in ["rdb", "ghc"]:
                src_path = Path(load_ckpt_path) / f"dump.{ckpt_type}"
                dst_path = s_data_dir / f"dump.{ckpt_type}"
                logging.info(f"Copy {src_path} to {dst_path}")
                shutil.copy(src_path, dst_path)
        with open(s_data_dir / "server.log", "w") as f_log:
            s = subprocess.Popen(args, cwd=s_data_dir, stdout=f_log, stderr=f_log)
        logging.info(f"Launch server s{sid} at port={port} (pid={s.pid})")
        s_list.append(s)

    # wait to check if are servers okay
    # may quick crash due to fail to listen ports
    time.sleep(0.5)

    for s in s_list:
        rc = s.poll()
        if rc is not None:
            for s in s_list:  # killall before raise
                s.kill()
            raise RuntimeError(
                f"redis-server (pid={s.pid}) exits unexpectedly with code {rc}"
            )

    return s_list


def launch_clients(
    num_clients: int,
    workload: str,  # serialized workload
    data_dir: str,
    ports: List[int],
    sid: int,
    passwords: List[str] | None = None,
    verbose: bool = False,
    check: bool = False,
    async_queue_depth=None,
    **kwargs,
) -> List[subprocess.Popen]:
    args: list[str] = ["uv", "run", "-m", CLIENT_MOD_PATH, workload]
    args.extend(["--ports"] + [f"{p}" for p in ports])
    if passwords is not None:
        args.append("--passwords")
        args.extend(passwords)
    for k, v in kwargs.items():
        args.extend([f"--{k}", f"{v}"])
    if verbose:
        args.append("--verbose")
    if check:
        args.append("--check")
    if async_queue_depth is not None:
        args.extend(["--async_queue_depth", f"{async_queue_depth}"])

    c_list = []
    for cid in range(num_clients):
        c_data_dir = Path(data_dir) / f"s{sid}" / f"c{cid}"
        prepare_data_dir(c_data_dir)
        with open(f"{c_data_dir}/client.log", "w") as f_log:
            c_args = args.copy()
            if workload.startswith("TRACE:"):
                c_args.extend(["--trace_shard_idx", f"{cid}"])
                c_args.extend(["--trace_num_shards", f"{num_clients}"])
            c_args.extend(["--data_dir", f"{c_data_dir}", "--name", f"s{sid}/c{cid}"])
            c = subprocess.Popen(c_args, stdout=f_log, stderr=f_log)
        c_list.append(c)
    for cid, c in enumerate(c_list):
        if c.returncode is not None:
            logging.error(f"Client s{sid}/c{cid} exits with code {c.returncode}")
    return c_list


def launch_remote_clients(
    remote_client: str,
    remote_path: str | Path,
    **kwargs,  # kwargs for `launch_clients`
) -> subprocess.Popen:
    serialized_kwargs = json.dumps(kwargs)
    cmd: str = (
        f"cd {remote_path} && "
        f"uv run -m driver.remote_launch_clients '{serialized_kwargs}'"
    )
    args = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        remote_client,
        f"bash -l -c {shlex.quote(cmd)}",
    ]
    c = subprocess.Popen(args)
    return c


def launch_preload_fill(
    num_preload: int,
    workload: Workload,
    port: int,
    sid: int,
    data_dir: str,
    batch_size: int,
    password: str | None = None,
    verbose: bool = False,
) -> List[subprocess.Popen]:
    # host should just be the default `localhost`
    args = ["uv", "run", "-m", CLIENT_PRELOAD_MOD_PATH]
    args.extend(["fill", f"{workload}"])
    args.extend(["--port", f"{port}"])
    args.extend(["--batch_size", f"{batch_size}"])
    args.extend(["--stride", f"{num_preload}"])

    if password is not None:
        args.extend(["--password", f"{password}"])
    if verbose:
        args.append("--verbose")

    p_list = []
    p_data_dir = Path(data_dir) / f"s{sid}" / "preload"
    prepare_data_dir(p_data_dir)
    for preload_id in range(num_preload):
        with open(f"{p_data_dir}/preload_{preload_id}.log", "w") as f_log:
            p = subprocess.Popen(
                args + ["--stride_shift", f"{preload_id}"], stdout=f_log, stderr=f_log
            )
        p_list.append(p)
    return p_list


def launch_preload_warmup(
    num_preload: int,
    workload: Workload,
    port: int,
    sid: int,
    batch_size: int,
    duration: int,
    data_dir: str,
    password: str | None = None,
    verbose: bool = False,
) -> List[subprocess.Popen]:
    # host should just be the default `localhost`
    args = ["uv", "run", "-m", CLIENT_PRELOAD_MOD_PATH, "warmup", f"{workload}"]
    args.extend(["--port", f"{port}"])
    args.extend(["--batch_size", f"{batch_size}"])
    args.extend(["--duration", f"{duration}"])
    if password is not None:
        args.extend(["--password", f"{password}"])
    if verbose:
        args.append("--verbose")

    p_list = []
    p_data_dir = Path(data_dir) / f"s{sid}" / "preload"
    prepare_data_dir(p_data_dir)
    for preload_id in range(num_preload):
        with open(f"{p_data_dir}/preload_{preload_id}.log", "w") as f_log:
            p = subprocess.Popen(
                args,
                stdout=f_log,
                stderr=f_log,
            )
        p_list.append(p)
    return p_list


def launch_preload_load(
    num_preload: int,
    workload: Workload,
    port: int,
    sid: int,
    batch_size: int,
    data_dir: str,
    password: str | None = None,
    verbose: bool = False,
) -> List[subprocess.Popen]:
    # host should just be the default `localhost`
    args = ["uv", "run", "-m", CLIENT_PRELOAD_MOD_PATH, "load", f"{workload}"]
    args.extend(["--port", f"{port}"])
    args.extend(["--batch_size", f"{batch_size}"])
    args.extend(["--stride", f"{num_preload}"])

    if password is not None:
        args.extend(["--password", f"{password}"])
    if verbose:
        args.append("--verbose")

    p_list = []
    p_data_dir = Path(data_dir) / f"s{sid}" / "preload"
    prepare_data_dir(p_data_dir)
    for preload_id in range(num_preload):
        with open(f"{p_data_dir}/preload_{preload_id}.log", "w") as f_log:
            p = subprocess.Popen(
                args + ["--stride_shift", f"{preload_id}"], stdout=f_log, stderr=f_log
            )
        p_list.append(p)
    return p_list


def isolate_proc_cpus(proc_list: List[subprocess.Popen]):
    # try to pin each process to a dedicated subset of CPUs
    # raise exception if fail (e.g., #cpu < #proc)
    per_proc_cpu = int(psutil.cpu_count() / len(proc_list))
    if per_proc_cpu <= 0:
        raise ValueError("No enough CPUs")
    for sid, proc in enumerate(proc_list):
        cpu_list = list(range(sid * per_proc_cpu, (sid + 1) * per_proc_cpu))
        psutil.Process(proc.pid).cpu_affinity(cpu_list)
        logging.info(f"Pin s{sid} to CPUs: {cpu_list}")
