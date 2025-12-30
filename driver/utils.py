import logging
import os
import subprocess
from pathlib import Path
from typing import List


def check_rc(
    ret: int, *, ok_msg: str = None, err_msg: str = None, err_panic: bool = True
) -> None:
    # check whether the return code is zero
    if ret == 0:
        if ok_msg is not None:
            logging.info(ok_msg)
    else:
        if err_panic:
            raise RuntimeError(err_msg)
        if err_msg is not None:
            logging.warning(err_msg)


def run_cmd(
    cmd: List[str] | str,
    *,
    ok_msg: str = None,
    err_msg: str = None,
    err_panic: bool = True,
    silent: bool = False,
    shell: bool = False,
) -> None:
    # run a command and check return code
    if not silent:
        cmd_str = cmd if isinstance(cmd, str) else " ".join(cmd)
        logging.info(f"Run `{cmd_str}`")
    ret = subprocess.call(
        cmd,
        stdout=subprocess.DEVNULL if silent else None,
        stderr=subprocess.DEVNULL if silent else None,
        shell=shell,
    )
    if not silent:
        logging.info(f"Cmd `{cmd_str}` completed with code {ret}")
    check_rc(ret, err_msg=err_msg, ok_msg=ok_msg, err_panic=err_panic)


def prepare_data_dir(data_dir: str | Path, cleanup=False):
    if cleanup:
        # should only be set by the most top-level script for each experiment
        run_cmd(["rm", "-rf", f"{data_dir}"], silent=True)
    os.makedirs(data_dir, exist_ok=True)
    assert os.path.exists(data_dir)
