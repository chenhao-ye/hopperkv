import argparse
import logging
from typing import List

from driver.launch import launch_servers
from driver.utils import run_cmd
from hopperkv.hopper_redis import wait_redis_ready


def main(ports: List[int], load_ckpt_paths: List[str] | None, cleanup: bool):
    if load_ckpt_paths is not None:
        assert len(ports) == len(load_ckpt_paths)
    s_list = launch_servers(
        ports=ports, load_ckpt_paths=load_ckpt_paths, cleanup=cleanup
    )
    for s, port in zip(s_list, ports):
        logging.info(f"Server at port {port} launched with pid {s.pid}")

    for port in ports:
        wait_redis_ready(port=port)

    for s in s_list:
        s.wait()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "ports",
        help="Ports for servers to listen",
        type=int,
        nargs="+",
    )
    parser.add_argument(
        "--load_ckpt_paths",
        help="A list of path to load checkpoint for every Redis server",
        type=str,
        nargs="+",
    )
    parser.add_argument(
        "--cleanup",
        help="Whether to cleanup existing redis before launch",
        action="store_true",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    try:
        main(**vars(args))
    except Exception as e:
        run_cmd(["sudo", "killall", "-9", "redis-server"], err_panic=False)
        raise e
