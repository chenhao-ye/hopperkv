# HopperKV

![Build & Test](https://github.com/chenhao-ye/hopperkv/actions/workflows/cmake.yml/badge.svg)

HopperKV is a multi-tenant key-value store that extends Redis to cache data from DynamoDB. It features the *HARE* allocation algorithm, which holistically allocates cache capacity, DynamoDB read/write units, and network bandwidth to maximize throughput while guaranteeing fairness across tenants.

**Please refer to [`ARTIFACT.md`](./ARTIFACT.md) for instructions to reproduce experiments.**

HopperKV is developed and tested on Ubuntu-22 with Redis 7.2.

HopperKV uses `uv` to manage Python dependencies and build related C++ code. All python scripts should be run via `uv run` (most should be run as a module `uv run -m`). Dependency installation and building can be done simply via `bash scripts/init_server.sh`.


## Repository Structure

Below is a quick walkthrough of the codebase.

- `hopperkv/`: the core of HopperKV. It consists of a Redis Module (`hopperkv/redis_module`) and an allocator (`hopperkv/alloc`). `hopperkv` itself is a Python module that can be imported.

  - The redis module will be compiled into `libhopper_redis_module.so` and loaded into Redis server processes.
  - The allocator consists of C++ code and Python code. The C++ code will be compiled into `hare_alloc_engine.cpython-***.so` (`***` depends on the platform) and accessible as a Python module via pybind.

- `driver/`: the driver code to launch and orchestrate server and client processes. The major entry point is `run` and `run_mp`.

  - `run` will start an experiment: starting servers and clients, importing allocator, and performing allocation based on the configured policy.
  - `run_mp` wraps on top of `run` that run for multiple policies.

  `run` and `run_mp` take a long list of arguments and are typically invoked through the shell scripts in `experiments/`.

- `experiments/`: some experiments-specific shell scripts. Usually each script represents one end-to-end experiment with benchmarking, data processing, plotting, etc. They are also good references for designing a new experiment.

- `scripts/`: some general shell scripts (e.g., plotting, building, data processing). They are also typically invoked by the shell scripts in `experiments/`.

- `replay/`: some scripts specific for trace-replay experiments.

- `lib/`: third-party dependencies.
