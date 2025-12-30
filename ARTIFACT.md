# HopperKV Artifact

This document describe the procedure to reproduce the experiments. All scripts assume the current working directory the project top-level directory `HopperKV/`.

## Get Started

HopperKV uses `uv` to manage decencies and build related C++ code. All python scripts should be run via `uv run` (most should be run as a module `uv run -m`). HopperKV is developed and tested on Ubuntu-22 with Redis 7.2. Note our heuristic replies on Redis-reported statics, whose definition may vary in different versions.

### Set up Environments

The operation below only needs to be done once (through it is safe to re-run any scripts).

#### Install Dependencies

To install all dependencies, run `scripts/init_server.sh`:

```shell
bash scripts/init_server.sh
```

For machines used as a benchmark client, it may not has the capacity to build the entire codebase, so we offer another `scripts/init_client.sh` which only installs decencies to run client code.

```shell
bash scripts/init_client.sh
```

#### Create Checkpoints

Since it takes very long time to warm up Redis, we will pre-create a few Redis checkpoints and load these checkpoints for experiments. The scripts below will create and save these checkpoints in `ckpt/`. The rest of experiments reply on these checkpoints.

```shell
# create general checkpoints
bash experiments/create_ckpt.sh
# create checkpoints specifically for policy "Non-Part" (aka. "global")
bash experiments/create_ckpt_scale_global.sh  # for scaling macrobenchmark
bash experiments/create_ckpt_dyn_global.sh    # for dynamic macrobenchmark
```

#### Download and Preprocess Twitter Trace

The Twitter trace-replay experiments require downloading the twitter trace and preprocess the traces. Since the full traces are very large, the scripts will only take a prefix of each.

```shell
bash replay/download_preprocess_trace.sh
bash replay/trim_cache.sh
```

### Microbenchmarks

The microbenchmarks contain two experiments: 1. varying working set size and 2. varying hotness distribution:

```shell
bash experiments/run_var_ws.sh       # varying working set size
bash experiments/run_var_distrib.sh  # varying hotness distribution
```

This should produce two figures `results/exper_var_ws/fixed_ws=6m/perf_resrc.pdf` (Figure 6a in the paper) and `results/exper_var_distrib_6m_0.99/ws=12m/perf_resrc.pdf` (Figure 6b).

### Scaling Macrobenchmark

To run scaling macrobenchmark:

```shell
bash experiments/run_scale.sh
```

This should produce two figures `results/exper_scale/norm_tput_cdf.pdf` (Figure 7a) and `results/exper_scale/norm_tput.pdf` (Figure 7b).

### Dynamic Macrobenchmark

To run dynamic macrobenchmark:

```shell
bash experiments/run_dyn.sh
```

This should produce a figure `results/exper_dyn/tput_timeline.pdf` (Figure 8).

### Dynamic Trace-Replay Macrobenchmark

The trace-replay macrobenchmarks run in two settings, one with 0.5GB baseline cache and another with 1GB:

```shell
bash experiments/run_trace_512m.sh  # with 0.5GB baseline cache
bash experiments/run_trace_1g.sh    # with 1GB baseline cache
```

This should produce four figures `results/exper_trace_512m/norm_tput_cdf.pdf` and `results/exper_trace_512m/norm_tput.pdf` (Figure 9a) and `results/exper_trace_1g/norm_tput_cdf.pdf` and `results/exper_trace_1g/norm_tput.pdf` (Figure 9b).


## Repository Structure

Below is a quick walkthrough of the codebase.

- `hopperkv/`: the core of HopperKV. It consists of a Redis Module (`hopperkv/redis_module`) and an allocator (`hopperkv/alloc`). `hopperkv` itself is a Python module that can be imported.

  - The redis module will be compiled into `libhopper_redis_module.so` and loaded into Redis server processes. These code will not be accessible through Python APIs.
  - The allocator consists of C++ code and Python code. The C++ code will be compiled into `hare_alloc_engine.cpython-***.so` (`***` depends on the platform) and accessible as a python module via pybind.

- `driver/`: the driver code to launch and orchestrate server and client processes. The major entry point is `run` and `run_mp`.

  - `run` will start an experiment: starting servers and clients, importing allocator, and performing allocation based on the configured policy.
  - `run_mp` wraps on top of `run` that run for multiple policies.

  `run` and `run_mp` take a long list of arguments and are typically invoked through the shell scripts in `experiments/`.

- `experiments/`: some experiments-specific shell scripts. Usually each script represents one end-to-end experiment with benchmarking, data processing, plotting, etc. They are also good references for designing a new experiment.

- `scripts/`: some general shell scripts (e.g., plotting, building, data processing). They are also typically invoked by the shell scripts in `experiments/`.

- `replay/`: some scripts specific for trace-replay experiments.

- `lib/`: third-party dependencies.
