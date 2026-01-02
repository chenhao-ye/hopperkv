# HopperKV Artifact

This document describe the procedure to reproduce the experiments in the FAST'26 paper *Cache-Centric Multi-Resource Allocation for Storage Services*. The original experiments in the paper run on an AWS EC2 cluster with real DynamoDB. This document provides instructions to reproduce all experiments on CloudLab with a mocked DynamoDB backend, which enables an easy-to-use development environment without expensive AWS bills.

## Cluster Setup

Experiments require one machine for the server and several for the clients. The provided scripts by default assume a cloudlab cluster with 7 machines, where node0 (10.10.1.1) is the server, and the rest (10.10.1.2, ...) are the clients.

Setup requirements:

- All nodes must have `hopperkv` downloaded in the same path.
- The server node must be able to `ssh` into other clients machines without passwords.

All scripts assume running on the server's `hopperkv/`.

## Quick Reproduction

On the server machine, run `prepare_artifact.sh`, which initializes the current server machine and clients machines (via ssh). This should only run once; skip if already done.

```shell
bash experiments/prepare_artifact.sh  # this takes ~3.5 hours
```

Then run all experiments

```shell
bash experiments/run_artifact.sh      # this takes ~13 hours
```

The experiment data are saved in `results/`. The main results are the following figures:
- `results/exper_var_ws/fixed_ws=6m/perf_resrc.pdf` (Figure 6a)
- `results/exper_var_distrib_6m_0.99/ws=12m/perf_resrc.pdf` (Figure 6b)
- `results/exper_scale/norm_tput_cdf.pdf` (Figure 7a)
- `results/exper_scale/norm_tput.pdf` (Figure 7b)
- `results/exper_dyn/tput_timeline.pdf` (Figure 8)
- `results/exper_trace_512m/norm_tput_cdf.pdf` and `results/exper_trace_512m/norm_tput.pdf` (Figure 9a)
- `results/exper_trace_1g/norm_tput_cdf.pdf` and `results/exper_trace_1g/norm_tput.pdf` (Figure 9b)

*Note: the policy "Non-Part" may cause the system converging to a different state, so its curve may not be an exact match to that in the paper.*

## Detailed Reproduction Instructions

This section provides detailed, step-by-step instructions for experiments in [**Quick Reproduction**](#quick-reproduction); feel free to skip.

### Set up Environments

The operation below only needs to be done once, through it is safe to re-run any scripts.

#### Install Dependencies

To install all dependencies and build the hopperkv, run `scripts/init_server.sh` on all machines:

```shell
bash scripts/init_server.sh
```

*Side note: if the client machine's memory is too small to build the project, use `bash scripts/init_client.sh` on the client machine instead, which installs only a subset of dependencies and skip building hopperkv C++ components.*

#### Create Checkpoints

Since it takes very long time to warm up Redis, we will pre-create a few Redis checkpoints and load these checkpoints for experiments. The scripts below will create and save these checkpoints in `ckpt/`. The rest of experiments rely on these checkpoints.

```shell
bash experiments/create_ckpt.sh               # general checkpoints
# create checkpoints specifically for policy "Non-Part" (aka. "global")
bash experiments/create_ckpt_scale_global.sh  # for scaling macrobenchmark
bash experiments/create_ckpt_dyn_global.sh    # for dynamic macrobenchmark
```

#### Download and Preprocess Twitter Trace

The Twitter trace-replay experiments require downloading and preprocessing the twitter trace. Since the full trace files are very large, the scripts will only take a prefix of each.

```shell
bash replay/download_preprocess_trace.sh  # save into `trace/`
bash replay/trim_cache.sh
```

Then copies `trace/` to all clients machines.

### Microbenchmarks

The microbenchmarks contain two experiments: 1. varying working set size and 2. varying hotness distribution:

```shell
bash experiments/run_var_ws.sh       # varying working set size; may take ~3 hours
bash experiments/run_var_distrib.sh  # varying hotness distribution; may take ~2.5 hours
```

This should produce two figures `results/exper_var_ws/fixed_ws=6m/perf_resrc.pdf` (Figure 6a) and `results/exper_var_distrib_6m_0.99/ws=12m/perf_resrc.pdf` (Figure 6b).

### Scaling Macrobenchmark

To run scaling macrobenchmark:

```shell
bash experiments/run_scale.sh  # may take ~0.5 hours
```

This should produce two figures `results/exper_scale/norm_tput_cdf.pdf` (Figure 7a) and `results/exper_scale/norm_tput.pdf` (Figure 7b).

### Dynamic Macrobenchmark

To run dynamic macrobenchmark:

```shell
bash experiments/run_dyn.sh  # may take ~6 hours
```

This should produce a figure `results/exper_dyn/tput_timeline.pdf` (Figure 8).

### Dynamic Trace-Replay Macrobenchmark

The trace-replay macrobenchmarks run in two settings, one with 0.5GB baseline cache and another with 1GB:

```shell
bash experiments/run_trace_512m.sh  # with 0.5GB baseline cache; may take ~0.3 hours
bash experiments/run_trace_1g.sh    # with 1GB baseline cache; may take ~0.3 hours
```

This should produce four figures `results/exper_trace_512m/norm_tput_cdf.pdf` (Figure 9a, left), `results/exper_trace_512m/norm_tput.pdf` (Figure 9a, right), `results/exper_trace_1g/norm_tput_cdf.pdf` (Figure 9b, left), and `results/exper_trace_1g/norm_tput.pdf` (Figure 9b, right).
