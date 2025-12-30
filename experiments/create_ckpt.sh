#!/bin/bash
set -euxo pipefail

sudo sysctl vm.overcommit_memory=1
bash scripts/build.sh

function run_ckpt_zipf() {
  val_size=$1
  uv run -m driver.run \
    "n=${2}m,k=16,v=${val_size},w=0,d=zipf:0.99" \
    "n=${3}m,k=16,v=${val_size},w=0,d=zipf:0.99" \
    --tables tbl tbl -a localhost \
    -d "results/ckpt/n=${2}:${3},k=15,v=${val_size}" \
    -t 600 --preheat_duration 60 \
    -b 4Gi 4000 4000 100M \
    --mock_dynamo \
    --alloc_sched 1100 --skip_alloc \
    --dump_ckpt_paths \
    "ckpt/n=${2}m,k=16,v=${val_size},d=zipf:0.99" \
    "ckpt/n=${3}m,k=16,v=${val_size},d=zipf:0.99"
}

function run_ckpt_scan() {
  val_size=$1
  uv run -m driver.run \
    "n=${2}m,k=16,v=${val_size},w=0,d=scan:0.99:100" \
    "n=${3}m,k=16,v=${val_size},w=0,d=scan:0.99:100" \
    --tables tbl tbl -a localhost \
    -d "results/ckpt/n=${2}:${3},k=15,v=${val_size},d=scan:0.99:100" \
    -t 600 --preheat_duration 60 \
    -b 4Gi 4k 4k 100M \
    --mock_dynamo \
    --alloc_sched 1100 --skip_alloc \
    --dump_ckpt_paths \
    "ckpt/n=${2}m,k=16,v=${val_size},d=scan:0.99:100" \
    "ckpt/n=${3}m,k=16,v=${val_size},d=scan:0.99:100"
}

run_ckpt_zipf 500 1 2
run_ckpt_zipf 500 3 4
run_ckpt_zipf 500 5 6
run_ckpt_zipf 500 7 8
run_ckpt_zipf 500 10 12
run_ckpt_zipf 500 14 16

run_ckpt_scan 500 6 12
run_ckpt_scan 500 8 16

run_ckpt_zipf 2000 6 12
run_ckpt_scan 2000 6 12
