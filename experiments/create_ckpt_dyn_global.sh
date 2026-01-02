#!/bin/bash
# This script creates checkpoints for dynamic experiments with global pool
# This script takes ~20 minutes to finish

set -euxo pipefail

sudo sysctl vm.overcommit_memory=1
bash scripts/build.sh

uv run -m driver.run \
  'n=6m,k=16,v=500,w=0.5,d=zipf:0.99' \
  'n=6m,k=16,v=500,w=0.05,d=zipf:0.99' \
  'n=6m,k=16,v=500,w=0,d=zipf:0.99' \
  'n=6m,k=16,v=500,w=0.05,d=scan:0.99:100' \
  --tables tbl tbl tbl tbl -a localhost \
  -d "results/ckpt/global_dyn" \
  -t 1200 --preheat_duration 60 \
  -b 2Gi 1k 1k 50M \
  --mock_dynamo \
  --alloc_sched 1100 --skip_alloc \
  --dump_ckpt_paths \
  "ckpt/global_dyn/s0" \
  "ckpt/global_dyn/s1" \
  "ckpt/global_dyn/s2" \
  "ckpt/global_dyn/s3" \
  --global_pool
