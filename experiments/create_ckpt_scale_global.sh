#!/bin/bash
# This script creates checkpoints for scaling experiments with global pool

set -euxo pipefail

sudo sysctl vm.overcommit_memory=1
bash scripts/build.sh

workloads=(
  "n=6m,k=16,v=500,w=0.5,d=zipf:0.99" 
  "n=6m,k=16,v=2000,w=0.5,d=zipf:0.99" 
  "n=12m,k=16,v=500,w=0.5,d=zipf:0.99" 
  "n=12m,k=16,v=2000,w=0.5,d=zipf:0.99" 
  "n=6m,k=16,v=500,w=0.05,d=zipf:0.99" 
  "n=6m,k=16,v=2000,w=0.05,d=zipf:0.99" 
  "n=12m,k=16,v=500,w=0.05,d=zipf:0.99" 
  "n=12m,k=16,v=2000,w=0.05,d=zipf:0.99" 
  "n=6m,k=16,v=500,w=0,d=zipf:0.99" 
  "n=6m,k=16,v=2000,w=0,d=zipf:0.99" 
  "n=12m,k=16,v=500,w=0,d=zipf:0.99" 
  "n=12m,k=16,v=2000,w=0,d=zipf:0.99" 
  "n=6m,k=16,v=500,w=0.05,d=scan:0.99:100" 
  "n=6m,k=16,v=2000,w=0.05,d=scan:0.99:100" 
  "n=12m,k=16,v=500,w=0.05,d=scan:0.99:100" 
  "n=12m,k=16,v=2000,w=0.05,d=scan:0.99:100" 
)

tables=()
for i in $(seq 0 15); do
  tables+=("tbl")
done

ckpt_paths=()
for i in $(seq 0 15); do
  ckpt_paths+=("ckpt/global_scale/s${i}")
done

uv run -m driver.run \
  "${workloads[@]}" \
  --tables "${tables[@]}" \
  -a localhost \
  -d "results/ckpt/global_scale" \
  -t 1800 --preheat_duration 60 \
  -b 2Gi 1k 1k 50M \
  --num_preload 4 \
  --alloc_sched 1100 --skip_alloc \
  --dump_ckpt_paths "${ckpt_paths[@]}" \
  --global_pool
