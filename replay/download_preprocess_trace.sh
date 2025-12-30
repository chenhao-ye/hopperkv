#!/bin/bash
set -euxo pipefail

cluster_list=(
  cluster2
  cluster19
  cluster33
  cluster34
  cluster40
  cluster54
)

min_ts_hours=12
max_ts_hours=13

min_ts=$((min_ts_hours * 3600))
max_ts=$((max_ts_hours * 3600))

mkdir -p trace

for cluster in "${cluster_list[@]}"; do
  trace_name="${cluster}_${min_ts_hours}-${max_ts_hours}h"
  rm -rf "trace/${trace_name}"

  uv run replay/streaming_download.py \
    --cluster "${cluster}" \
    --max-timestamp "${max_ts}" \
  | tee "trace/${cluster}.sort" \
  | uv run replay/preprocess.py --dump-dir "trace/${trace_name}" \
    --min-timestamp "${min_ts}" --max-timestamp "${max_ts}" 2>&1 \
  | tee "trace/${trace_name}.log" &
done

wait
