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

for cluster in "${cluster_list[@]}"; do
  trace_name="trace/${cluster}_12-13h"
  uv run replay/trim_cache_image.py -c 2147483648 "${trace_name}/cache_image.csv" &
done

wait
