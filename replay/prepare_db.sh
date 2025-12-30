#!/bin/bash
# set -euxo pipefail

# pack only necessary data to ship to AWS

cluster_list=(
  cluster2
  cluster19
  cluster33
  cluster34
  cluster40
  cluster54
)

persist_image_list=()

for cluster in "${cluster_list[@]}"; do
  persist_image_list+=("trace/${cluster}_12-13h/persist_image.csv")
done

echo "${persist_image_list[@]}"

uv run replay/prepare_db_image.py -o trace_db_image.csv "${persist_image_list[@]}"

echo "Dump output to trace_db_image.csv"
