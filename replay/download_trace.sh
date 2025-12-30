#!/bin/bash
set -euxo pipefail

# Download twitter traces and decompress
# These traces are HUGE! Make sure there is enough storage space
cluster_list=(
  cluster2
  cluster19
  cluster33
  cluster34
  cluster40
  cluster54
)

mkdir -p trace
cd trace

for cluster in "${cluster_list[@]}"; do
  curl "https://ftp.pdl.cmu.edu/pub/datasets/twemcacheWorkload/open_source/${cluster}.sort.zst" \
  | zstd -d > "${cluster}.sort" &
done

wait
