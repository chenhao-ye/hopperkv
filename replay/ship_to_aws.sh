#!/bin/bash
set -euxo pipefail

# pack only necessary data to ship to AWS

cluster_list=(
  cluster2
  cluster19
  cluster33
  cluster34
  cluster40
  cluster54
)

ship_dir="trace_ship"

mkdir -p "${ship_dir}"

for cluster in "${cluster_list[@]}"; do
  mkdir -p "${ship_dir}/${cluster}_12-13h"
  for fname in "persist_image.csv" "trimmed_cache_image.csv" "req_trace.csv"; do
    cp "trace/${cluster}_12-13h/${fname}" "${ship_dir}/${cluster}_12-13h/${fname}"
  done
done

# Now trace_ship is a directory with only necessary trace for the server
du -h "${ship_dir}" # check the size

# Run command below to copy to the AWS server
# note it is free to send data to AWS, but expensive to send data out from AWS
#   scp -r trace_ship [server_addr]:[harekv_path]/trace
