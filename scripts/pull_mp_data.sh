#!/bin/bash
set -euxo pipefail

data_dir=$1
shift

for policy in "base" "drf" "hare" "memshare" "global"; do
  data_subdir="${data_dir}/${policy}"
  if [ -d "${data_subdir}" ]; then
    uv run -m scripts.merge_data "${data_subdir}" "$@"
  fi
done
