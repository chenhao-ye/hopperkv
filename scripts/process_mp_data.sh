#!/bin/bash
set -euxo pipefail

data_dir=$1

for policy in "base" "drf" "hare" "memshare" "global"; do
  data_subdir="${data_dir}/${policy}"
  if [ -d "${data_subdir}" ]; then
  uv run -m scripts.plot_tput_latency "${data_subdir}"
  uv run -m scripts.plot_mrc "${data_subdir}"
  for lat_ts in "${@:2}"; do
    uv run -m scripts.plot_latency_cdf "${data_subdir}" "${lat_ts}"
  done
  uv run -m scripts.analyze "${data_subdir}" | tee "${data_subdir}/report.md"
  fi
done

echo "# Combined Report of ${data_dir}" >"${data_dir}/report.md"
echo "" >>"${data_dir}/report.md"
cat "${data_dir}"/*/report.md >>"${data_dir}/report.md"
