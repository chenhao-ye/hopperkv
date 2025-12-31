#!/bin/bash
set -euxo pipefail

# whether running on CloudLab or AWS
USE_CLOUDLAB=${USE_CLOUDLAB:-0}  # else, use AWS
# whether running clients on the local machine or remote machines
USE_LOCAL=${USE_LOCAL:-0}
# whether using mocked DynamoDB
USE_MOCK=${USE_MOCK:-0}

if [ "$USE_CLOUDLAB" != "0" ]; then
  # assume use 6 client nodes
  remote_clients=(
    "10.10.1.2"
    "10.10.1.3"
    "10.10.1.4"
    "10.10.1.5"
    "10.10.1.6"
    "10.10.1.7"
  )
  if [ "$USE_LOCAL" = "0" ]; then
    server_addr="10.10.1.1"
  else
    server_addr="localhost"
  fi
else  # use AWS
  readarray -t remote_clients < <(
    aws ec2 describe-instances \
      --query 'Reservations[*].Instances[*].[PrivateIpAddress]' \
      --filters "Name=instance-state-name,Values=running" \
                "Name=tag:Name,Values=HopperKV-trace-c*" \
      --output=text
  )
  server_addr=$(hostname -I | awk '{print $NF}')
fi

remote_args=()
if [ "$USE_LOCAL" = "0" ]; then
  remote_args+=( \
    --remote_clients "${remote_clients[@]}"\
    --remote_path "$(realpath --relative-to="$HOME" "$(dirname "$0")/..")")
fi

mock_args=()
if [ "$USE_MOCK" != "0" ]; then
    mock_args+=(--mock)
fi

sudo sysctl vm.overcommit_memory=1

bash scripts/build.sh

tables=(
  "cluster2_12-13h"
  "cluster19_12-13h"
  "cluster33_12-13h"
  "cluster34_12-13h"
  "cluster40_12-13h"
  "cluster54_12-13h"
)

# if not USE_MOCK, must prepare these tables on DynamoDB beforehand
# for trace-replay workload, each policy-run will modify a table's data, so each
# table can only be used once for the fair comparison
# this requires in total: 6 clients x 4 policies = 24 table
# NOTE: these tables must be re-prepared before running this script next time
policy_tables=(
  "trace_table_0"
  "trace_table_1"
  "trace_table_2"
  "trace_table_3"
  "trace_table_4"
  "trace_table_5"
  "trace_table_6"
  "trace_table_7"
  "trace_table_8"
  "trace_table_9"
  "trace_table_10"
  "trace_table_11"
  "trace_table_12"
  "trace_table_13"
  "trace_table_14"
  "trace_table_15"
  "trace_table_16"
  "trace_table_17"
  "trace_table_18"
  "trace_table_19"
  "trace_table_20"
  "trace_table_21"
  "trace_table_22"
  "trace_table_23"
)

workloads=(
  'TRACE:loop:trace/cluster2_12-13h/req_trace.csv'
  'TRACE:loop:trace/cluster19_12-13h/req_trace.csv'
  'TRACE:loop:trace/cluster33_12-13h/req_trace.csv'
  'TRACE:loop:trace/cluster34_12-13h/req_trace.csv'
  'TRACE:loop:trace/cluster40_12-13h/req_trace.csv'
  'TRACE:loop:trace/cluster54_12-13h/req_trace.csv'
)

preheat_image_paths=(
  'trace/cluster2_12-13h/trimmed_cache_image.csv'
  'trace/cluster19_12-13h/trimmed_cache_image.csv'
  'trace/cluster33_12-13h/trimmed_cache_image.csv'
  'trace/cluster34_12-13h/trimmed_cache_image.csv'
  'trace/cluster40_12-13h/trimmed_cache_image.csv'
  'trace/cluster54_12-13h/trimmed_cache_image.csv'
)

mock_image_paths=(
  'trace/cluster2_12-13h/persist_image.csv'
  'trace/cluster19_12-13h/persist_image.csv'
  'trace/cluster33_12-13h/persist_image.csv'
  'trace/cluster34_12-13h/persist_image.csv'
  'trace/cluster40_12-13h/persist_image.csv'
  'trace/cluster54_12-13h/persist_image.csv'
)

data_dir="results/exper_trace_1g"

uv run -m driver.run_mp pipeline \
  "${workloads[@]}" \
  --tables "${tables[@]}" \
  -a "${server_addr}" \
  -d "${data_dir}" \
  -t 60 --preheat_duration 0 \
  --alloc_sched 50 \
  --alloc_stat_window 30 \
  -b 1Gi 1k 1k 50M \
  --load_cache_image_paths "${preheat_image_paths[@]}" \
  --load_mock_image_paths "${mock_image_paths[@]}" \
  --alloc_apply_threshold 5% \
  --mrc_salt 1% \
  --report_freq 10 \
  --num_clients 32 --async_queue_depth 4 \
  --num_preload 8 \
  --preload_batch_size 128 \
  --ghost_max_cache_scale 2 \
  --ghost_hint_kv_sizes 100 150 1200 350 200 250 \
  --ghost_num_ticks 128 128 128 128 128 128 \
  "${remote_args[@]}" "${mock_args[@]}" \
  --policy_tables "${policy_tables[@]}" \
  2>&1 | tee run_mp.log

mv run_mp.log "${data_dir}/run_mp.log"

bash scripts/pull_mp_data.sh "${data_dir}" "${remote_args[@]}"
bash scripts/process_mp_data.sh "${data_dir}"
uv run -m scripts.merge_mp_data "${data_dir}" 10 40
uv run -m scripts.plot_mp "${data_dir}" --yscale K \
  --tput_width_scale 0.25 --tput_height_scale 0.15 --ticks 0 1 2 --sep_legend
find "${data_dir}" -name 'dump.rdb' -delete  # cleanup RDB files
