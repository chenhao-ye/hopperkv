#!/bin/bash
set -euxo pipefail

# whether running on CloudLab or AWS
USE_CLOUDLAB=${USE_CLOUDLAB:-0}  # else, use AWS
# whether running clients on the local machine or remote machines
USE_LOCAL=${USE_LOCAL:-0}
# whether using mocked DynamoDB
USE_MOCK=${USE_MOCK:-0}

echo "=== Configuration ==="
echo "USE_CLOUDLAB=${USE_CLOUDLAB}"
echo "USE_LOCAL=${USE_LOCAL}"
echo "USE_MOCK=${USE_MOCK}"
echo "====================="

if [ "$USE_CLOUDLAB" != "0" ]; then
  # assume use 4 client nodes
  remote_clients=(
    "10.10.1.2"
    "10.10.1.3"
    "10.10.1.4"
    "10.10.1.5"
  )
  if [ "$USE_LOCAL" = "0" ]; then
    server_addr="10.10.1.1"
  else
    server_addr="localhost"
  fi
else  # use AWS
  # require exactly 4 EC2 instances named HopperKV-c* running
  readarray -t remote_clients < <(
    aws ec2 describe-instances \
      --query 'Reservations[*].Instances[*].[PrivateIpAddress]' \
      --filters "Name=instance-state-name,Values=running" \
                "Name=tag:Name,Values=HopperKV-c*" \
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

# if not USE_MOCK, must prepare these tables on DynamoDB beforehand
tables=(
  "k16v500.2000n6m-0"
  "k16v500.2000n6m-1"
  "k16v500.2000n6m-2"
  "k16v500.2000n6m-3"
)

sudo sysctl vm.overcommit_memory=1

bash scripts/build.sh

for ws in 6 8 12 16; do
  data_dir="results/exper_ycsb/ws=${ws}m"

  ckpt_paths=(
    "ckpt/n=${ws}m,k=16,v=500,d=zipf:0.99"
    "ckpt/n=${ws}m,k=16,v=500,d=zipf:0.99"
    "ckpt/n=${ws}m,k=16,v=500,d=zipf:0.99"
    "ckpt/n=${ws}m,k=16,v=500,d=scan:0.99:100"
  )

  workloads=(
    "n=${ws}m,k=16,v=500,w=0.5,d=zipf:0.99"
    "n=${ws}m,k=16,v=500,w=0.05,d=zipf:0.99"
    "n=${ws}m,k=16,v=500,w=0,d=zipf:0.99"
    "n=${ws}m,k=16,v=500,w=0.05,d=scan:0.99:100"
  )

  uv run -m driver.run_mp pipeline \
    "${workloads[@]}" \
    --tables "${tables[@]}" \
    --num_clients 16 \
    --async_queue_depth 2 \
    --mrc_salt 1% \
    -a "${server_addr}" \
    -d "${data_dir}" \
    -t 60 --preheat_duration 60 \
    --alloc_sched 50 \
    --alloc_stat_window 40 \
    -b 2Gi 1k 1k 50M \
    --load_ckpt_paths "${ckpt_paths[@]}" \
    "${remote_args[@]}" "${mock_args[@]}" \
    2>&1 | tee run_mp.log

  mv run_mp.log "${data_dir}/run_mp.log"

  bash scripts/pull_mp_data.sh "${data_dir}" "${remote_args[@]}"
  bash scripts/process_mp_data.sh "${data_dir}" 30
  uv run -m scripts.merge_mp_data "${data_dir}" 20 40
  uv run -m scripts.plot_mp "${data_dir}" --yscale K
  find "${data_dir}" -name 'dump.rdb' -delete  # cleanup RDB files
done
