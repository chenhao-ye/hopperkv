#!/bin/bash
set -euxo pipefail

# whether running on CloudLab or AWS
USE_CLOUDLAB=${USE_CLOUDLAB:-1}  # else, use AWS
# whether running clients on the local machine or remote machines
USE_LOCAL=${USE_LOCAL:-0}
# whether using mocked DynamoDB
USE_MOCK=${USE_MOCK:-1}

REQUIRED_CLIENTS=2

if [ "$USE_CLOUDLAB" != "0" ]; then
  NUM_NODES=${NUM_NODES:-2}
  remote_clients=()  # infer remote_clients IPs based on NUM_NODES
  for i in $(seq 0 $((REQUIRED_CLIENTS - 1))); do
    node_idx=$((i % NUM_NODES))
    remote_clients+=("10.10.1.$((node_idx + 2))")
  done
  if [ "$USE_LOCAL" = "0" ]; then
    server_addr="10.10.1.1"
  else
    server_addr="localhost"
  fi
else  # use AWS
  # require exactly 2 EC2 instances named HopperKV-c* running
  readarray -t remote_clients < <(
    aws ec2 describe-instances \
      --query 'Reservations[*].Instances[*].[PrivateIpAddress]' \
      --filters "Name=instance-state-name,Values=running" \
                "Name=tag:Name,Values=HopperKV-c*" \
      --output=text
  )
  server_addr=$(hostname -I | awk '{print $NF}')
fi

# validate that we have the required number of clients
if [ "${#remote_clients[@]}" -ne "$REQUIRED_CLIENTS" ]; then
  echo "ERROR: Expected $REQUIRED_CLIENTS clients, but got ${#remote_clients[@]}"
  echo "remote_clients: ${remote_clients[*]}"
  exit 1
fi

remote_args=()
if [ "$USE_LOCAL" = "0" ]; then
  remote_args+=( \
    --remote_clients "${remote_clients[@]}" \
    --remote_path "$(realpath --relative-to="$HOME" "$(dirname "$0")/..")")
fi

mock_args=()
if [ "$USE_MOCK" != "0" ]; then
    mock_args+=(--mock)
fi

# if not USE_MOCK, must prepare these tables on DynamoDB beforehand
tables=( k16v500.2000n16m-0 k16v500.2000n16m-1 )

sudo sysctl vm.overcommit_memory=1

bash scripts/build.sh

# note: unit of working set (ws) is million keys
fixed_ws=6
fixed_theta=0.99
var_ws=12

exper_data_dir="results/exper_var_distrib_${fixed_ws}m_${fixed_theta}/ws=${var_ws}m"
rm -rf "${exper_data_dir}"

fixed_ckpt_path="ckpt/n=${fixed_ws}m,k=16,v=500,d=zipf:0.99"
var_ckpt_path="ckpt/n=${var_ws}m,k=16,v=500,d=zipf:0.99"

for var_theta in 0 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 0.99; do
  data_dir="${exper_data_dir}/var_theta=${var_theta}"

  uv run -m driver.run_mp pipeline \
    "n=${var_ws}m,k=16,v=500,w=0.05,d=zipf:${var_theta}" \
    "n=${fixed_ws}m,k=16,v=500,w=0.05,d=zipf:${fixed_theta}" \
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
    --load_ckpt_paths "${var_ckpt_path}" "${fixed_ckpt_path}" \
    "${remote_args[@]}" "${mock_args[@]}" \
    2>&1 | tee run_mp.log

  mv run_mp.log "${data_dir}/run_mp.log"

  bash scripts/pull_mp_data.sh "${data_dir}" "${remote_args[@]}"
  bash scripts/process_mp_data.sh "${data_dir}" 30
  uv run -m scripts.merge_mp_data "${data_dir}" 20 40
  find "${data_dir}" -name 'dump.rdb' -delete  # cleanup RDB files
  uv run -m scripts.plot_var "${exper_data_dir}" \
    --xlabel "\$T_1\$ Zipfian \$\theta\$" --xticks 0 0.2 0.4 0.6 0.8 0.99
done
