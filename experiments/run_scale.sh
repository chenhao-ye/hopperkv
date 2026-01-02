#!/bin/bash
set -euxo pipefail

# whether running on CloudLab or AWS
USE_CLOUDLAB=${USE_CLOUDLAB:-1}  # else, use AWS
# whether running clients on the local machine or remote machines
USE_LOCAL=${USE_LOCAL:-0}
# whether using mocked DynamoDB
USE_MOCK=${USE_MOCK:-1}

REQUIRED_CLIENTS=16

if [ "$USE_CLOUDLAB" != "0" ]; then
  # ideally, each client should run on a dedicated machine, but that
  # requires 16 client nodes; for resource constraints, we consolidate clients
  # onto NUM_NODES nodes
  NUM_NODES=${NUM_NODES:-4}
  remote_clients=()  # 16 clients distributed across NUM_NODES nodes
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
  # require exactly 16 EC2 instances named HopperKV-c* running
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
    --remote_clients "${remote_clients[@]}"\
    --remote_path "$(realpath --relative-to="$HOME" "$(dirname "$0")/..")")
fi

mock_args=()
if [ "$USE_MOCK" != "0" ]; then
    mock_args+=(--mock)
fi

mock_args=()
if [ "$USE_MOCK" != "0" ]; then
    mock_args+=(--mock)
fi

# if not USE_MOCK, must prepare these tables on DynamoDB beforehand
tables=(
  "k16v500.2000n16m-0"
  "k16v500.2000n16m-1"
  "k16v500.2000n16m-2"
  "k16v500.2000n16m-3"
  "k16v500.2000n16m-4"
  "k16v500.2000n16m-5"
  "k16v500.2000n16m-6"
  "k16v500.2000n16m-7"
  "k16v500.2000n16m-8"
  "k16v500.2000n16m-9"
  "k16v500.2000n16m-10"
  "k16v500.2000n16m-11"
  "k16v500.2000n16m-12"
  "k16v500.2000n16m-13"
  "k16v500.2000n16m-14"
  "k16v500.2000n16m-15"
)

workloads=(
  "n=6m,k=16,v=500,w=0.5,d=zipf:0.99" 
  "n=6m,k=16,v=2000,w=0.5,d=zipf:0.99" 
  "n=12m,k=16,v=500,w=0.5,d=zipf:0.99" 
  "n=12m,k=16,v=2000,w=0.5,d=zipf:0.99" 
  "n=6m,k=16,v=500,w=0.05,d=zipf:0.99" 
  "n=6m,k=16,v=2000,w=0.05,d=zipf:0.99" 
  "n=12m,k=16,v=500,w=0.05,d=zipf:0.99" 
  "n=12m,k=16,v=2000,w=0.05,d=zipf:0.99" 
  "n=6m,k=16,v=500,w=0,d=zipf:0.99" 
  "n=6m,k=16,v=2000,w=0,d=zipf:0.99" 
  "n=12m,k=16,v=500,w=0,d=zipf:0.99" 
  "n=12m,k=16,v=2000,w=0,d=zipf:0.99" 
  "n=6m,k=16,v=500,w=0.05,d=scan:0.99:100" 
  "n=6m,k=16,v=2000,w=0.05,d=scan:0.99:100" 
  "n=12m,k=16,v=500,w=0.05,d=scan:0.99:100" 
  "n=12m,k=16,v=2000,w=0.05,d=scan:0.99:100" 
)

ckpt_paths=(
  "ckpt/n=6m,k=16,v=500,d=zipf:0.99" 
  "ckpt/n=6m,k=16,v=2000,d=zipf:0.99" 
  "ckpt/n=12m,k=16,v=500,d=zipf:0.99" 
  "ckpt/n=12m,k=16,v=2000,d=zipf:0.99" 
  "ckpt/n=6m,k=16,v=500,d=zipf:0.99" 
  "ckpt/n=6m,k=16,v=2000,d=zipf:0.99" 
  "ckpt/n=12m,k=16,v=500,d=zipf:0.99" 
  "ckpt/n=12m,k=16,v=2000,d=zipf:0.99" 
  "ckpt/n=6m,k=16,v=500,d=zipf:0.99" 
  "ckpt/n=6m,k=16,v=2000,d=zipf:0.99" 
  "ckpt/n=12m,k=16,v=500,d=zipf:0.99" 
  "ckpt/n=12m,k=16,v=2000,d=zipf:0.99" 
  "ckpt/n=6m,k=16,v=500,d=scan:0.99:100" 
  "ckpt/n=6m,k=16,v=2000,d=scan:0.99:100" 
  "ckpt/n=12m,k=16,v=500,d=scan:0.99:100" 
  "ckpt/n=12m,k=16,v=2000,d=scan:0.99:100" 
)

global_ckpt_paths=()
for i in $(seq 0 15); do
  global_ckpt_paths+=("ckpt/global_scale/s${i}")
done

sudo sysctl vm.overcommit_memory=1

bash scripts/build.sh

data_dir="results/exper_scale"

uv run -m driver.run_mp pipeline \
  "${workloads[@]}" \
  --tables "${tables[@]}" \
  --num_clients 16 \
  --async_queue_depth 2 \
  -a "${server_addr}" \
  -d "${data_dir}" \
  -t 120 --preheat_duration 20 \
  --alloc_sched 50 \
  --alloc_stat_window 40 \
  -b 2Gi 1k 1k 50M \
  --mrc_salt 1% \
  --load_ckpt_paths "${ckpt_paths[@]}" \
  "${remote_args[@]}" "${mock_args[@]}" \
  --include_global \
  --global_load_ckpt_paths "${global_ckpt_paths[@]}" \
  2>&1 | tee run_mp.log
mv run_mp.log "${data_dir}/run_mp.log"

bash scripts/pull_mp_data.sh "${data_dir}" "${remote_args[@]}"
bash scripts/process_mp_data.sh "${data_dir}"
uv run -m scripts.merge_mp_data "${data_dir}" 20 100 --include_global
uv run -m scripts.plot_mp "${data_dir}" --yscale K --include_global \
  --tput_width_scale 0.7 --cdf_width_scale 0.3 --ticks 0 1 2 3 4 \
  --highlight hare
find "${data_dir}" -name 'dump.rdb' -delete  # cleanup RDB files
