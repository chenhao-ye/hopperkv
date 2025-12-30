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
  "k16v500.2000n16m-0"
  "k16v500.2000n16m-1"
  "k16v500.2000n16m-2"
  "k16v500.2000n16m-3"
)

workloads=(
  'n=6m,k=16,v=500,w=0.5, d=zipf:0.99    @4min; ~w=0.45        @8min;  ~w=0.4         @12min; ~w=0.35        @16min; ~w=0.3         @20min; ~w=0.35        @24min; ~w=0.4          @28min; ~w=0.45        @32min; ~w=0.5         @36min; ~w=0.45        @40min; ~w=0.4         @44min; ~w=0.35        @48min; ~w=0.3'
  'n=6m,k=16,v=500,w=0.05,d=zipf:0.99    @5min; ~n=6.5m        @9min;  ~n=7m          @13min; ~n=7.5m        @17min; ~n=8m          @21min; ~n=8.5m        @25min; ~n=9m           @29min; ~n=9.5m        @33min; ~n=10m         @37min; ~n=10.5m       @41min; ~n=11m         @45min; ~n=11.5m       @49min; ~n=12m'
  'n=6m,k=16,v=500,w=0,   d=zipf:0.99    @6min; ~d=zipf:0.8    @10min; ~d=zipf:0.6    @14min; ~d=zipf:0.8    @18min; ~d=zipf:0.99   @22min; ~d=zipf:0.8    @26min; ~d=zipf:0.6     @30min; ~d=zipf:0.8    @34min; ~d=zipf:0.99   @38min; ~d=zipf:0.8    @42min; ~d=zipf:0.6    @46min; ~d=zipf:0.8    @50min; ~d=zipf:0.99'
  'n=6m,k=16,v=500,w=0.05,d=scan:0.99:100@7min; ~d=scan:0.99:80@11min; ~d=scan:0.99:60@15min; ~d=scan:0.99:40@19min; ~d=scan:0.99:60@23min; ~d=scan:0.99:80@27min; ~d=scan:0.99:100@31min; ~d=scan:0.99:80@35min; ~d=scan:0.99:60@39min; ~d=scan:0.99:40@43min; ~d=scan:0.99:60@47min; ~d=scan:0.99:80@51min; ~d=scan:0.99:100'
)

ckpt_paths=(
  'ckpt/n=6m,k=16,v=500,d=zipf:0.99'
  'ckpt/n=6m,k=16,v=500,d=zipf:0.99'
  'ckpt/n=6m,k=16,v=500,d=zipf:0.99'
  'ckpt/n=6m,k=16,v=500,d=scan:0.99:100'
)

global_ckpt_paths=()
for i in $(seq 0 3); do
  global_ckpt_paths+=("ckpt/global_scale/s${i}")
done

sudo sysctl vm.overcommit_memory=1

bash scripts/build.sh

data_dir="results/exper_dyn"

uv run -m driver.run_mp parallel \
  "${workloads[@]}" \
  --tables "${tables[@]}" \
  --num_clients 16 --async_queue_depth 4 \
  -a "${server_addr}" \
  -d "${data_dir}" \
  -t 3600 --preheat_duration 600 \
  --alloc_sched_rep 20:20 \
  --alloc_stat_window 20 \
  -b 2Gi 1k 1k 50M \
  --load_ckpt_paths "${ckpt_paths[@]}" \
  --alloc_apply_threshold 5% \
  --mrc_salt 1% \
  --report_freq 10 \
  --include_global \
  --global_load_ckpt_paths "${global_ckpt_paths[@]}" \
  --ghost_max_cache_scale 2 \
  --ghost_num_ticks 128 64 64 64 \
  --gradual \
  --smooth_window 3 \
  "${remote_args[@]}" "${mock_args[@]}" \
  2>&1 | tee run_mp.log

mv run_mp.log "${data_dir}/run_mp.log"

bash scripts/pull_mp_data.sh "${data_dir}" "${remote_args[@]}"
bash scripts/process_mp_data.sh "${data_dir}"
uv run -m scripts.plot_timeline "${data_dir}" \
  --include_global \
  --group 10 --xscale min \
  --xticks 0 10 20 30 40 50 \
  --dense 3:20000:5 \
  --markers \
  '0:w:45%@4;  w:40%@8;  w:35%@12;  w:30%@16;  w:35%@20;  w:40%@24; w:45%@28;  w:50%@32;  w:45%@36;   w:40%@40; w:35%@44;   w:30%@48' \
  '1:n:6.5M@5; n:7M@9;   n:7.5M@13; n:8M@17;   n:8.5M@21; n:9M@25;  n:9.5M@29; n:10M@33;  n:10.5M@37; n:11M@41; n:11.5M@45; n:12M@49' \
  '2:z:0.8@6;  z:0.6@10; z:0.8@14;  z:0.99@18; z:0.8@22;  z:0.6@26; z:0.8@30;  z:0.99@34; z:0.8@38;   z:0.6@42; z:0.8@46' \
  '3:s:80@7;   s:60@11;  s:40@15;   s:60@19;   s:80@23;   s:100@27; s:80@31;   s:60@35;   s:40@39;    s:60@43;  s:80@47'
find "${data_dir}" -name 'dump.rdb' -delete  # cleanup RDB files
