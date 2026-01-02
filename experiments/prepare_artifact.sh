#!/bin/bash
set -euo pipefail

LOG_FILE="prepare_artifact.log"
rm -f "$LOG_FILE"

remote_clients=(
  "10.10.1.2"
  "10.10.1.3"
  "10.10.1.4"
  "10.10.1.5"
  "10.10.1.6"
  "10.10.1.7"
)

echo "=== Starting artifact preparation ==="
SECONDS=0

run_script() {
    local script_path="$1"
    local script_name
    local step_start

    script_name=$(basename "$script_path")
    step_start=$SECONDS

    if ! bash "$script_path" >> "$LOG_FILE" 2>&1; then
        echo "ERROR: $script_name failed. Check $LOG_FILE for details."
        exit 1
    fi

    echo "$script_name completed in $((SECONDS - step_start)) seconds"
}

# install dependencies
echo "--- Installing dependencies ---"
run_script scripts/init_server.sh

# initialize remote clients
echo "--- Initializing remote clients ---"
relative_dir=$(realpath --relative-to="$HOME" "$(dirname "$0")/..")

for client in "${remote_clients[@]}"; do
    echo "Initializing client $client..."
    client_start=$SECONDS

    if ! ssh -o StrictHostKeyChecking=no "$client" "cd ~/$relative_dir && bash scripts/init_server.sh" >> "$LOG_FILE" 2>&1; then
        echo "ERROR: Failed to initialize client $client. Check $LOG_FILE for details."
        exit 1
    fi

    echo "Client $client initialized in $((SECONDS - client_start)) seconds"
done

# create checkpoints
echo "--- Creating checkpoints ---"
run_script experiments/create_ckpt.sh
run_script experiments/create_ckpt_scale_global.sh  # for scaling macrobenchmark
run_script experiments/create_ckpt_dyn_global.sh    # for dynamic macrobenchmark

# download and preprocess traces
echo "--- Downloading and preprocessing traces ---"
run_script replay/download_preprocess_trace.sh
run_script replay/trim_cache.sh

# copy traces to remote clients
echo "--- Copying traces to remote clients ---"
for client in "${remote_clients[@]}"; do
    echo "Copying traces to client $client..."
    client_start=$SECONDS

    if ! scp -o StrictHostKeyChecking=no -r trace/ "$client:~/$relative_dir/" >> "$LOG_FILE" 2>&1; then
        echo "ERROR: Failed to copy traces to client $client. Check $LOG_FILE for details."
        exit 1
    fi

    echo "Traces copied to client $client in $((SECONDS - client_start)) seconds"
done

echo "=== Total preparation time: $SECONDS seconds ==="
