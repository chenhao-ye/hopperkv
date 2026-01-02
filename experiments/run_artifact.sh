#!/bin/bash
set -euo pipefail

LOG_FILE="run_artifact.log"
rm -f "$LOG_FILE"

echo "=== Starting artifact experiments ==="
SECONDS=0

run_script() {
    local script_path="$1"
    local script_name
    local step_start

    script_name=$(basename "$script_path")
    step_start=$SECONDS

    echo "Running $script_name..."
    if ! bash "$script_path" >> "$LOG_FILE" 2>&1; then
        echo "ERROR: $script_name failed. Check $LOG_FILE for details."
        exit 1
    fi

    echo "$script_name completed in $((SECONDS - step_start)) seconds"
}

# Microbenchmarks
echo "--- Running microbenchmarks ---"
run_script experiments/run_var_ws.sh       # varying working set size
run_script experiments/run_var_distrib.sh  # varying hotness distribution

# Scaling Macrobenchmark
echo "--- Running scaling macrobenchmark ---"
run_script experiments/run_scale.sh

# Dynamic Macrobenchmark
echo "--- Running dynamic macrobenchmark ---"
run_script experiments/run_dyn.sh

# Dynamic Trace-Replay Macrobenchmarks
echo "--- Running trace-replay macrobenchmarks ---"
run_script experiments/run_trace_512m.sh  # with 0.5GB baseline cache
run_script experiments/run_trace_1g.sh    # with 1GB baseline cache

echo "=== Total experiment time: $SECONDS seconds ==="
echo "=== All experiments completed successfully ==="
echo ""
echo "Generated figures:"
echo "  - results/exper_var_ws/fixed_ws=6m/perf_resrc.pdf (Figure 6a)"
echo "  - results/exper_var_distrib_6m_0.99/ws=12m/perf_resrc.pdf (Figure 6b)"
echo "  - results/exper_scale/norm_tput_cdf.pdf (Figure 7a)"
echo "  - results/exper_scale/norm_tput.pdf (Figure 7b)"
echo "  - results/exper_dyn/tput_timeline.pdf (Figure 8)"
echo "  - results/exper_trace_512m/norm_tput_cdf.pdf (Figure 9a)"
echo "  - results/exper_trace_512m/norm_tput.pdf (Figure 9a)"
echo "  - results/exper_trace_1g/norm_tput_cdf.pdf (Figure 9b)"
echo "  - results/exper_trace_1g/norm_tput.pdf (Figure 9b)"
