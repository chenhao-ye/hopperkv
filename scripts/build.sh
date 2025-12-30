#!/bin/bash
set -euxo pipefail

build_type=${1:-Release}
cmake -B build -DCMAKE_BUILD_TYPE="${build_type}" -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build build --config "${build_type}" -j "$(nproc)"
