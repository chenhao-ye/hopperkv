#!/usr/bin/env bash

set -e
set -x

SRC_DIR=$(dirname $0)
BUILD_DIR=${SRC_DIR}/build

cmake -S ${SRC_DIR} -B ${BUILD_DIR} -DDYNAMO_BUILD_TEST=ON
cmake --build ${BUILD_DIR} -j `nproc`

${BUILD_DIR}/test_dynamo
