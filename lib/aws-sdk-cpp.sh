#!/bin/bash

sudo apt install libssl-dev libcurl4-openssl-dev

SRC_DIR=$(dirname $0)/aws-sdk-cpp
BUILD_DIR=${SRC_DIR}/build
INSTALL_DIR=${SRC_DIR}/install
# INSTALL_DIR=/usr/local

git clone \
  --depth 1 \
  --shallow-submodules \
  --recurse-submodules -j `nproc` \
  https://github.com/aws/aws-sdk-cpp.git \
  --branch 1.11.308 \
  ${SRC_DIR}

cmake \
  -S ${SRC_DIR} \
  -B ${BUILD_DIR} \
  -DCMAKE_INSTALL_PREFIX=${INSTALL_DIR} \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_ONLY="dynamodb" \
  -DENABLE_TESTING=OFF

cmake --build ${BUILD_DIR} -j `nproc` --target install
