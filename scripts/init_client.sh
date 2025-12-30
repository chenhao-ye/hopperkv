#!/bin/bash
# This script provides a quick way to initialize a freshly provisioned EC2
# assumed config: ubuntu-22.04, x86
# current working directory is the repository top-level directory

set -euxo pipefail

git submodule update --init --recursive

sudo apt update
sudo apt install -y libssl-dev libcurl4-openssl-dev iperf clang-format cmake htop zsh clang unzip curl

curl -LsSf https://astral.sh/uv/install.sh | sh

# force uv to skip building Redis module and alloc engine
# because the client does not need them and may not be able to compile them
echo 'export CMAKE_ARGS="-DBUILD_REDIS_MODULE=OFF -DBUILD_ALLOC_ENGINE=OFF"' >> ~/.bashrc
echo 'export CMAKE_ARGS="-DBUILD_REDIS_MODULE=OFF -DBUILD_ALLOC_ENGINE=OFF"' >> ~/.zshrc

# shellcheck disable=SC1090,SC1091
source ~/.bashrc

export CMAKE_ARGS="-DBUILD_REDIS_MODULE=OFF -DBUILD_ALLOC_ENGINE=OFF"
uv sync
