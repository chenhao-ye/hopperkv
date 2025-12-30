#!/bin/bash
# This script provides a quick way to initialize a freshly provisioned EC2
# assumed config: ubuntu-22.04, x86
# current working directory is the repository top-level directory

set -euxo pipefail

git submodule update --init --recursive

sudo apt update
sudo apt install -y libssl-dev libcurl4-openssl-dev iperf clang-format cmake htop zsh clang unzip curl zlib1g-dev python3-dev

# make a temporary directory for download & install
mkdir -p install
cd install

# install aws cli; assuming x86 (please refer to https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html for ARM)
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install --update

# install redis 7.2
sudo apt install -y pkg-config
git clone https://github.com/redis/redis.git
cd redis
git checkout 7.2
make -j "$(nproc)"
sudo make install
cd ../..
# it's okay to remove `install` directory but also okay to leave it there...

# build AWS SDK C++
bash lib/aws-sdk-cpp.sh

curl -LsSf https://astral.sh/uv/install.sh | sh
# shellcheck disable=SC1090,SC1091
source ~/.bashrc

uv sync
