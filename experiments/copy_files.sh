#!/bin/bash
# This script copy files from the local machine to AWS EC2 machines

set -euxo pipefail

# configure the remote path
REMOTE_PATH=~/workspace/HopperKV

clients=$(
  aws ec2 describe-instances \
    --query 'Reservations[*].Instances[*].[PrivateIpAddress]' \
    --filters "Name=instance-state-name,Values=running" \
              "Name=tag:Name,Values=HopperKV-c*" \
    --output=text \
    | tr '\n' ' '
)

for c in $clients; do
  ssh -o StrictHostKeyChecking=no "ubuntu@$c" "rm -rf $REMOTE_PATH"
  scp -o StrictHostKeyChecking=no -r "$(pwd)" "ubuntu@$c:$REMOTE_PATH"
done
