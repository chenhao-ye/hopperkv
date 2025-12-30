#!/bin/bash
set -euxo pipefail

for val_size in 500 2000; do
  name="k16v${val_size}n16m"
  uv run -m scripts.dynamo_client \
    --table ${name} \
    -n 16000000 -k 16 -v ${val_size} \
    --csv_path results/tbl/${name}.csv

  split -C 128m results/tbl/${name}.csv results/tbl/${name}_ --additional-suffix=.csv --numeric-suffixes=1 -a 4
  rm results/tbl/${name}.csv
done

aws s3 cp results/tbl s3://k16v500.2000n16m/ --recursive

echo "Visit https://us-east-2.console.aws.amazon.com/dynamodbv2/home#import-from-s3 to import the table"
