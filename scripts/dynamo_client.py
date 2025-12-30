import argparse
import logging
import time
from pathlib import Path
from typing import Optional

import boto3

from driver.client.workload.kv_format import (
    KvFormatParams,
    get_format_params,
    make_key,
    make_val,
)


def print_item_size(key_size: int, val_size: int, key_attr: str, val_attr: str):
    format_params: KvFormatParams = get_format_params(key_size, val_size)
    serializer = boto3.dynamodb.types.TypeSerializer()
    k = make_key(0, format_params)
    v = make_val(0, format_params)

    raw_size = len(k) + len(v)
    assert raw_size == key_size + val_size
    field_size = raw_size + len(key_attr) + len(val_attr)
    json_size = len(str(serializer.serialize({key_attr: k, val_attr: v})))
    print(
        f"Estimated: raw_size={raw_size}, field_size={field_size}, json_size={json_size}"
    )


def get_table(table_name: str, key_attr: str, is_create: bool):
    dynamodb = boto3.resource("dynamodb")

    table = (
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": key_attr, "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": key_attr, "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        if is_create
        else dynamodb.Table(table_name)
    )
    if is_create:
        waiter = boto3.client("dynamodb").get_waiter("table_exists")
        waiter.wait(TableName=table_name)

    return table


def main(
    table_name: str,
    is_create: bool,
    num_keys: int,
    key_size: int,
    val_size: int,
    key_attr: str,
    val_attr: str,
    start_from_offset: int,
    csv_path: Optional[Path],
):
    if csv_path is not None:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_file = csv_path.open("w")
    else:
        table = get_table(table_name, key_attr, is_create)

    format_params: KvFormatParams = get_format_params(key_size, val_size)
    # print_item_size(key_size, val_size, key_attr, val_attr)
    t0 = time.perf_counter()
    for offset in range(start_from_offset, num_keys):
        k = make_key(offset, format_params)
        v = make_val(offset, format_params)
        if csv_path is not None:
            csv_file.write(f"{k},{v}\n")
        else:
            table.put_item(Item={key_attr: k, val_attr: v})
    t1 = time.perf_counter()
    logging.info(f"Completed in {t1 - t0:.2f} seconds")
    if csv_path is not None:
        csv_file.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", help="Name of the table", type=str, required=True)
    parser.add_argument(
        "--create",
        help="Whether create this table",
        action="store_true",
    )
    parser.add_argument(
        "-n",
        "--num_keys",
        help="Number of keys in the working set",
        type=int,
        required=True,
    )
    parser.add_argument(
        "-k", "--key_size", help="Key size in bytes", type=int, required=True
    )
    parser.add_argument(
        "-v", "--val_size", help="Value size in bytes", type=int, required=True
    )
    parser.add_argument(
        "--key_attr",
        help="Key attribute (i.e., column name)",
        type=str,
        default="k",
    )
    parser.add_argument(
        "--val_attr",
        help="Value attribute (i.e., column name)",
        type=str,
        default="v",
    )
    parser.add_argument(
        "--start_from_offset",
        help="Starting from a given offset",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--csv_path",
        help="Path to save the CSV file",
        type=Path,
        default=None,
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    main(
        table_name=args.table,
        is_create=args.create,
        num_keys=args.num_keys,
        key_size=args.key_size,
        val_size=args.val_size,
        key_attr=args.key_attr,
        val_attr=args.val_attr,
        start_from_offset=args.start_from_offset,
        csv_path=args.csv_path,
    )
