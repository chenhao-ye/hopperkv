#!/usr/bin/env python3
import argparse
import logging
from collections import deque
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="Trim cache image to fit within specified cache size"
    )
    parser.add_argument("cache_image_path", type=Path, help="Input cache image file")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Output trimmed cache image file "
        "(default: trimmed_<input_filename> in same directory)",
    )
    parser.add_argument(
        "-c",
        "--cache_size",
        type=int,
        default=4 * 1024 * 1024 * 1024,
        help="Maximum cache size in bytes (default: 4GB)",
    )

    args = parser.parse_args()

    # Set default output path if not provided
    if args.output is None:
        input_path = Path(args.cache_image_path)
        args.output = input_path.parent / f"trimmed_{input_path.name}"

    # Use deque to maintain suffix of entries that fit in cache
    entries = deque()
    current_size = 0

    logging.info(
        f"Reading {args.cache_image_path} and trimming to "
        f"max size {args.cache_size:,} bytes..."
    )

    with open(args.cache_image_path, "r") as f:
        # Skip header line
        f.readline()

        for line in f:
            line = line.strip()

            # Parse key,val_size
            key, val_size = line.split(",")
            val_size = int(val_size)

            # Calculate total size for this entry
            kv_size = len(key) + val_size

            # Add new entry
            entries.append((key, val_size))
            current_size += kv_size

            # Remove entries from front until we fit in cache_size
            while current_size > args.cache_size and entries:
                removed_key, removed_val_size = entries.popleft()
                removed_kv_size = len(removed_key) + removed_val_size
                current_size -= removed_kv_size

    logging.info(f"Trimmed to {len(entries)} entries, total size: {current_size} bytes")

    # Write trimmed cache image
    with open(args.output, "w") as f:
        f.write("key,val_size\n")
        for key, val_size in entries:
            f.write(f"{key},{val_size}\n")

    logging.info(f"Trimmed cache image written to {args.output}")


if __name__ == "__main__":
    main()
