#!/usr/bin/env python3
"""
Streaming download and decompress twitter traces with timestamp filtering.
Stops downloading when timestamp exceeds the specified threshold.
"""

import argparse
import logging
import subprocess
import sys


def stream_and_filter(
    cluster: str, max_timestamp: int, output_file: str = None
) -> None:
    """Stream download, decompress, and filter a single cluster's trace."""
    url = f"https://ftp.pdl.cmu.edu/pub/datasets/twemcacheWorkload/open_source/{cluster}.sort.zst"

    logging.info(f"Starting streaming download for {cluster}...")

    # Start curl and zstd processes
    curl_proc = subprocess.Popen(
        ["curl", "-s", url], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    zstd_proc = subprocess.Popen(
        ["zstd", "-d"],
        stdin=curl_proc.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Close curl stdout to allow it to receive SIGPIPE
    curl_proc.stdout.close()

    stopped_early = False

    # Choose output destination
    output_stream = open(output_file, "w") if output_file else sys.stdout

    try:
        for line_count, line in enumerate(zstd_proc.stdout, 1):
            # Extract timestamp (first column)
            timestamp = int(line.split(",", 1)[0])

            # Check if timestamp exceeds threshold
            if timestamp > max_timestamp:
                logging.info(
                    f"Stopped {cluster} at line {line_count:,}: timestamp {timestamp:,} > {max_timestamp:,}"
                )
                stopped_early = True
                break

            # Write the line to output
            output_stream.write(line)

            # Progress indicator every 100k lines
            if line_count % 1_000_000 == 0:
                logging.info(
                    f"Processed {line_count:,} lines for {cluster} (current timestamp: {timestamp:,})"
                )
    finally:
        # Close file if it was opened
        if output_file:
            output_stream.close()

    # Terminate the processes gracefully
    zstd_proc.terminate()
    curl_proc.terminate()

    # Wait for processes to finish
    zstd_proc.wait()
    curl_proc.wait()

    if not stopped_early:
        logging.info(f"Completed {cluster}: processed all {line_count:,} lines")

    return line_count, stopped_early


def main():
    parser = argparse.ArgumentParser(
        description="Stream download traces with timestamp filtering"
    )
    parser.add_argument(
        "-c",
        "--cluster",
        type=str,
        required=True,
        help="Cluster name (e.g., cluster34)",
    )
    parser.add_argument(
        "-t",
        "--max-timestamp",
        type=float,
        default=float("inf"),
        help="Stop when timestamp exceeds this value",
    )
    parser.add_argument(
        "-f", "--output-file", type=str, help="Output to file instead of stdout"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stderr,
    )

    logging.info(f"Max timestamp: {args.max_timestamp}")
    logging.info(f"Cluster: {args.cluster}")

    # Process the single cluster
    stream_and_filter(args.cluster, args.max_timestamp, args.output_file)

    logging.info("Download completed!")


if __name__ == "__main__":
    main()
