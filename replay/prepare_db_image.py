import argparse
import logging


def main():
    parser = argparse.ArgumentParser(
        description="Process CSV files and generate k,v output"
    )
    parser.add_argument(
        "input_files", nargs="+", help="Input CSV files with key,val_size columns"
    )
    parser.add_argument("-o", "--output", required=True, help="Output CSV file")

    args = parser.parse_args()

    with open(args.output, "w") as output_file:
        output_file.write("k,v\n")

        for input_file in args.input_files:
            logging.info(f"Start to process {input_file}...")
            with open(input_file, "r") as f:
                # Check if first line is header
                first_line = f.readline()
                first_line = first_line.strip()
                assert first_line == "key,val_size", f"Unexpected header: {first_line}"

                # Process remaining lines
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    # Parse key,val_size
                    parts = line.split(",", 1)
                    if len(parts) != 2:
                        logging.warning(f"Incorrect format (will skip): {line}")
                        continue

                    key = parts[0]
                    val_size = int(parts[1])

                    # Generate value as "v" repeated val_size times
                    value = "v" * val_size

                    output_file.write(f"{key},{value}\n")

            logging.info(f"Finish processing {input_file}.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    main()
