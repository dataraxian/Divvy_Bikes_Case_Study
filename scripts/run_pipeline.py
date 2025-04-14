### run_pipeline.py
import os
import logging
import argparse
from s3_divvy import core, metadata, processing
from s3_divvy.config import EXTRACT_DIR, QUALITY_CHECK_MODE  # Added QUALITY_CHECK_MODE

logging.basicConfig(level=logging.INFO)

def find_first_csv(directory):
    # Detect actual .csv filename in each extract folder
    for fname in os.listdir(directory):
        if fname.lower().endswith(".csv"):
            return os.path.join(directory, fname)
    return None

def run(mode="duckdb", quality_check=None):
    # Allow override via CLI; fallback to config toggle
    qc_mode = quality_check if quality_check is not None else QUALITY_CHECK_MODE

    current_df = core.list_s3_files()
    if current_df.empty:
        logging.info("No files to process.")
        return

    previous_df = metadata.load_metadata()
    files_to_process = metadata.compare_metadata(current_df, previous_df)

    for _, row in files_to_process.iterrows():
        file_name = row["file_name"]
        zip_path = core.download_file(file_name)
        if not zip_path:
            continue

        extract_path = os.path.join(EXTRACT_DIR, file_name.replace(".zip", ""))
        os.makedirs(extract_path, exist_ok=True)
        core.extract_zip(zip_path, extract_path)
        core.save_file_hash(zip_path)

    metadata.save_metadata(current_df)

    if mode == "bulk":
        processing.process_csv_file("", mode="bulk")
    else:
        for _, row in files_to_process.iterrows():
            extract_path = os.path.join(EXTRACT_DIR, row["file_name"].replace(".zip", ""))
            csv_path = find_first_csv(extract_path)
            if csv_path:
                processing.process_csv_file(csv_path, mode=mode, quality_check=qc_mode)
            else:
                logging.warning(f"No CSV found in {extract_path}, skipping")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Divvy pipeline")
    parser.add_argument("--mode", default="duckdb", help="Processing mode: duckdb, pandas, or bulk")
    parser.add_argument("--quality-check", action="store_true", help="Enable strict quality validation")
    args = parser.parse_args()

    run(mode=args.mode, quality_check=args.quality_check)
