### run_pipeline.py
import os
import logging
from s3_divvy import core, metadata, processing
from s3_divvy.config import EXTRACT_DIR

logging.basicConfig(level=logging.INFO)

def run():
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

        csv_file_name = file_name.replace(".zip", ".csv")
        csv_path = os.path.join(extract_path, csv_file_name)
        processing.process_csv_file(csv_path)

    metadata.save_metadata(current_df)

if __name__ == "__main__":
    run()
