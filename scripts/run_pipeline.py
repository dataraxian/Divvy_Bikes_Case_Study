### run_pipeline.py
import os
import logging
import argparse
from datetime import datetime, timezone
import duckdb

from s3_divvy import core, metadata, processing, ingestion_log, config

logging.basicConfig(level=logging.INFO)


def find_first_csv(directory):
    for fname in os.listdir(directory):
        if fname.lower().endswith(".csv"):
            return os.path.join(directory, fname)
    return None


def run(mode="duckdb", quality_check=None, force=False, dry_run=False):
    qc_mode = quality_check if quality_check is not None else config.QUALITY_CHECK_MODE

    current_df = core.list_s3_files()
    if current_df.empty:
        logging.info("No files to process.")
        return

    previous_df = metadata.load_metadata()
    if force:
        files_to_process = current_df
        logging.info("--force: Processing all available files.")
    else:
        files_to_process = metadata.compare_metadata(current_df, previous_df)

    if files_to_process.empty:
        logging.info("No new or updated files to process.")
        return

    for _, row in files_to_process.iterrows():
        file_name = row["file_name"]
        zip_path = core.download_file(file_name)
        if not zip_path:
            continue

        extract_path = os.path.join(config.EXTRACT_DIR, file_name.replace(".zip", ""))
        os.makedirs(extract_path, exist_ok=True)

        if dry_run:
            logging.info(f"[DRY RUN] Would extract {zip_path} to {extract_path}")
            logging.info(f"[DRY RUN] Would save hash for {zip_path}")
        else:
            core.extract_zip(zip_path, extract_path)
            core.save_file_hash(zip_path)

    if not dry_run:
        metadata.save_metadata(current_df)
    else:
        logging.info("[DRY RUN] Would update metadata store.")

    if mode == "bulk":
        start_dt = datetime.now(timezone.utc)
        if not dry_run:
            result = processing.process_csv_file("", mode="bulk")
        else:
            result = True
            logging.info("[DRY RUN] Would process all extracted CSVs in bulk mode.")
        end_dt = datetime.now(timezone.utc)

        if not dry_run:
            ingestion_log.log_ingestion_entry({
                "file_name": "<bulk>",
                "mode": mode,
                "quality_check": False,
                "start_time": start_dt.isoformat(timespec="seconds"),
                "end_time": end_dt.isoformat(timespec="seconds"),
                "duration_sec": round((end_dt - start_dt).total_seconds(), 1),
                "status": "success" if result else "failed",
                "inserted_rows": "",
                "reject_count": ""
            })

    else:
        for _, row in files_to_process.iterrows():
            file_name = row["file_name"]
            extract_path = os.path.join(config.EXTRACT_DIR, file_name.replace(".zip", ""))
            csv_path = find_first_csv(extract_path)
            if not csv_path:
                logging.warning(f"No CSV found in {extract_path}, skipping")
                continue

            start_dt = datetime.now(timezone.utc)
            if not dry_run:
                result = processing.process_csv_file(csv_path, mode=mode, quality_check=qc_mode)
            else:
                result = True
                logging.info(f"[DRY RUN] Would process file {csv_path} using mode='{mode}' and quality_check={qc_mode}")
            end_dt = datetime.now(timezone.utc)

            base_name = os.path.basename(csv_path).replace(".csv", "")
            reject_count = 0
            inserted_rows = 0
            status = "success"

            if result is True and not dry_run:
                with duckdb.connect(config.DUCKDB_PATH) as con:
                    inserted_rows = con.execute(
                        f"SELECT COUNT(*) FROM trips WHERE source_file = '{base_name}'"
                    ).fetchone()[0]

                    if qc_mode:
                        try:
                            reject_count = con.execute("SELECT COUNT(*) FROM rejects").fetchone()[0]
                            if reject_count > 0:
                                status = "rejected"
                        except Exception:
                            reject_count = 0
            elif result is not True:
                status = "failed"

            if not dry_run:
                ingestion_log.log_ingestion_entry({
                    "file_name": file_name.replace(".zip", ".csv"),
                    "mode": mode,
                    "quality_check": qc_mode,
                    "start_time": start_dt.isoformat(timespec="seconds"),
                    "end_time": end_dt.isoformat(timespec="seconds"),
                    "duration_sec": round((end_dt - start_dt).total_seconds(), 1),
                    "status": status,
                    "inserted_rows": inserted_rows,
                    "reject_count": reject_count
                })


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Divvy pipeline")
    parser.add_argument("--mode", default="duckdb", help="Processing mode: duckdb, pandas, or bulk")
    parser.add_argument("--quality-check", action="store_true", help="Enable strict quality validation")
    parser.add_argument("--force", action="store_true", help="Process all files regardless of metadata comparison")
    parser.add_argument("--dry-run", action="store_true", help="Simulate the pipeline without writing to DB or logs")
    args = parser.parse_args()

    run(mode=args.mode, quality_check=args.quality_check, force=args.force, dry_run=args.dry_run)
