# run_pipeline.py
import os
import logging
import argparse
import re
from datetime import datetime, timezone
import duckdb
import pandas as pd

from s3_divvy import (
    core, metadata, processing, ingestion_log,
    download_log, rollback, config
)

logging.basicConfig(level=logging.INFO)


def find_first_csv(directory):
    for fname in os.listdir(directory):
        if fname.lower().endswith(".csv"):
            return os.path.join(directory, fname)
    return None


def extract_date_string(file_name):
    match = re.search(r"\d{6,8}", file_name)
    return match.group(0) if match else ""


def run(mode="duckdb", quality_check=None, force_download=False, force_ingest=False, dry_run=False, limit=None, verify_hash=False):
    qc_mode = quality_check if quality_check is not None else config.QUALITY_CHECK_MODE
    now_ts = datetime.now(timezone.utc)

    current_df = core.list_s3_files()
    if current_df.empty:
        logging.info("No files to process.")
        return

    current_df["date_key"] = current_df["file_name"].apply(extract_date_string)
    current_df.sort_values(by=["date_key", "file_name"], ascending=[False, False], inplace=True)

    if limit:
        current_df = current_df.head(limit)
        logging.info(f"--limit {limit}: Restricting to {len(current_df)} most recent files")

    previous_df = metadata.load_metadata()

    if force_ingest:
        files_to_process = current_df
        logging.info("--force-ingest: Reprocessing all available files.")
    else:
        files_to_process = metadata.compare_metadata(current_df, previous_df)
        logging.info(f"{len(files_to_process)} new or updated files detected.")

    if files_to_process.empty:
        logging.info("No new or updated files to process.")
        return

    updated_metadata_rows = []

    for _, row in files_to_process.iterrows():
        file_name = row["file_name"]
        expected_etag = row.get("etag")
        expected_size = row["size"]
        last_modified = row["last_modified"]

        zip_path = None
        sha256 = None
        download_id = None
        result = None

        if dry_run:
            logging.info(f"[DRY RUN] Would download: {file_name}")
            continue

        zip_path, sha256, result = core.download_file(file_name, expected_etag, force_download=force_download)

        if not zip_path or result != "downloaded":
            logging.warning(f"Skipping {file_name} due to failed or skipped download.")
            continue

        prev_row = previous_df[previous_df.file_name == file_name]
        suspicious = False
        reason = None

        if not prev_row.empty:
            old_size = prev_row.iloc[0]["size"]
            old_sha256 = prev_row.iloc[0].get("sha256")

            if expected_size < old_size:
                suspicious = True
                reason = "Smaller file detected"
            elif old_sha256 and sha256 != old_sha256:
                suspicious = True
                reason = "SHA256 mismatch"

            if suspicious:
                rollback.archive_and_log_rollback(zip_path, old_sha256, sha256, reason)

        extract_path = os.path.join(config.EXTRACT_DIR, file_name.replace(".zip", ""))
        os.makedirs(extract_path, exist_ok=True)
        core.extract_zip(zip_path, extract_path)

        download_id = download_log.log_download({
            "file_name": file_name,
            "s3_etag": expected_etag,
            "sha256": sha256,
            "size": expected_size,
            "download_time": now_ts.isoformat(),
            "result": result
        })

        updated_metadata_rows.append({
            "file_name": file_name,
            "size": expected_size,
            "last_modified": last_modified,
            "etag": expected_etag,
            "sha256": sha256,
            "download_time": now_ts,
            "download_id": download_id
        })

    if not dry_run and updated_metadata_rows:
        metadata.save_metadata(pd.DataFrame(updated_metadata_rows))

    if mode == "bulk":
        process_bulk(mode, qc_mode, dry_run)
    else:
        for row in updated_metadata_rows:
            ingest_single(row["file_name"], mode, qc_mode, dry_run, verify_hash)


def process_bulk(mode, qc_mode, dry_run):
    start_dt = datetime.now(timezone.utc)
    result = True if dry_run else processing.process_csv_file("", mode=mode)
    end_dt = datetime.now(timezone.utc)

    if not dry_run:
        ingestion_log.log_ingestion_entry({
            "file_name": "<bulk>",
            "mode": mode,
            "quality_check": qc_mode,
            "start_time": start_dt.isoformat(timespec="seconds"),
            "end_time": end_dt.isoformat(timespec="seconds"),
            "duration_sec": round((end_dt - start_dt).total_seconds(), 1),
            "status": "success" if result else "failed",
            "inserted_rows": "",
            "reject_count": ""
        })


def ingest_single(file_name, mode, qc_mode, dry_run, verify_hash):
    extract_path = os.path.join(config.EXTRACT_DIR, file_name.replace(".zip", ""))
    csv_path = find_first_csv(extract_path)
    if not csv_path:
        logging.warning(f"No CSV found in {extract_path}, skipping")
        return

    if verify_hash:
        import hashlib
        expected_hash = None
        recomputed_hash = None

        try:
            sha256 = hashlib.sha256()
            with open(csv_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    sha256.update(chunk)
            recomputed_hash = sha256.hexdigest()

            with duckdb.connect(config.LOG_DB_PATH) as con:
                query = """
                    SELECT sha256 FROM csv_hash_log
                    WHERE file_name = ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                """
                result = con.execute(query, (os.path.basename(csv_path),)).fetchone()
                if result:
                    expected_hash = result[0]

            if expected_hash and recomputed_hash != expected_hash:
                logging.warning(f"SHA256 mismatch for {file_name}: {recomputed_hash} (expected: {expected_hash})")

        except Exception as e:
            logging.error(f"Failed to verify SHA256 for {csv_path}: {e}")

    start_dt = datetime.now(timezone.utc)
    result = True if dry_run else processing.process_csv_file(csv_path, mode=mode, quality_check=qc_mode)
    end_dt = datetime.now(timezone.utc)

    status = "success"
    inserted_rows = 0
    reject_count = 0

    if result is True and not dry_run:
        with duckdb.connect(config.DUCKDB_PATH) as con:
            base_name = os.path.basename(csv_path).replace(".csv", "")
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

    # CLI config overrides
    parser.add_argument("--bucket", help="Override the S3 bucket")
    parser.add_argument("--download-dir", help="Override path for downloaded ZIPs")
    parser.add_argument("--extract-dir", help="Override path for extracted CSVs")
    parser.add_argument("--duckdb-path", help="Path to DuckDB data warehouse")
    parser.add_argument("--logdb-path", help="Path to DuckDB log database")

    # Pipeline flags
    parser.add_argument("--mode", "-m", default="duckdb", help="Processing mode: duckdb, pandas, or bulk")
    parser.add_argument("--quality-check", "-q", action="store_true", help="Enable strict quality validation")
    parser.add_argument("--force-ingest", "-i", action="store_true", help="Force reprocessing all CSVs")
    parser.add_argument("--force-download", "-d", action="store_true", help="Force re-download all files")
    parser.add_argument("--force", "-f", action="store_true", help="Force both re-download and re-ingest")
    parser.add_argument("--limit", "-n", type=int, help="Only process the N most recent files")
    parser.add_argument("--dry-run", "-t", action="store_true", help="Simulate the pipeline without writing to DB or logs")
    parser.add_argument("--verify-hash", "-vh", action="store_true", help="Verify CSV hash before ingestion")

    args = parser.parse_args()
    config.override_from_args(args)

    if args.force:
        args.force_ingest = True
        args.force_download = True

    run(
        mode=args.mode,
        quality_check=args.quality_check,
        force_download=args.force_download,
        force_ingest=args.force_ingest,
        dry_run=args.dry_run,
        limit=args.limit,
        verify_hash=args.verify_hash
    )