### ingestion_log.py
import os
import csv
from datetime import datetime
from . import config
# from .config import INGESTION_LOG_PATH

# Ensure header is always consistent
FIELDNAMES = [
    "file_name", "mode", "quality_check",
    "start_time", "end_time", "duration_sec",
    "status", "inserted_rows", "reject_count"
]

def log_ingestion_entry(entry: dict):
    log_path = config.INGESTION_LOG_PATH
    is_new_file = not os.path.exists(log_path)

    with open(log_path, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)

        if is_new_file:
            writer.writeheader()

        writer.writerow(entry)

def create_log_entry(
    file_name: str,
    mode: str,
    quality_check: bool,
    start_time: datetime,
    end_time: datetime,
    status: str,
    inserted_rows: int = 0,
    reject_count: int = 0
) -> dict:
    duration = (end_time - start_time).total_seconds()
    return {
        "file_name": file_name,
        "mode": mode,
        "quality_check": quality_check,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_sec": round(duration, 1),
        "status": status,
        "inserted_rows": inserted_rows,
        "reject_count": reject_count
    }