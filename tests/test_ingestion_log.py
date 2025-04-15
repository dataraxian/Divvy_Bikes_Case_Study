### test_ingestion_log.py
import os
import csv
import pytest
from datetime import datetime, timezone
from s3_divvy import ingestion_log
from s3_divvy.config import INGESTION_LOG_PATH

@pytest.fixture(autouse=True)
def clean_log():
    if os.path.exists(INGESTION_LOG_PATH):
        os.remove(INGESTION_LOG_PATH)
    yield
    if os.path.exists(INGESTION_LOG_PATH):
        os.remove(INGESTION_LOG_PATH)

def test_log_file_created_and_entry_appended():
    now = datetime.now(timezone.utc)
    entry = ingestion_log.create_log_entry(
        file_name="202301-divvy.csv",
        mode="duckdb",
        quality_check=True,
        start_time=now,
        end_time=now,
        status="success",
        inserted_rows=10000,
        reject_count=0
    )
    ingestion_log.log_ingestion_entry(entry)

    assert os.path.exists(INGESTION_LOG_PATH)

    with open(INGESTION_LOG_PATH, newline="") as f:
        reader = list(csv.DictReader(f))
        assert len(reader) == 1
        row = reader[0]
        assert row["file_name"] == "202301-divvy.csv"
        assert row["status"] == "success"
        assert row["inserted_rows"] == "10000"
        assert row["reject_count"] == "0"

def test_multiple_entries_append_correctly():
    now = datetime.now(timezone.utc)
    for i in range(3):
        entry = ingestion_log.create_log_entry(
            file_name=f"file_{i}.csv",
            mode="duckdb",
            quality_check=False,
            start_time=now,
            end_time=now,
            status="success",
            inserted_rows=2,
            reject_count=0
        )
        ingestion_log.log_ingestion_entry(entry)

    with open(INGESTION_LOG_PATH, newline="") as f:
        reader = list(csv.DictReader(f))
        assert len(reader) == 3
        assert reader[0]["file_name"] == "file_0.csv"
        assert reader[2]["file_name"] == "file_2.csv"
