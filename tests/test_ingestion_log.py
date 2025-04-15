### test_ingestion_log.py
import os
import csv
import pytest
from datetime import datetime, timezone
from s3_divvy import ingestion_log

@pytest.fixture
def temp_log_path(tmp_path):
    original_path = ingestion_log.config.INGESTION_LOG_PATH
    ingestion_log.config.INGESTION_LOG_PATH = tmp_path / "temp_log.csv"
    yield ingestion_log.config.INGESTION_LOG_PATH
    ingestion_log.config.INGESTION_LOG_PATH = original_path

def test_log_file_created_and_entry_appended(temp_log_path):
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

    assert os.path.exists(temp_log_path)

    with open(temp_log_path, newline="") as f:
        reader = list(csv.DictReader(f))
        assert len(reader) == 1
        row = reader[0]
        assert row["file_name"] == "202301-divvy.csv"
        assert row["status"] == "success"
        assert row["inserted_rows"] == "10000"
        assert row["reject_count"] == "0"

def test_multiple_entries_append_correctly(temp_log_path):
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

    with open(temp_log_path, newline="") as f:
        reader = list(csv.DictReader(f))
        assert len(reader) == 3
        assert reader[0]["file_name"] == "file_0.csv"
        assert reader[2]["file_name"] == "file_2.csv"
        