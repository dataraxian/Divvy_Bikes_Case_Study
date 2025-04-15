### test_run_pipeline.py
import os
import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import scripts.run_pipeline as run_pipeline
from s3_divvy import metadata, core, ingestion_log, config

@pytest.fixture
def sample_metadata(tmp_path):
    df = pd.DataFrame({
        "file_name": ["dummy.csv"],
        "size": [1234],
        "last_modified": pd.to_datetime(["2024-01-01"])
    })
    path = tmp_path / "file_metadata.csv"
    df.to_csv(path, index=False)
    return path

def test_pipeline_runs(monkeypatch, tmp_path, sample_metadata):
    # Patch config paths
    monkeypatch.setattr(metadata, "METADATA_PATH", str(sample_metadata))
    monkeypatch.setattr(core, "DOWNLOAD_DIR", str(tmp_path / "zip"))
    monkeypatch.setattr(core, "EXTRACT_DIR", str(tmp_path / "csv"))
    monkeypatch.setattr(core, "HASH_DIR", str(tmp_path / "hash"))

    # Force log path to temp dir
    log_path = tmp_path / "file_ingestion_log.csv"
    monkeypatch.setattr("s3_divvy.config.INGESTION_LOG_PATH", str(log_path))

    # Simulate listing files in S3
    monkeypatch.setattr(core, "list_s3_files", lambda: pd.DataFrame({
        "file_name": ["dummy.csv"],
        "size": [1234],
        "last_modified": pd.to_datetime(["2024-02-01"])
    }))

    # Simulate downloading a file
    def fake_download(file_name):
        path = tmp_path / "zip" / file_name
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            f.write(b"Fake ZIP content")
        return str(path)
    monkeypatch.setattr(core, "download_file", fake_download)

    # Simulate extract_zip and save_file_hash
    monkeypatch.setattr(core, "extract_zip", lambda src, dest: None)
    monkeypatch.setattr(core, "save_file_hash", lambda x: None)

    # Simulate presence of extracted CSV
    extracted_csv_dir = tmp_path / "csv" / "dummy"
    extracted_csv_dir.mkdir(parents=True, exist_ok=True)
    csv_path = extracted_csv_dir / "dummy.csv"
    csv_path.write_text("ride_id,start_time\nA1,2025-01-01 10:00")

    # Mock processing to trigger log writing
    def fake_process_csv(csv_path, mode=None, quality_check=None):
        start = datetime.now(timezone.utc)
        end = start
        ingestion_log.log_ingestion_entry({
            "file_name": os.path.basename(csv_path),
            "mode": mode,
            "quality_check": quality_check,
            "start_time": start.isoformat(),
            "end_time": end.isoformat(),
            "duration_sec": 0.0,
            "status": "success",
            "inserted_rows": 1,
            "reject_count": 0
        })
        return True

    monkeypatch.setattr(run_pipeline.processing, "process_csv_file", fake_process_csv)

    # Ensure log directory exists
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Run pipeline
    run_pipeline.run(mode="duckdb")

    # ✅ Confirm log file was created
    assert log_path.exists(), "Expected ingestion log file was not created"

    # ✅ Confirm log contents
    log_df = pd.read_csv(log_path)
    assert not log_df.empty
    assert "file_name" in log_df.columns
    assert log_df.iloc[0]["file_name"] == "dummy.csv"