import pytest
import pandas as pd
from pathlib import Path
import scripts.run_pipeline as run_pipeline
from s3_divvy import metadata, core

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

    # Simulate listing files in S3
    monkeypatch.setattr(core, "list_s3_files", lambda: pd.DataFrame({
        "file_name": ["dummy.csv"],
        "size": [1234],
        "last_modified": pd.to_datetime(["2024-02-01"])
    }))

    # Simulate downloading a file (create dummy ZIP manually)
    def fake_download(file_name):
        path = tmp_path / "zip" / file_name
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            f.write(b"Fake ZIP content")
        return str(path)
    monkeypatch.setattr(core, "download_file", fake_download)

    # Simulate extract_zip and process_csv_file (no-op)
    monkeypatch.setattr(core, "extract_zip", lambda src, dest: None)
    monkeypatch.setattr(core, "save_file_hash", lambda x: None)
    monkeypatch.setattr(run_pipeline.processing, "process_csv_file", lambda x, mode=None: True)

    # Run pipeline
    run_pipeline.run(mode="duckdb")
    assert True
