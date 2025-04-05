import pandas as pd
import pytest
from s3_divvy import metadata

def test_load_metadata_empty(tmp_path):
    # Simulate no existing metadata file
    original_path = metadata.METADATA_PATH
    metadata.METADATA_PATH = tmp_path / "nonexistent.csv"

    df = metadata.load_metadata()
    assert df.empty
    assert list(df.columns) == ["file_name", "size", "last_modified"]

    metadata.METADATA_PATH = original_path

def test_save_and_load_metadata(tmp_path):
    test_df = pd.DataFrame({
        "file_name": ["a.csv", "b.csv"],
        "size": [123, 456],
        "last_modified": pd.to_datetime(["2024-01-01", "2024-02-01"])
    })
    original_path = metadata.METADATA_PATH
    metadata.METADATA_PATH = tmp_path / "test_meta.csv"

    metadata.save_metadata(test_df)
    loaded = metadata.load_metadata()

    pd.testing.assert_frame_equal(loaded, test_df)

    metadata.METADATA_PATH = original_path

def test_compare_metadata():
    prev = pd.DataFrame({
        "file_name": ["a.csv", "b.csv"],
        "size": [123, 456],
        "last_modified": pd.to_datetime(["2024-01-01", "2024-02-01"])
    })
    curr = pd.DataFrame({
        "file_name": ["a.csv", "b.csv", "c.csv"],
        "size": [123, 789, 321],
        "last_modified": pd.to_datetime(["2024-01-01", "2024-03-01", "2024-04-01"])
    })

    result = metadata.compare_metadata(curr, prev)

    # Should detect "b.csv" as updated and "c.csv" as new
    assert len(result) == 2
    assert set(result["file_name"]) == {"b.csv", "c.csv"}