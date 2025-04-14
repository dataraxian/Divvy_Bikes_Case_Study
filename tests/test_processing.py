### test_processing.py
import duckdb
import pandas as pd
import pytest
from s3_divvy import processing

@pytest.fixture
def sample_csv(tmp_path):
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("""ride_id,rideable_type,started_at,ended_at,start_station_name,start_station_id,end_station_name,end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual
A1,classic_bike,2025-01-01 10:00,2025-01-01 10:10,Station A,STA001,Station B,STB001,41.0,-87.6,41.1,-87.7,member
A2,electric_bike,2025-01-02 12:00,2025-01-02 12:20,Station C,STA002,Station D,STB002,41.2,-87.5,41.3,-87.4,casual
""")
    return csv_path

@pytest.fixture
def duckdb_path(tmp_path, monkeypatch):
    db_path = tmp_path / "test.duckdb"
    monkeypatch.setattr(processing.config, "DUCKDB_PATH", str(db_path))
    return db_path

def test_duckdb_mode_creates_table(sample_csv, duckdb_path):
    table_name = sample_csv.stem.replace("-", "_")

    success = processing.process_csv_file(str(sample_csv), mode="duckdb", quality_check=False)
    assert success is True

    con = duckdb.connect(str(duckdb_path))
    result = con.execute(f"SELECT * FROM t_{table_name}").fetch_df()
    assert len(result) == 2
    assert "ride_id" in result.columns
    con.close()

def test_duckdb_appends_to_trips(sample_csv, duckdb_path):
    table_name = sample_csv.stem.replace("-", "_")
    processing.process_csv_file(str(sample_csv), mode="duckdb", quality_check=False)

    con = duckdb.connect(str(duckdb_path))
    result = con.execute("SELECT * FROM trips").fetch_df()
    assert len(result) == 2
    assert "source_file" in result.columns
    assert all(result["source_file"] == table_name)
    con.close()

def test_duckdb_quality_check_fails_on_bad_csv(tmp_path, duckdb_path):
    # Create a malformed CSV with an invalid line format
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text("id,value\n1,good\n\"UNTERMINATED\"\n2,good")

    success = processing.process_csv_file(str(bad_csv), mode="duckdb", quality_check=True)
    # Should fail parsing and return None due to reject count
    assert success is None

def test_duckdb_quality_check_passes_on_good_csv(sample_csv, duckdb_path):
    success = processing.process_csv_file(str(sample_csv), mode="duckdb", quality_check=True)
    assert success is True

def test_bulk_mode_loads_multiple_csvs(tmp_path, duckdb_path):
    # Create mock CSV files in a directory
    extract_dir = tmp_path / "csv"
    extract_dir.mkdir(parents=True, exist_ok=True)

    # Create 3 small CSV files with 2 rows each
    for i in range(3):
        file = extract_dir / f"sample_{i}.csv"
        file.write_text("ride_id,started_at\nX1,2025-01-01\nX2,2025-01-02\n")

    # Patch EXTRACT_DIR to the test directory
    from s3_divvy import config
    original_extract_dir = config.EXTRACT_DIR
    config.EXTRACT_DIR = str(extract_dir)

    success = processing.process_csv_file("", mode="bulk")
    assert success is True

    con = duckdb.connect(str(duckdb_path))
    result = con.execute("SELECT * FROM trips").fetch_df()
    assert len(result) == 6  # 3 files × 2 rows
    assert "ride_id" in result.columns
    con.close()

    config.EXTRACT_DIR = original_extract_dir  # Restore config

def test_bulk_mode_rejects_quality_check(tmp_path, duckdb_path):
    # Should reject bulk + quality_check
    success = processing.process_csv_file("", mode="bulk", quality_check=True)
    assert success is None

def test_pandas_mode(sample_csv):
    df = processing.process_csv_file(str(sample_csv), mode="pandas")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "ride_id" in df.columns
