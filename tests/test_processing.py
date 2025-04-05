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
    monkeypatch.setattr(processing, "DUCKDB_PATH", str(db_path))
    return db_path

def test_duckdb_mode_creates_table(sample_csv, duckdb_path):
    table_name = sample_csv.stem.replace("-", "_")

    success = processing.process_csv_file(str(sample_csv), mode="duckdb")
    assert success is True

    con = duckdb.connect(str(duckdb_path))
    result = con.execute(f"SELECT * FROM {table_name}").fetch_df()
    assert len(result) == 2
    assert "ride_id" in result.columns
    con.close()

def test_duckdb_appends_to_trips(sample_csv, duckdb_path):
    table_name = sample_csv.stem.replace("-", "_")
    processing.process_csv_file(str(sample_csv), mode="duckdb")

    con = duckdb.connect(str(duckdb_path))
    result = con.execute("SELECT * FROM trips").fetch_df()
    assert len(result) == 2
    assert "source_file" in result.columns
    assert all(result["source_file"] == table_name)
    con.close()

def test_pandas_mode(sample_csv):
    df = processing.process_csv_file(str(sample_csv), mode="pandas")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "ride_id" in df.columns