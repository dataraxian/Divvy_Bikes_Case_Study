### metadata.py
import duckdb
import pandas as pd
from datetime import datetime
from . import config
import os

def ensure_metadata_table():
    """Create the file_metadata table if it doesn't exist."""
    with duckdb.connect(config.LOG_DB_PATH) as con:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {config.METADATA_TABLE} (
                file_name TEXT PRIMARY KEY,
                size BIGINT,
                last_modified TIMESTAMP,
                etag TEXT,
                sha256 TEXT,
                download_time TIMESTAMP,
                download_id UUID
            )
        """)

def load_metadata():
    """Load metadata from DuckDB table."""
    ensure_metadata_table()
    with duckdb.connect(config.LOG_DB_PATH) as con:
        try:
            df = con.execute(f"SELECT * FROM {config.METADATA_TABLE}").fetchdf()
            return df
        except Exception as e:
            print(f"Error loading metadata: {e}")
            return pd.DataFrame(columns=[
                "file_name", "size", "last_modified", "etag", "sha256", "download_time", "download_id"
            ])

def save_metadata(df: pd.DataFrame):
    """
    Save metadata to DuckDB file_metadata table.
    If file_name already exists, overwrite row.
    """
    ensure_metadata_table()
    with duckdb.connect(config.LOG_DB_PATH) as con:
        try:
            con.register("new_meta", df)
            con.execute(f"""
                INSERT INTO {config.METADATA_TABLE}
                SELECT * FROM new_meta
                ON CONFLICT(file_name) DO UPDATE SET
                    size=excluded.size,
                    last_modified=excluded.last_modified,
                    etag=excluded.etag,
                    sha256=excluded.sha256,
                    download_time=excluded.download_time,
                    download_id=excluded.download_id
            """)
        except Exception as e:
            print(f"Error saving metadata: {e}")

def compare_metadata(current_df: pd.DataFrame, previous_df: pd.DataFrame):
    """
    Compare new metadata (from S3) vs stored metadata (from DuckDB).
    Return only files that are new or changed.
    """
    if previous_df.empty:
        return current_df

    current_df["last_modified"] = pd.to_datetime(current_df["last_modified"])
    previous_df["last_modified"] = pd.to_datetime(previous_df["last_modified"])

    new_files = current_df[~current_df['file_name'].isin(previous_df['file_name'])]

    merged = current_df.merge(previous_df, on="file_name", suffixes=("_new", "_old"))
    changed = merged[
        (merged["etag_new"] != merged["etag_old"]) |
        (merged["size_new"] != merged["size_old"]) |
        (merged["last_modified_new"] > merged["last_modified_old"])
    ]

    updated_files = changed[["file_name", "size_new", "last_modified_new", "etag_new"]]
    updated_files.columns = ["file_name", "size", "last_modified", "etag"]

    if "sha256" in current_df.columns:
        updated_files = updated_files.merge(current_df[["file_name", "sha256"]], on="file_name", how="left")

    return pd.concat([new_files, updated_files], ignore_index=True)
