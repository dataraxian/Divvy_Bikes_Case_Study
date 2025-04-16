### download_log.py
import duckdb
from datetime import datetime
from . import config

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {config.DOWNLOAD_LOG_TABLE} (
    download_id TEXT PRIMARY KEY,
    file_name TEXT NOT NULL,
    etag TEXT,
    sha256 TEXT,
    download_time TIMESTAMP,
    status TEXT,
    etag_match BOOLEAN,
    local_path TEXT
)
"""

def init_log_db():
    with duckdb.connect(config.LOG_DB_PATH) as con:
        con.execute(SCHEMA)

def log_download(entry: dict):
    """
    Save a single download event to DuckDB.
    """
    init_log_db()
    with duckdb.connect(config.LOG_DB_PATH) as con:
        con.execute(f"""
            INSERT INTO {config.DOWNLOAD_LOG_TABLE} (
                download_id, file_name, etag, sha256, download_time, status, etag_match, local_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entry.get("download_id"),
            entry.get("file_name"),
            entry.get("etag"),
            entry.get("sha256"),
            entry.get("download_time"),
            entry.get("status"),
            entry.get("etag_match"),
            entry.get("path")
        ))

def get_latest_downloads():
    """
    Returns the latest downloads for inspection.
    """
    init_log_db()
    with duckdb.connect(config.LOG_DB_PATH) as con:
        return con.execute(f"""
            SELECT * FROM {config.DOWNLOAD_LOG_TABLE}
            ORDER BY download_time DESC
            LIMIT 50
        """).fetchdf()
