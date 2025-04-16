### ingestion_log.py
import duckdb
from . import config

def _ensure_table():
    with duckdb.connect(config.LOG_DB_PATH) as con:
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {config.INGESTION_LOG_TABLE} (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            file_name TEXT,
            mode TEXT,
            quality_check BOOLEAN,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duration_sec DOUBLE,
            status TEXT,
            inserted_rows BIGINT,
            reject_count BIGINT,
            download_id BIGINT
        );
        """)

def log_ingestion_entry(entry: dict):
    _ensure_table()
    with duckdb.connect(config.LOG_DB_PATH) as con:
        con.execute(f"""
        INSERT INTO {config.INGESTION_LOG_TABLE} (
            file_name, mode, quality_check,
            start_time, end_time, duration_sec,
            status, inserted_rows, reject_count, download_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entry.get("file_name"),
            entry.get("mode"),
            entry.get("quality_check"),
            entry.get("start_time"),
            entry.get("end_time"),
            entry.get("duration_sec"),
            entry.get("status"),
            entry.get("inserted_rows"),
            entry.get("reject_count"),
            entry.get("download_id")
        ))

def get_ingestion_log():
    _ensure_table()
    with duckdb.connect(config.LOG_DB_PATH) as con:
        return con.execute(f"SELECT * FROM {config.INGESTION_LOG_TABLE}").df()