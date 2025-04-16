### rollback.py
import os
import shutil
from datetime import datetime, timezone
import duckdb
from . import config

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {config.ROLLBACK_LOG_TABLE} (
    rollback_id TEXT PRIMARY KEY,
    file_name TEXT,
    replaced_sha256 TEXT,
    replaced_time TIMESTAMP,
    new_sha256 TEXT,
    new_time TIMESTAMP,
    reason TEXT,
    backup_path TEXT
)
"""

def init_rollback_log():
    with duckdb.connect(config.LOG_DB_PATH) as con:
        con.execute(SCHEMA)


def archive_and_log_rollback(file_path: str, old_sha256: str, new_sha256: str, reason: str) -> str:
    """
    Backs up the current file before it is replaced, logs rollback event.
    Returns the path to the archived file.
    """
    if not os.path.exists(file_path):
        return None

    init_rollback_log()

    file_name = os.path.basename(file_path)
    timestamp = datetime.now(timezone.utc).isoformat()#.strftime("%Y%m%dT%H%M%S")
    rollback_id = f"{file_name}-{timestamp}"
    archive_dir = os.path.join(config.ROLLBACK_DIR, file_name)
    os.makedirs(archive_dir, exist_ok=True)
    backup_path = os.path.join(archive_dir, f"{timestamp}.bak")

    shutil.copy2(file_path, backup_path)

    with duckdb.connect(config.LOG_DB_PATH) as con:
        con.execute(f"""
            INSERT INTO {config.ROLLBACK_LOG_TABLE} (
                rollback_id, file_name, replaced_sha256, replaced_time,
                new_sha256, new_time, reason, backup_path
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rollback_id,
            file_name,
            old_sha256,
            datetime.now(timezone.utc).isoformat()
,
            new_sha256,
            datetime.now(timezone.utc).isoformat()
,
            reason,
            backup_path
        ))

    return backup_path
