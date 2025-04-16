### redundancy.py
import os
import duckdb
import shutil
from . import config

def _ensure_table():
    with duckdb.connect(config.LOG_DB_PATH) as con:
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {config.DUPLICATE_LOG_TABLE} (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            retained_file TEXT,
            removed_file TEXT,
            reason TEXT,
            removal_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

def remove_redundant_files(prefer="oldest"):
    _ensure_table()

    with duckdb.connect(config.LOG_DB_PATH) as con:
        df = con.execute(f"""
            SELECT file_name, sha256, MIN(download_time) as download_time
            FROM {config.DOWNLOAD_LOG_TABLE}
            WHERE sha256 IS NOT NULL
            GROUP BY sha256
            HAVING COUNT(*) > 1
        """).df()

        for _, row in df.iterrows():
            sha = row["sha256"]
            dups = con.execute(f"""
                SELECT file_name, download_time
                FROM {config.DOWNLOAD_LOG_TABLE}
                WHERE sha256 = ?
                ORDER BY download_time
            """, (sha,)).fetchall()

            if not dups or len(dups) < 2:
                continue

            if prefer == "newest":
                retained = dups[-1][0]
                to_remove = [x[0] for x in dups[:-1]]
            else:
                retained = dups[0][0]
                to_remove = [x[0] for x in dups[1:]]

            for file in to_remove:
                local_path = os.path.join(config.DOWNLOAD_DIR, file)
                if os.path.exists(local_path):
                    try:
                        os.remove(local_path)
                        con.execute(f"""
                            INSERT INTO {config.DUPLICATE_LOG_TABLE} (retained_file, removed_file, reason)
                            VALUES (?, ?, ?)
                        """, (retained, file, f"duplicate ({prefer})"))
                        print(f"Removed duplicate: {file}")
                    except Exception as e:
                        print(f"Failed to remove {file}: {e}")