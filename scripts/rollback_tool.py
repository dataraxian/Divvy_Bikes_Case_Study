### rollback_tool.py
import os
import argparse
from datetime import datetime, timezone
import shutil
import duckdb
from s3_divvy import config

def list_rollbacks(limit=5):
    with duckdb.connect(config.LOG_DB_PATH) as con:
        query = f"""
        SELECT rollback_id, file_name, replaced_time, reason
        FROM {config.ROLLBACK_LOG_TABLE}
        ORDER BY replaced_time DESC
        LIMIT {limit}
        """
        df = con.execute(query).fetchdf()
        if df.empty:
            print("⚠️ No rollback candidates found.")
        else:
            print(df)

def perform_rollback(rollback_id: str):
    with duckdb.connect(config.LOG_DB_PATH) as con:
        row = con.execute(f"""
            SELECT * FROM {config.ROLLBACK_LOG_TABLE}
            WHERE rollback_id = ?
        """, (rollback_id,)).fetchone()

        if not row:
            print(f"❌ No rollback found with ID: {rollback_id}")
            return

        file_name, backup_path = row[1], row[-1]
        dest_path = os.path.join(config.DOWNLOAD_DIR, file_name)

        if not os.path.exists(backup_path):
            print(f"❌ Backup file not found at: {backup_path}")
            return

        print(f"✅ Restoring {file_name} from backup...")
        shutil.copy2(backup_path, dest_path)

        # Update metadata to point to restored hash
        restored_sha256 = None
        with open(backup_path, "rb") as f:
            import hashlib
            sha = hashlib.sha256()
            while chunk := f.read(8192):
                sha.update(chunk)
            restored_sha256 = sha.hexdigest()

        con.execute(f"""
            UPDATE {config.METADATA_TABLE}
            SET sha256 = ?, download_time = ?, download_id = NULL
            WHERE file_name = ?
        """, (restored_sha256, datetime.now(timezone.utc).isoformat(), file_name))

        print(f"📝 Metadata updated. Rollback complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rollback management tool")
    parser.add_argument("--list", action="store_true", help="Show recent rollback candidates")
    parser.add_argument("--rollback-id", help="Specify a rollback_id to restore from backup")
    args = parser.parse_args()

    if args.list:
        list_rollbacks()
    elif args.rollback_id:
        perform_rollback(args.rollback_id)
    else:
        parser.print_help()