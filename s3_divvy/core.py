### core.py
import os
import logging
import requests
import hashlib
import zipfile
from datetime import datetime, timezone
import pandas as pd
import boto3
import uuid
from botocore.exceptions import NoCredentialsError
from . import config

logger = logging.getLogger(__name__)
s3_client = boto3.client("s3")


def list_s3_files():
    """List all .zip files from the S3 bucket and include size, last_modified, ETag."""
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=config.S3_BUCKET)

        all_files = []
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".zip"):
                    all_files.append({
                        "file_name": key,
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"].isoformat(),
                        "etag": obj["ETag"].strip('"')
                    })

        return pd.DataFrame(all_files)
    except NoCredentialsError:
        logger.error("AWS credentials not found.")
        return pd.DataFrame()
    except Exception as e:
        logger.exception(f"Error listing S3 files: {e}")
        return pd.DataFrame()


def get_sha256(file_path):
    """Calculate SHA256 for a given file."""
    sha256 = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()
    except Exception as e:
        logger.warning(f"Failed to calculate SHA256 for {file_path}: {e}")
        return None


def download_file(file_name: str, expected_etag: str = None, force_download=False):
    """
    Download file from S3 unless it already exists and matches expected ETag.
    Returns a dict with metadata for further logging.
    """
    file_url = f"https://{config.S3_BUCKET}.s3.amazonaws.com/{file_name}"
    local_path = os.path.join(config.DOWNLOAD_DIR, file_name)
    os.makedirs(config.DOWNLOAD_DIR, exist_ok=True)

    file_status = "skipped"
    should_download = force_download or not os.path.exists(local_path)

    if not should_download:
        local_hash = get_sha256(local_path)
        if expected_etag and expected_etag == local_hash:
            logger.info(f"File exists and matches expected hash: {file_name}")
        else:
            logger.warning(f"Local file mismatch or unknown ETag: {file_name}")
            should_download = True

    if should_download:
        try:
            logger.info(f"Downloading: {file_url}")
            with requests.get(file_url, stream=True) as r:
                r.raise_for_status()
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            logger.info(f"Download complete: {file_name}")
            file_status = "downloaded"
        except Exception as e:
            logger.exception(f"Failed to download {file_name}")
            return {
                "file_name": file_name,
                "status": "failed",
                "path": None,
                "sha256": None,
                "etag_match": False,
                "download_time": datetime.now(timezone.utc).isoformat(),
                "download_id": str(uuid.uuid4())
            }

    # Calculate hash regardless
    sha256 = get_sha256(local_path)
    download_id = str(uuid.uuid4())

    return {
        "file_name": file_name,
        "status": file_status,
        "path": local_path,
        "sha256": sha256,
        "etag_match": expected_etag == sha256 if expected_etag else None,
        "download_time": datetime.now(timezone.utc).isoformat(),
        "download_id": download_id
    }


### core.py (append near the end)

def hash_and_log_csv(csv_path: str):
    import hashlib
    import duckdb
    from datetime import datetime

    sha256 = hashlib.sha256()
    with open(csv_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    hash_value = sha256.hexdigest()

    with duckdb.connect(config.LOG_DB_PATH) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS csv_hash_log (
                file_name TEXT,
                sha256 TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.execute("""
            INSERT INTO csv_hash_log (file_name, sha256)
            VALUES (?, ?)
        """, (os.path.basename(csv_path), hash_value))

    return hash_value


def extract_zip(file_path: str, extract_to: str):
    """Unzip contents and hash all .csv files post-extraction."""
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        logger.info(f"Extracted {file_path} to {extract_to}")

        for f in os.listdir(extract_to):
            if f.lower().endswith(".csv"):
                csv_path = os.path.join(extract_to, f)
                hash_val = hash_and_log_csv(csv_path)
                logger.info(f"Hashed CSV: {f} → {hash_val}")

    except zipfile.BadZipFile as e:
        logger.error(f"Failed to extract {file_path}: {e}")
        