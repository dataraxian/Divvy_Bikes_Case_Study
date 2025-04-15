### core.py
import os
import logging
import requests
import hashlib
import zipfile
from botocore.exceptions import NoCredentialsError
import boto3
import pandas as pd
from datetime import datetime
from . import config
# from .config import S3_BUCKET, DOWNLOAD_DIR, EXTRACT_DIR, HASH_DIR, USE_BOTO3_DOWNLOAD

logger = logging.getLogger(__name__)
s3_client = boto3.client("s3")

def list_s3_files():
    try:
        response = s3_client.list_objects_v2(Bucket=config.S3_BUCKET)
        if "Contents" not in response:
            logger.warning("No contents in S3 bucket.")
            return pd.DataFrame()

        data = [
            {
                "file_name": obj["Key"],
                "size": obj["Size"],
                "last_modified": obj["LastModified"].isoformat()
            }
            for obj in response["Contents"]
            if obj["Key"].endswith(".zip")  # Only include .zip files
        ]
        return pd.DataFrame(data)
    except NoCredentialsError:
        logger.error("AWS credentials not found.")
        return pd.DataFrame()
    except Exception as e:
        logger.exception(f"Error listing S3 files: {e}")
        return pd.DataFrame()

def download_file(file_name: str):
    file_url = f"https://{config.S3_BUCKET}.s3.amazonaws.com/{file_name}"
    local_path = os.path.join(config.DOWNLOAD_DIR, file_name)

    if os.path.exists(local_path):
        logger.info(f"File already exists: {file_name}")
        return local_path

    try:
        logger.info(f"Downloading {file_url}")
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        logger.info(f"Downloaded: {file_name}")
        return local_path
    except Exception as e:
        logger.exception(f"Download failed for {file_url}: {e}")
        return None

def extract_zip(file_path: str, extract_to: str):
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        logger.info(f"Extracted {file_path} to {extract_to}")
    except zipfile.BadZipFile as e:
        logger.error(f"Failed to extract {file_path}: {e}")

def save_file_hash(file_path: str):
    hash_path = os.path.join(config.HASH_DIR, os.path.basename(file_path) + ".sha256")
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    with open(hash_path, 'w') as hash_file:
        hash_file.write(sha256.hexdigest())
    logger.info(f"Hash saved: {hash_path}")
