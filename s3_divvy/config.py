### config.py
import os

# Bucket configuration
S3_BUCKET = os.getenv("S3_BUCKET", "divvy-tripdata")

# Directory settings
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
DOWNLOAD_DIR = os.path.join(DATA_DIR, "zip")
EXTRACT_DIR = os.path.join(DATA_DIR, "csv")
HASH_DIR = os.path.join(DATA_DIR, "hash")

# Download method
USE_BOTO3_DOWNLOAD = os.getenv("USE_BOTO3_DOWNLOAD", "false").lower() == "true"

# Metadata file path
METADATA_PATH = os.path.join(BASE_DIR, "..", "metadata", "file_metadata.csv")

# Create directories if not present
for directory in [DOWNLOAD_DIR, EXTRACT_DIR, HASH_DIR, os.path.dirname(METADATA_PATH)]:
    os.makedirs(directory, exist_ok=True)
    