### config.py
import os
from dotenv import load_dotenv

# Root project directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables from .env at root
load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"))

# S3
S3_BUCKET = os.getenv("S3_BUCKET", "divvy-tripdata")  # fallback default

# Data storage
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", os.path.join(BASE_DIR, "data", "zip"))
EXTRACT_DIR = os.getenv("EXTRACT_DIR", os.path.join(BASE_DIR, "data", "csv"))
HASH_DIR = os.getenv("HASH_DIR", os.path.join(BASE_DIR, "data", "hash"))
ROLLBACK_DIR = os.getenv("ROLLBACK_DIR", os.path.join(BASE_DIR, "data", "rollback"))

# Metadata & logs
METADATA_TABLE = "file_metadata"
DOWNLOAD_LOG_TABLE = "download_log"
ROLLBACK_LOG_TABLE = "rollback_log"
INGESTION_LOG_TABLE = "ingestion_log"
DUPLICATE_LOG_TABLE = "duplicate_log"

# Database paths
DUCKDB_PATH = os.getenv("DUCKDB_PATH", os.path.join(BASE_DIR, "data", "warehouse.duckdb"))
LOG_DB_PATH = os.getenv("LOG_DB_PATH", os.path.join(BASE_DIR, "data", "logs.duckdb"))

# Pipeline behavior
QUALITY_CHECK_MODE = os.getenv("QUALITY_CHECK_MODE", "True").lower() in ("true", "1", "yes")

# Dummy constant
DUMMY_CONSTANT = "dummy_constant"