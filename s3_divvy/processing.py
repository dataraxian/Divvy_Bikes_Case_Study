### processing.py
import os
import pandas as pd
import logging
from .config import EXTRACT_DIR

logger = logging.getLogger(__name__)

def process_csv_file(file_path: str, mode: str = "pandas"):
    logger.info(f"Processing file: {file_path} with mode: {mode}")
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded CSV with shape: {df.shape}")
        return df  # Extendable to DuckDB, etc.
    except Exception as e:
        logger.error(f"Failed to process CSV {file_path}: {e}")
        return None