### processing.py
import os
import pandas as pd
import duckdb
import logging
from .config import DUCKDB_PATH, EXTRACT_DIR

logger = logging.getLogger(__name__)

def process_csv_file(file_path: str, mode: str = "pandas"):
    logger.info(f"Processing file: {file_path} with mode: {mode}")
    try:
        if mode == "duckdb":
            con = duckdb.connect(DUCKDB_PATH)

            # Extract table name from file name
            base_name = os.path.basename(file_path).replace(".csv", "")
            table_name = base_name.replace("-", "_").replace(" ", "_")

            # Create raw table
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}')")
            logger.info(f"Raw table created: {table_name}")

            # Append to unified 'trips' table with a source_file column
            con.execute(f"CREATE TABLE IF NOT EXISTS trips AS SELECT * FROM {table_name} LIMIT 0")
            con.execute(f"INSERT INTO trips SELECT *, '{base_name}' AS source_file FROM {table_name}")
            con.close()
            logger.info(f"Appended data to 'trips' table from: {file_path}")
            return True

        elif mode == "bulk":
            con = duckdb.connect(DUCKDB_PATH)
            con.execute(f"CREATE OR REPLACE TABLE trips AS SELECT *, filename AS source_file FROM read_csv_auto('{EXTRACT_DIR}/*.csv', union_by_name=True, filename=True)")
            con.close()
            logger.info("Bulk-loaded all CSVs into unified 'trips' table")
            return True

        else:
            df = pd.read_csv(file_path)
            logger.info(f"Loaded CSV with shape: {df.shape}")
            return df

    except Exception as e:
        logger.error(f"Failed to process CSV {file_path}: {e}")
        return None
