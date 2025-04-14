### processing.py
import os
import pandas as pd
import duckdb
import logging
from . import config
# from .config import DUCKDB_PATH, EXTRACT_DIR

logger = logging.getLogger(__name__)

def process_csv_file(file_path: str, mode: str = "pandas", quality_check: bool = False):
    logger.info(f"Processing file: {file_path} with mode: {mode}, quality_check={quality_check}")
    try:
        if mode == "duckdb":
            con = duckdb.connect(config.DUCKDB_PATH)

            # Extract table name from file name
            base_name = os.path.basename(file_path).replace(".csv", "")
            table_name = f"t_{base_name.replace('-', '_').replace(' ', '_')}"

            # 🔄 Set DuckDB parameters based on quality_check flag
            """ defaults:
            all_varchar: false
            auto_detect: true
            ignore_errors: false
            rejects_table: reject_errors
            store_rejects: false
            strict_mode: true
            sample_size: 20480
            union_by_name: false
            """
            
            if quality_check:
                # Strict mode: don't set ignore_errors or union_by_name, allow rejects_table
                query_create_raw = f"""
                    CREATE OR REPLACE TABLE {table_name} AS 
                    SELECT * 
                    FROM read_csv_auto('{file_path}'
                        -- All columns forced to string
                        , all_varchar=TRUE
                        -- Required for tracking malformed rows
                        , store_rejects=TRUE
                        , rejects_table='rejects'
                        -- Disable sampling
                        , sample_size=-1
                    )
                """
            else:
                # Production mode: skip malformed rows, allow loose schema alignment
                query_create_raw = f"""
                    CREATE OR REPLACE TABLE {table_name} AS 
                    SELECT * 
                    FROM read_csv_auto('{file_path}'
                        -- All columns forced to string
                        , all_varchar=TRUE
                        -- Skip bad rows automatically
                        , ignore_errors=TRUE
                        -- Align by column names
                        , union_by_name=TRUE
                        -- Disable sampling
                        , sample_size=-1
                    )
                """

            con.execute(query_create_raw)
            logger.info(f"Raw table created: {table_name}")

            # If quality_check is enabled, validate there are no rejected rows
            if quality_check:
                rejects = con.execute("SELECT COUNT(*) FROM rejects").fetchone()[0]
                if rejects > 0:
                    logger.warning(f"Rejects found in quality check: {rejects} rows")
                    con.close()
                    return None

            # Count rows in raw table
            raw_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

            # Create 'trips' table structure if it doesn't exist
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS trips AS 
                SELECT *, '{base_name}' AS source_file 
                FROM {table_name} 
                LIMIT 0
            """)

            # Insert new data
            con.execute(f"""
                INSERT INTO trips 
                SELECT *, '{base_name}' AS source_file 
                FROM {table_name}
            """)

            # Count rows inserted
            inserted_count = con.execute(f"""
                SELECT COUNT(*) FROM trips WHERE source_file = '{base_name}'
            """).fetchone()[0]

            logger.info(f"Inserted {inserted_count} of {raw_count} rows from: {file_path}")
            con.close()
            return True

        elif mode == "bulk":
            con = duckdb.connect(config.DUCKDB_PATH)
            con.execute(f"""
                CREATE OR REPLACE TABLE trips AS 
                SELECT *, filename AS source_file 
                FROM read_csv_auto('{config.EXTRACT_DIR}/*.csv'
                    , union_by_name=TRUE
                    , filename=TRUE
                )
            """)
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
