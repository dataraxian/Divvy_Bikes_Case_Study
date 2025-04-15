### metadata.py
import os
import pandas as pd
from datetime import datetime
from . import config
# from .config import METADATA_PATH


def load_metadata():
    if os.path.exists(config.METADATA_PATH):
        try:
            df = pd.read_csv(config.METADATA_PATH, parse_dates=["last_modified"])
            return df
        except Exception as e:
            print(f"Error loading metadata: {e}")
    return pd.DataFrame(columns=["file_name", "size", "last_modified"])

def save_metadata(df: pd.DataFrame):
    try:
        df.to_csv(config.METADATA_PATH, index=False)
    except Exception as e:
        print(f"Error saving metadata: {e}")

def compare_metadata(current_df: pd.DataFrame, previous_df: pd.DataFrame):
    if previous_df.empty:
        return current_df

    current_df["last_modified"] = pd.to_datetime(current_df["last_modified"])
    previous_df["last_modified"] = pd.to_datetime(previous_df["last_modified"])

    new_files = current_df[~current_df['file_name'].isin(previous_df['file_name'])]
    updated_files = current_df.merge(previous_df, on="file_name", suffixes=("_new", "_old"))
    updated_files = updated_files[
        updated_files["last_modified_new"] > updated_files["last_modified_old"]
    ][["file_name", "size_new", "last_modified_new"]]
    updated_files.columns = ["file_name", "size", "last_modified"]

    return pd.concat([new_files, updated_files], ignore_index=True)
