# divvy_downloader.py

import os
import requests
import zipfile
import pandas as pd
from tqdm import tqdm
from datetime import datetime

# Base URL for Divvy data
BASE_URL = "https://divvy-tripdata.s3.amazonaws.com/"
DOWNLOAD_DIR = "zip"
EXTRACT_DIR = "csv"

# List of file names (you may update this to scrape or generate based on metadata)
file_names = [
    "202101-divvy-tripdata.zip"
    ,"202102-divvy-tripdata.zip"
    ,"202103-divvy-tripdata.zip"
    ,"202104-divvy-tripdata.zip"
    ,"202105-divvy-tripdata.zip"
    ,"202106-divvy-tripdata.zip"
    ,"202107-divvy-tripdata.zip"
    ,"202108-divvy-tripdata.zip"
    ,"202109-divvy-tripdata.zip"
    ,"202110-divvy-tripdata.zip"
    ,"202111-divvy-tripdata.zip"
    ,"202112-divvy-tripdata.zip"
    ,"202201-divvy-tripdata.zip"
    ,"202202-divvy-tripdata.zip"
    ,"202203-divvy-tripdata.zip"
    ,"202204-divvy-tripdata.zip"
    ,"202205-divvy-tripdata.zip"
    ,"202206-divvy-tripdata.zip"
    ,"202207-divvy-tripdata.zip"
    ,"202208-divvy-tripdata.zip"
    ,"202209-divvy-tripdata.zip"
    ,"202210-divvy-tripdata.zip"
    ,"202211-divvy-tripdata.zip"
    ,"202212-divvy-tripdata.zip"
    ,"202301-divvy-tripdata.zip"
    ,"202302-divvy-tripdata.zip"
    ,"202303-divvy-tripdata.zip"
    ,"202304-divvy-tripdata.zip"
    ,"202305-divvy-tripdata.zip"
    ,"202306-divvy-tripdata.zip"
    ,"202307-divvy-tripdata.zip"
    ,"202308-divvy-tripdata.zip"
    ,"202309-divvy-tripdata.zip"
    ,"202310-divvy-tripdata.zip"
    ,"202311-divvy-tripdata.zip"
    ,"202312-divvy-tripdata.zip"
    ,"202401-divvy-tripdata.zip"
    ,"202402-divvy-tripdata.zip"
    ,"202403-divvy-tripdata.zip"
    ,"202404-divvy-tripdata.zip"
    ,"202405-divvy-tripdata.zip"
    ,"202406-divvy-tripdata.zip"
    ,"202407-divvy-tripdata.zip"
    ,"202408-divvy-tripdata.zip"
    ,"202409-divvy-tripdata.zip"
    ,"202410-divvy-tripdata.zip"
    ,"202411-divvy-tripdata.zip"
    ,"202412-divvy-tripdata.zip"
    ,"202501-divvy-tripdata.zip"
    ,"202502-divvy-tripdata.zip"
]
def download_file(file_url, file_path):
    """Download a file from a URL."""
    print(f"Downloading {file_url}...")
    try:
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)
        print(f"Downloaded {file_url}")
    except Exception as e:
        print(f"Error downloading {file_url}: {e}")

def extract_zip(file_path, extract_to):
    """Extract a ZIP file to a specified folder."""
    print(f"Extracting {file_path}...")
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"Extracted {file_path}")
    except zipfile.BadZipFile as e:
        print(f"Error extracting {file_path}: {e}")

def process_csv(file_path):
    """Process the extracted CSV file (example: load into DataFrame)."""
    print(f"Processing {file_path}...")
    try:
        df = pd.read_csv(file_path)
        # Example: check for missing values or any other necessary processing
        print(df.head())  # Placeholder: check the first few rows
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def main():
    """Main function to download, extract, and process the files."""
    # Ensure the directories exist
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(EXTRACT_DIR, exist_ok=True)
    
    # Loop through each file, download, extract, and process
    for file_name in file_names:
        file_url = BASE_URL + file_name
        download_path = os.path.join(DOWNLOAD_DIR, file_name)
        
        # Check if file already exists and download if not
        if not os.path.exists(download_path):
            download_file(file_url, download_path)
        
        # Extract ZIP
        extract_path = os.path.join(EXTRACT_DIR, file_name.replace('.zip', ''))
        if not os.path.exists(extract_path):  # Extract only if not already extracted
            os.makedirs(extract_path, exist_ok=True)
            extract_zip(download_path, extract_path)

        # Process the extracted CSV
        csv_file_path = os.path.join(extract_path, file_name.replace('.zip', '.csv'))
        if os.path.exists(csv_file_path):
            process_csv(csv_file_path)

if __name__ == "__main__":
    main()