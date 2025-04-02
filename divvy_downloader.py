# divvy_downloader.py

import os
import re
import csv
import requests
from bs4 import BeautifulSoup
from zipfile import ZipFile
from datetime import datetime
from tqdm import tqdm

# --- Configuration ---
BASE_URL = "https://divvy-tripdata.s3.amazonaws.com/"
INDEX_URL = BASE_URL + "index.html"
ZIP_DIR = "zip"
EXTRACT_DIR = "extracted"
MANIFEST_FILE = "index_manifest.csv"

# --- Ensure folders exist ---
os.makedirs(ZIP_DIR, exist_ok=True)
os.makedirs(os.path.join(EXTRACT_DIR, "csv"), exist_ok=True)
os.makedirs(os.path.join(EXTRACT_DIR, "xlsx"), exist_ok=True)
os.makedirs(os.path.join(EXTRACT_DIR, "other"), exist_ok=True)

# --- Load existing manifest if present ---
manifest = {}
if os.path.exists(MANIFEST_FILE):
    with open(MANIFEST_FILE, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            manifest[row["file_name"]] = row["date_modified"]

# --- Download and parse the HTML index ---
print("Fetching file index...")
resp = requests.get(INDEX_URL)
soup = BeautifulSoup(resp.content, "html.parser")
rows = soup.select("table tr")[1:]  # skip header
print(f"Found {len(rows)} rows in the HTML table.") # DEBUG

# --- Collect updated or new files ---
to_download = []
for row in rows:
    cols = row.find_all("td")
    if len(cols) < 4:
        continue

    name = cols[0].text.strip()
    modified = cols[1].text.strip()
    file_type = cols[3].text.strip().lower()

    if not name.endswith(".zip") or file_type != "zip file":
        continue

    # Standardize datetime
    modified_dt = datetime.strptime(re.sub(r"(\d{1,2})(st|nd|rd|th)", r"\1", modified), "%m/%d/%Y %I:%M:%S %p")
    modified_str = modified_dt.isoformat()

    # Compare with manifest
    if name not in manifest or manifest[name] != modified_str:
        to_download.append((name, modified_str))

print(f"{len(to_download)} new or updated files found.")

# --- Download and extract updated files ---
for name, modified_str in tqdm(to_download, desc="Downloading"):
    url = BASE_URL + name
    zip_path = os.path.join(ZIP_DIR, name)

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    with ZipFile(zip_path, 'r') as zip_ref:
        for member in zip_ref.namelist():
            member_lower = member.lower()
            if member_lower.endswith(".csv"):
                dest_folder = os.path.join(EXTRACT_DIR, "csv")
            elif member_lower.endswith(".xlsx"):
                dest_folder = os.path.join(EXTRACT_DIR, "xlsx")
            else:
                dest_folder = os.path.join(EXTRACT_DIR, "other")

            zip_ref.extract(member, dest_folder)

    # Update manifest
    manifest[name] = modified_str

# --- Save manifest ---
with open(MANIFEST_FILE, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["file_name", "date_modified"])
    writer.writeheader()
    for file, date in sorted(manifest.items()):
        writer.writerow({"file_name": file, "date_modified": date})

print("✅ Done: All new/updated ZIPs downloaded, extracted, and tracked.")