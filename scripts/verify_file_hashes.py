### verify_file_hashes.py
import os
import hashlib
import argparse
from s3_divvy import config, metadata

def compute_sha256(path):
    sha = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha.update(chunk)
        return sha.hexdigest()
    except Exception as e:
        return None

def verify_hashes():
    df = metadata.load_metadata()

    if df.empty:
        print("No metadata found.")
        return

    mismatches = []
    missing = []

    print(f"Verifying {len(df)} files...\n")
    for _, row in df.iterrows():
        file_name = row["file_name"]
        expected_sha = row.get("sha256")

        local_path = os.path.join(config.DOWNLOAD_DIR, file_name)
        if not os.path.exists(local_path):
            missing.append(file_name)
            continue

        actual_sha = compute_sha256(local_path)
        if expected_sha and actual_sha != expected_sha:
            mismatches.append((file_name, expected_sha, actual_sha))

    print("✅ Complete.\n")

    if missing:
        print(f"❌ Missing files ({len(missing)}):")
        for m in missing:
            print(f" - {m}")
        print()

    if mismatches:
        print(f"⚠️ Hash mismatches ({len(mismatches)}):")
        for fname, expected, actual in mismatches:
            print(f" - {fname}\n   expected: {expected}\n   actual:   {actual}")
        print()

    if not missing and not mismatches:
        print("🎉 All hashes match local files.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify downloaded files match metadata SHA256 hashes")
    args = parser.parse_args()
    verify_hashes()