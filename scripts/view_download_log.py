### view_download_log.py
import pandas as pd
from s3_divvy import download_log

def main():
    print("\n📦 Latest 50 Downloads\n" + "-"*40)
    df = download_log.get_latest_downloads()

    if df.empty:
        print("No downloads logged yet.")
        return

    # Optional: limit fields shown
    display_cols = ["download_time", "file_name", "status", "sha256", "etag_match"]
    print(df[display_cols].to_markdown(index=False))

if __name__ == "__main__":
    main()