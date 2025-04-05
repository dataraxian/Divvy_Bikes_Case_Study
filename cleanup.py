import os
import shutil

# Paths to clean
CLEAN_PATHS = [
    "data/zip",
    "data/csv",
    "data/hash",
    "metadata/file_metadata.csv",
    "data/divvy.duckdb",
]

def clean_path(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
        print(f"🗑️  Removed directory: {path}")
    elif os.path.isfile(path):
        os.remove(path)
        print(f"🗑️  Removed file: {path}")
    else:
        print(f"⚠️  Skipped (not found): {path}")

def main():
    print("\n🧹 Cleaning up generated files...")
    for path in CLEAN_PATHS:
        clean_path(path)
    print("\n✅ Cleanup complete.\n")

if __name__ == "__main__":
    main()