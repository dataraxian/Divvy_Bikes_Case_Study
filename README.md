# 🚲 Divvy Bikes Data Pipeline (S3 → DuckDB)

A modular, testable, and reproducible ETL pipeline designed to ingest **Divvy bike trip data** from a public **S3 bucket**, process and store it locally using **DuckDB**, and support downstream analytics or dashboarding.

---

## 📁 Project Structure

```
project-root/
├── s3_divvy/              # Core logic (modular ETL pipeline)
│   ├── config.py          # Config paths, flags, and constants
│   ├── core.py            # S3 listing, downloads, extraction, hashing
│   ├── metadata.py        # Metadata comparison + saving/loading
│   ├── processing.py      # CSV ingestion (pandas / DuckDB)
│   └── __init__.py
│
├── scripts/
│   └── run_pipeline.py    # Pipeline entrypoint script
│
├── data/                  # Local data directories
│   ├── zip/               # Downloaded ZIP files
│   ├── csv/               # Extracted CSVs
│   └── hash/              # SHA256 hashes of files
│
├── metadata/
│   └── file_metadata.csv  # Current S3 state snapshot
│
├── tests/                 # Unit + integration tests (pytest)
│   ├── test_core.py
│   ├── test_metadata.py
│   ├── test_processing.py
│   └── test_run_pipeline.py
│
├── requirements.txt
├── requirements-dev.txt
├── environment.yml
├── pytest.ini
└── README.md
```

---

## 🧠 Features

✅ Public S3 integration (HTTPS or boto3-optional)  
✅ Delta-aware metadata comparison (new + updated files only)  
✅ Safe file hashing (SHA-256) for reproducibility  
✅ Flexible processing backend: `pandas` or `duckdb`  
✅ Hybrid + bulk DuckDB ingestion modes  
✅ PyTest suite with full unit + integration coverage  
✅ Configurable, extensible structure for real-world scale

---

## 🚀 Quickstart

### 1. 📦 Setup the environment (via conda)
```bash
conda env create -f environment.yml
conda activate s3-divvy
```

### 2. ✅ Run tests
```bash
pytest -v
```

### 3. 🏃 Run the pipeline
```bash
python scripts/run_pipeline.py        # Default: duckdb mode
python scripts/run_pipeline.py bulk   # Optional: bulk mode
```

---

## ⚙️ Modes

### `duckdb` (default)
- Processes one file at a time
- Loads each into its own table
- Appends to a unified `trips` table (with `source_file` column)

### `bulk`
- Loads all CSVs using DuckDB’s `read_csv_auto()` with `union_by_name=True`
- Much faster for full refresh scenarios

---

## 📦 Tech Stack

- **Python 3.12**
- **DuckDB** – Local SQL engine for fast analytics
- **Pandas** – Metadata and fallback processing
- **Boto3 / Requests** – S3 listing and file downloads
- **PyTest** – Testing framework
- **Moto** – AWS mocking for tests

---

## 📊 Portfolio Use Case
- Designed to support **Power BI**, **Jupyter**, or **Streamlit** dashboards
- Can easily export from DuckDB → `.parquet`, `.csv`, or API-ready formats
- Fully local, portable, and reproducible

---

## 📈 Example Extensions
- ❓ Add CLI flags to control date filtering or destination
- 📊 Build a Power BI dashboard on top of `trips` table
- ☁️ Export to cloud warehouse (e.g., BigQuery or Snowflake)
- 🧪 Add GitHub Actions for CI

---

## 👤 Author
Jonathan [your name or GitHub]  
Built as part of a data analytics engineering portfolio ⚡

---

## 📄 License
MIT or your choice
