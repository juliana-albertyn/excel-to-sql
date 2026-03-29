# Excel-to-SQL Pipeline

**Turn messy Excel files into clean, validated data — ready for reporting or databases.**

A lightweight, reliable ETL pipeline designed for real-world spreadsheets with inconsistent formats and errors.

---

## Who this is for

* Small businesses using Excel as a data source
* NGOs / schools managing manual datasets
* Anyone struggling with messy spreadsheets
* Developers needing a predictable import pipeline

---

## What you get

* Clean Excel file (**original + cleaned data**)
* Per-sheet CSV exports
* Clear error reporting
* Reproducible, structured pipeline

---

## Key features

* Deterministic ingestion (YAML-configured)
* Column validation (types, required fields, ranges, FK, datetime)
* Transparent cleaning (no hidden transformations)
* Structured logs for auditing and debugging

---

## How it works

1. Define schema and rules in YAML
2. Add your Excel files
3. Run the pipeline

Outputs:

* Clean Excel (before + after comparison)
* CSVs per sheet
* Logs with validation results

---

## Pipeline structure

* `pipeline.py` — orchestration
* `extractor.py` — load + structure checks
* `cleaner.py` — normalisation
* `transformer.py` — business rules
* `validator.py` — constraints
* `loader.py` — outputs

---

## Why this is different

* **Deterministic**: same input → same output
* **Transparent**: users can see exactly what changed
* **Practical**: built for messy, real-world Excel data

---

## Install

```bash
git clone https://github.com/<your-username>/excel_to_sql_pipeline.git
cd excel_to_sql_pipeline
pip install -r requirements.txt
```

Requirements: Python 3.10+, pandas, openpyxl

---

## Roadmap

* SQL export (tables, inserts, upserts)
* CLI
* JSON output

---

## Contributing

* Open issues / PRs
* Keep changes focused
* Add tests (`pytest`)
