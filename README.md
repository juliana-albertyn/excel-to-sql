
## Excel to SQL Pipeline

A structured data processing utility that converts messy Excel files into clean, validated SQL-ready datasets.

This project demonstrates:

* Data cleaning
* Column normalization
* Validation logic
* Error handling
* Logging
* SQL generation

Designed as a portfolio project to simulate real-world client data import workflows.

---

## 🚀 Project Overview

Real-world Excel files are often:

* Inconsistently formatted
* Containing missing values
* Using mixed data types
* Including redundant or junk columns

This tool:

1. Loads Excel input
2. Cleans and normalizes the data
3. Applies validation rules
4. Logs processing steps
5. Outputs structured SQL statements or database-ready files

---

## 🏗 Architecture

```
excel-to-sql-pipeline/
│
├── main.py
├── requirements.txt
├── README.md
│
├── src/
│   ├── parser.py
│   ├── cleaner.py
│   ├── validator.py
│   ├── sql_writer.py
│   └── logger.py
│
├── sample_data/
│   ├── messy_input.xlsx
│   └── output.sql
│
└── tests/
```

---

## 🧩 Features

* Structured logging
* Modular design
* Clear separation of concerns
* Data validation rules
* SQL insert generation
* Handles missing or malformed data
* Removes junk / redundant columns
* Outputs clean columns after original columns for comparison

---

## 🛠 Technologies Used

* Python 3.x
* pandas
* openpyxl
* logging

---

## ▶️ How to Run

### 1️⃣ Clone repository

```
git clone https://github.com/yourusername/excel-to-sql-pipeline.git
cd excel-to-sql-pipeline
```

### 2️⃣ Create virtual environment

```
python -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows
```

### 3️⃣ Install dependencies

```
pip install -r requirements.txt
```

### 4️⃣ Run the pipeline

```
python main.py
```

---

## 📊 Example Workflow

Input:

* Messy Excel sheet
* Inconsistent column names
* Missing values
* Mixed types

Output:

* Cleaned dataset
* Standardized column naming
* SQL INSERT statements
* Detailed processing log

---

## 🎯 Use Case Simulation

This project simulates a typical client scenario:

> “We have multiple Excel files from different sources. We need them cleaned and imported into our database.”

The pipeline demonstrates how such a workflow can be automated reliably.

---

## 🔐 Error Handling & Logging

The system logs:

* Invalid data rows
* Type conversion failures
* Missing required fields
* Duplicate entries

Logging ensures traceability and easier debugging.

---

## 📈 Future Improvements

* Direct database connection (PostgreSQL/MySQL)
* Configurable schema mapping
* CLI arguments
* Batch file processing
* Docker support

---

## 📄 License

MIT License

---