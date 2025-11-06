# 🎓 ALX Data Warehouse ETL Pipeline (Bronze → Silver → Gold)

## 📘 Overview

This SQL-based **ETL (Extract, Transform, Load)** pipeline powers the **ALX Data Warehouse**, handling the full data flow from raw CSV ingestion to structured analytics-ready tables.

The project follows a **medallion architecture**:

- 🥉 **Bronze Layer** → Raw data ingestion from CSV files
- 🥈 **Silver Layer** → Data cleaning and normalization
- 🥇 **Gold Layer** → Aggregated learner performance and analytics

---

## 🗂️ Data Warehouse Diagram

<img width="881" height="467" alt="Data Warehouse Diagram" src="https://github.com/user-attachments/assets/65671d48-54ac-4932-bcc9-5bc581d3e38e" />

---

## ⚙️ 1. Bronze Layer — Data Ingestion

**File:** `bronze.load_bronze.sql`

### Purpose
Loads raw student activity data from a CSV file into the `bronze.pw_learner_activity` table.

### Key Features
- Checks if the CSV file exists before loading
- Truncates the target table before new data insertion
- Reads CSV as a single CLOB and splits it into rows/columns dynamically
- Skips the CSV header during insertion
- Logs each step for easy debugging

### Dependencies
- SQL Server’s `xp_fileexist` and `OPENROWSET` features
- Helper function `dbo.SplitCSV` for parsing CSV lines

### Example Run
```sql
EXEC bronze.load_bronze;
```
# 2. Silver Layer — Data Cleaning & Standardization

**File:** `clean_bronze_data.sql`

## Purpose
Cleans the raw ingested bronze data to ensure **consistent formatting** for downstream analytics. This step removes unwanted characters like:

- Double quotes (`"`)
- Single quotes (`'`)

from all NVARCHAR/text columns.

## Why This Matters
When importing CSVs, text fields often retain quotation marks or extra spaces. These inconsistencies can:

- Break joins and lookups  
- Affect aggregations  
- Cause mismatched comparisons  

Cleaning the data ensures **reliable and accurate reporting** in the Gold Layer.

## Key Features
- Removes unwanted characters from all text columns  
- Standardizes phone numbers and emails  
- Ensures age ranges and course names are clean  
- Prepares data for aggregation in Gold Layer

## Example Run
```sql
-- Clean bronze data
EXEC clean_bronze_data;

-- Check cleaned data
SELECT TOP 10 * FROM bronze.pw_learner_activity;
```

## ⚙️ 3. Gold Layer — Learner Course Pivot & Dashboard

**File:** `generate_course_performance_pivot.sql`

### Purpose
Transforms the aggregated learner performance into a **course-wise pivot table** for easy reporting. This makes it simple to see **each learner’s submissions, passes, failed assignments, and missed assignments per course**.

### Key Features
- Converts course-level metrics into columns
- Handles multiple courses dynamically using `CASE` and `COALESCE`
- Formats phone numbers consistently
- Calculates an overall **Learner Status**:
  - **On Track** → LMS score ≥ 70
  - **Needs Improvement** → 50 ≤ score < 70
  - **Off Track** → LMS score < 50
