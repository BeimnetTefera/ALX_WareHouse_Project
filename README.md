# 🎓 ALX Data Warehouse ETL Pipeline (Bronze → Silver → Gold)
## 📘 Overview

This SQL-based ETL (Extract, Transform, Load) pipeline powers the ALX Data Warehouse, handling the full data flow from raw CSV ingestion to structured analytics-ready tables.

The project follows a medallion architecture:

- Bronze Layer → Raw data ingestion from CSV files.

- Silver Layer → Data cleaning and normalization.

- Gold Layer → Aggregated learner performance and analytics.


## Data WareHouse Diagram

<img width="881" height="467" alt="image" src="https://github.com/user-attachments/assets/65671d48-54ac-4932-bcc9-5bc581d3e38e" />

## 1. Bronze Layer — Data Ingestion

File: bronze.load_bronze.sql

# Purpose

Loads raw student activity data from a CSV file into the bronze.pw_learner_activity table.

# Key Features

- Checks if the CSV file exists before loading.

- Truncates the target table before new data insertion.

- Reads CSV as a single CLOB and splits it into rows/columns dynamically.

- Skips the CSV header during insertion.

- Logs each step for easy debugging.

# Dependencies

SQL Server’s xp_fileexist and OPENROWSET features.

The helper function dbo.SplitCSV for parsing CSV lines.

Example Run :-

  EXEC bronze.load_bronze;
