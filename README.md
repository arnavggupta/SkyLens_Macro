# SkyLens 360 — US Domestic Flight Operations Analytics Platform

> Data Engineering for AWS · Macro Project · Sigma DataTech · AWS Batch 1 · Team 1



---

## Overview

SkyLens 360 is an end-to-end data platform that ingests, cleans, and analyses **5.8 million US domestic flight records from 2015**. Raw CSVs flow through a Medallion Architecture (Bronze → Silver → Gold) on AWS + Databricks, with results visualised in a Streamlit dashboard.

**Catalog:** `skylens_macro` · **Schema:** `team1`

---

## Team

| ID | Name |
|---|---|
| TSV780 | Aditya Rajesh Gahukar |
| TSV859 | Aditya Sah |
| TSV771 | Anshu Kashyap |
| TSV781 | Arnav Gupta |
| TSV848 | Shubham Kumar |
| TSV839 | Soham Khanna |
| TSV755 | Suraj Kumar Singh |
| TSV865 | Suroj Verma |

---

## Architecture

```
12 Monthly CSVs + Reference Files
        ↓  (S3 Upload)
Bronze  →  Raw Delta tables (bronze_flights, bronze_airlines, bronze_airports)
        ↓  (Airflow DAG 2)
Silver  →  Cleaned, parsed, enriched (silver_flights)
        ↓  (Airflow DAG 3)
Gold    →  Star schema (2 fact tables + 7 dimension tables)
        ↓  (databricks-sql-connector)
Streamlit Dashboard (5 pages)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Storage | Amazon S3 |
| Ingestion | Databricks Auto Loader |
| Orchestration | Apache Airflow (AWS MWAA) |
| Processing | Databricks PySpark + Delta Lake |
| Warehouse | Databricks SQL Warehouse |
| Dashboard | Streamlit + Plotly |
| Version Control | Git + GitHub |

---

## Repository Structure

```
SkyLens_Macro/
├── ingestion/          # Bronze Auto Loader scripts
├── transforms/         # Silver PySpark transformation + enrichment jobs
├── warehouse/          # Gold DDL + ELT SQL (star schema)
├── orchestration/      # 3 Airflow DAGs (Bronze / Silver / Gold)
├── tests/              # data_quality_runner.py (44 automated DQ tests)
├── dashboard/          # Streamlit app (5 pages) + requirements.txt
├── data/               # airline_name_changes.csv (SCD2 simulation)
└── docs/               # Data dictionary, architecture diagram, screenshots
```

---

## How to Run

### 1. Upload data to S3
```bash
# Split flights.csv into 12 monthly files first
python ingestion/split_flights.py

# Then upload to S3
aws s3 cp data/flights/ s3://s3-de-q1-26/DE-Training/SkyLens_Macro_Team1/raw/flights/ --recursive
aws s3 cp data/airlines.csv s3://s3-de-q1-26/DE-Training/SkyLens_Macro_Team1/raw/airlines/
aws s3 cp data/airports.csv s3://s3-de-q1-26/DE-Training/SkyLens_Macro_Team1/raw/airports/
```

### 2. Trigger Airflow DAGs
```bash
airflow dags trigger dag_01_bronze_ingestion
airflow dags trigger dag_02_silver_transform
airflow dags trigger dag_03_gold_load
```

### 3. Run the dashboard locally
```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

---

## Environment Variables

Set these before running the dashboard — **never hardcode credentials**:

```bash
export DATABRICKS_HOST="your-workspace.azuredatabricks.net"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxxxx"
export DATABRICKS_TOKEN="dapixxxxxxxxxxxxxxxxx"
```

Get these from: **Databricks workspace → SQL Warehouses → Connection Details**

---

## Data Quality

44 automated tests across Bronze, Silver, and Gold layers. Results logged to `data_quality_log` Delta table.

```bash
python tests/data_quality_runner.py           # all layers
python tests/data_quality_runner.py BRONZE    # single layer
```

---

## Dashboard Pages

| Page | Description |
|---|---|
| Executive Summary | KPIs, monthly trends, top/bottom airlines |
| Airline Performance | Per-airline breakdown, delay types, worst routes |
| Route Analysis | Busiest/worst routes, route search, delay heatmap |
| Airport Operations | Taxi times, congestion, hourly delay trends |
| Data Quality | Test results, pass rate, layer row count comparison |

---

<div align="center">
  <sub>Built by Team 1 · Sigma DataTech · AWS Batch 1</sub>
</div>
