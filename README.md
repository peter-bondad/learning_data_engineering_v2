# 🚀 Crypto Data Pipeline (ELT)

## 📌 Overview

This project is an end-to-end **ELT data pipeline** that ingests real-time cryptocurrency data from the CoinCap API, stores it in PostgreSQL, and transforms it using dbt into analytics-ready datasets.

The pipeline follows modern data engineering practices:

* **Extract & Load** handled by Python
* **Transformations** handled by dbt
* **Orchestration-ready** with Apache Airflow
* **Containerized** using Docker

---

## 🏗️ Architecture

```
CoinCap API
     ↓
Python (Extract & Load)
     ↓
PostgreSQL (staging schema)
     ↓
dbt (Transformations)
     ↓
Analytics Tables
```

---

## 📂 Project Structure

```
src/                # Python ingestion layer (extract/load)
coin_cap_dbt/       # dbt transformations
  ├── models/
  │   ├── staging/  # cleaned data
  │   └── marts/    # aggregated analytics
airflow/            # orchestration (DAGs)
docker-compose.yml  # infrastructure setup
```

---

## 🔄 Data Flow

1. Extract cryptocurrency data from CoinCap API
2. Load raw data into PostgreSQL (`staging.coin_cap`)
3. Transform data using dbt:

   * `stg_coin_cap` → cleaned dataset
   * `marts_coin_cap` → aggregated analytics
4. Query analytics-ready tables

---

## 🧠 Data Modeling

### 🔹 Staging Layer

* Cleans raw API data
* Standardizes schema
* Source defined via `source()`

### 🔹 Mart Layer

* Aggregates metrics per cryptocurrency
* Uses `ref()` for dependency tracking

---

## ✅ Data Quality

Implemented dbt tests to ensure data integrity:

* `not_null` → required fields (id, symbol, price, ingested_at)
* `unique` → primary key validation (id)

Run tests:

```bash
dbt test
```

---

## ⚡ Running the Project

### 1. Start services

```bash
docker-compose up -d
```

---

### 2. Run ingestion

```bash
python src/pipelines/coin_cap_pipeline.py
```

---

### 3. Run dbt transformations

```bash
cd coin_cap_dbt
dbt run
dbt test
```

---

## 🛠️ Tech Stack

* Python (data ingestion)
* PostgreSQL (data warehouse)
* dbt (data transformations)
* Apache Airflow (orchestration)
* Docker (containerization)

---

## 🚀 Future Improvements

* Implement incremental models for large-scale data processing
* Integrate dbt runs into Airflow DAG
* Add data lineage visualization (`dbt docs`)
* Introduce snapshotting for historical tracking

---

## 📊 Key Learnings

* Separation of concerns (Python for ingestion, dbt for transformation)
* Importance of data quality testing
* Designing scalable and modular data pipelines
* Applying ELT architecture in real-world scenarios

---
