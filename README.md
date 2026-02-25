# NYC Yellow Taxi ETL

A modular, reproducible ETL pipeline built using NYC TLC Yellow Taxi trip data.

This project is designed to practice data engineering fundamentals:
- Data ingestion
- Schema validation
- Data cleaning and transformation
- Analytical data loading
- Reproducible batch processing

---

## 📊 Dataset

Source: NYC TLC Yellow Taxi trip records (public dataset)

The dataset contains detailed trip-level records including:
- Pickup and dropoff timestamps
- Trip distance
- Fare amount
- Payment type
- Location IDs
- Passenger count

The data is large-scale and well-suited for ETL practice.

---

## 🎯 Project Objectives

1. Build a clean, modular ETL pipeline (Extract → Transform → Load)
2. Ensure reproducible monthly batch processing
3. Implement basic data quality validation rules
4. Load processed data into a local analytical database
5. Maintain clear documentation and structured project milestones

---

## 🏗 Planned Architecture

Raw data  
↓  
Validation layer  
↓  
Cleaned data layer  
↓  
Analytical warehouse (DuckDB)

The pipeline will be CLI-driven and parameterized by year and month.

---

## 📁 Planned Structure

data/
raw/
cleaned/
warehouse/

src/
extract/
transform/
load/
utils/

---

## 🚀 Current Status

**Phase 1–2 — ETL pipeline and marts implemented.**

- **Extract**: Download monthly Parquet from NYC TLC; write to `data/raw/` with metadata (row count, schema).
- **Transform**: Clean with DuckDB (trip_distance 0–100, fare ≥ 0, dropoff ≥ pickup); output to `data/cleaned/` with cleaning reports.
- **Load**: Load cleaned data into DuckDB at `data/warehouse/taxi.duckdb` (one table per month, idempotent).
- **Marts**: `mart_hourly_demand` and `mart_daily_summary` built from warehouse; exported to `data/marts/*.parquet`.
- **CLI**: Single-month (`--year`, `--month`) and multi-month (`--start`, `--end`) with stages `extract`, `transform`, `load`, `all`, `mart_hourly`, `mart_daily`.

**Possible next steps:**
- Add `requirements.txt` and basic tests.
- Optional: explicit schema validation in the transform layer.
