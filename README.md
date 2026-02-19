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

Phase 0 — Project initialization and planning.

Next milestone:
- Implement Extract stage for single-month ingestion.
