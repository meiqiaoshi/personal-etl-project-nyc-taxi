# Architecture Overview

## Objective

Build a modular batch ETL pipeline for NYC Yellow Taxi data that is:

- Reproducible
- Parameterized by year/month
- Easy to extend
- Suitable for local analytical workloads

---

## Data Flow

1. Extract  
   - Download monthly dataset
   - Store in raw layer

2. Transform  
   - Validate schema
   - Clean invalid records
   - Normalize data types
   - Output cleaned parquet files

3. Load  
   - Load into local DuckDB database
   - Create analytical tables
   - Enable SQL-based analysis

---

## Layered Structure

data/
- raw/
- cleaned/
- warehouse/

src/
- extract/
- transform/
- load/

---

## Execution Model

- CLI-driven
- Single-month processing
- Idempotent (safe to re-run)
- Logs basic metadata and validation results

---

## Future Extensions (Optional)

- Multi-month batch processing
- Data quality reporting
- Incremental loading
- Basic tests
