# Project Status (Handoff for Follow-up Discussion)

This document describes the current state of the NYC Yellow Taxi ETL project so another agent or human can assess what exists, what is missing, and what could be done next.

---

## Purpose and Scope

- **Goal**: A practice ETL pipeline for NYC TLC Yellow Taxi trip data: extract monthly Parquet from the public source, validate and clean, load into a local analytical store (DuckDB), and build simple marts.
- **Scope**: CLI-driven, batch, parameterized by year/month. Single-machine, no orchestration or streaming. Suited for learning and portfolio use.

---

## Repository Layout

```
.
├── main.py                 # CLI entry: --year/--month or --start/--end, --stage
├── requirements.txt       # requests, duckdb, pytest
├── README.md               # Overview, setup, usage
├── .gitignore              # data/, parquet, __pycache__, etc.
├── docs/
│   ├── PROJECT_STATUS.md   # This file
│   ├── architecture.md    # Data flow, layers, execution model
│   ├── roadmap.md          # Phase 0–4 (partially outdated)
│   ├── data_validation_notes.md
│   └── example_queries.md
├── src/
│   ├── extract/
│   │   └── download.py     # Download Parquet, write metadata
│   ├── transform/
│   │   └── clean.py        # Schema validation + cleaning, reports
│   ├── load/
│   │   └── build_warehouse.py  # Load into DuckDB, one table per month
│   └── marts/
│       ├── hourly_demand.py    # Hourly trips/revenue/avg distance
│       └── daily_summary.py    # Daily summary, export parquet
├── scripts/
│   └── explore.py         # Ad-hoc exploration on raw (hardcoded path)
└── tests/
    ├── __init__.py
    ├── test_main.py        # parse_year_month, iter_months, run_stage (mocked)
    └── test_extract.py     # build_filename, build_url
```

Runtime directories (created on demand, gitignored): `data/raw`, `data/cleaned`, `data/warehouse`, `data/marts`.

---

## What Is Implemented

### Extract (`src/extract/download.py`)

- Builds URL for NYC TLC monthly Parquet (`yellow_tripdata_YYYY-MM.parquet`), downloads to `data/raw/`.
- Skips download if file already exists.
- After download, uses DuckDB to count rows and `DESCRIBE` schema; writes `data/raw/metadata_YYYY-MM.txt`.

### Transform (`src/transform/clean.py`)

- **Schema validation (before any cleaning)**:
  - Required columns: `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `trip_distance`, `fare_amount`.
  - Type hints: TIMESTAMP for the two datetime columns, DOUBLE for trip_distance and fare_amount.
  - Reads actual schema via DuckDB `DESCRIBE` on the raw Parquet; compares; on mismatch writes a failure report (errors + full actual schema) to `data/cleaned/cleaning_report_YYYY-MM.txt` and raises `SchemaValidationError` (no cleaning is run).
- **Cleaning**: Filters with DuckDB: `trip_distance` in (0, 100), `fare_amount >= 0`, `tpep_dropoff_datetime >= tpep_pickup_datetime`. Writes cleaned Parquet to `data/cleaned/`. Writes success cleaning report (row counts, removed ratio, applied rules, schema validation: passed).

### Load (`src/load/build_warehouse.py`)

- Reads cleaned Parquet for the given year/month, creates/overwrites table `yellow_YYYY_MM` in `data/warehouse/taxi.duckdb`. Idempotent per month. Prints row count and pickup time range.

### Marts (`src/marts/hourly_demand.py`, `src/marts/daily_summary.py`)

- Both: discover all `yellow_*` tables in the warehouse, build a union view, aggregate (hourly or daily), create a mart table and export to `data/marts/*.parquet`. Mart stages do not take year/month; they use all loaded monthly tables.

### CLI (`main.py`)

- Modes: single month (`--year`, `--month`) or range (`--start YYYY-MM`, `--end YYYY-MM`).
- Stages: `extract`, `transform`, `load`, `all` (extract + transform + load), `mart_hourly`, `mart_daily`.
- For `all`, runs extract → transform → load for each month in range. When stage is `mart_hourly` or `mart_daily`, the same mart is run once per month in the loop (redundant but harmless).

---

## Tech Stack and Dependencies

- **Python**: Uses standard library plus `requests`, `duckdb`, `pytest` (see `requirements.txt`). No config file or env-based config; paths and rules are in code.
- **Data**: Parquet for raw and cleaned; DuckDB for transform logic, warehouse, and marts. No other DB or cloud.

---

## Tests

- **Location**: `tests/test_main.py`, `tests/test_extract.py`.
- **Coverage**: `parse_year_month` (valid/invalid), `iter_months` (single month, same year, cross year), `run_stage` for each stage (mocked so no I/O). Extract: `build_filename`, `build_url`. No tests for transform/load/marts (would require fixtures or mocks).
- **Run**: From repo root, `pytest tests/ -v` (or `python -m pytest tests/ -v`). No CI configured.

---

## Documentation Present

- **README.md**: Project intro, dataset, objectives, architecture summary, current status, setup, usage (single/month range, marts). Partially redundant with this file.
- **docs/architecture.md**: Data flow, layers, execution model, future extensions (some already done, e.g. multi-month).
- **docs/roadmap.md**: Phase 0–4; phases 1–3 and parts of 4 are done; roadmap is not fully updated.
- **docs/data_validation_notes.md**, **docs/example_queries.md**: Present; content not summarized here.

---

## What Is Not Done / Limitations

- No `utils/` package (planned in README structure).
- No logging (only `print`); no config file for paths or cleaning thresholds.
- No CI (e.g. GitHub Actions) to run tests.
- Mart stages are invoked in a loop over months although they ignore month; could be refactored for clarity.
- No incremental load strategy documented or implemented; each month is full replace.
- Explore script uses a hardcoded path and is for ad-hoc use only.

---

## Possible Follow-ups for Discussion

- **Align docs**: Update `docs/roadmap.md` and README “Possible next steps” to reflect schema validation and tests.
- **Quality/reporting**: Add simple data quality metrics (e.g. null rates, min/max) to cleaning report or a separate report.
- **Config**: Move paths and cleaning thresholds (e.g. trip_distance bounds) to a config file or env.
- **Logging**: Replace `print` with `logging` and consistent format.
- **CI**: Add a workflow to run `pytest tests/` on push/PR.
- **Orchestration**: Optional cron or Airflow/Dagster for scheduled monthly runs.
- **Further tests**: Unit tests for `validate_schema`, `get_actual_schema`; integration test with a small fixture Parquet if desired.

This status is accurate as of the last update to this file; code and other docs may have changed slightly.
