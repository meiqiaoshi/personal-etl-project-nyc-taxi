# Data Validation Notes – 2023-01

Exploratory validation was performed on the raw dataset to identify potential data quality issues before implementing transform rules.

## Key Findings

- Total rows: 3,066,766
- No NULL values in `trip_distance` and `fare_amount`
- Extreme outliers detected:
  - `trip_distance` max: 258,928.15 miles (unrealistic)
  - `fare_amount` min: -900.0 (invalid negative fare)
- 3 records with dropoff time earlier than pickup time

## Initial Cleaning Rules Defined

- `trip_distance > 0`
- `trip_distance < 100`
- `fare_amount >= 0`
- `tpep_dropoff_datetime >= tpep_pickup_datetime`

These rules aim to remove obvious data corruption and extreme outliers while preserving valid trip records.