import os
import duckdb
from datetime import datetime
from src.config import load_config


config = load_config()

min_dist = config["cleaning"]["trip_distance"]["min"]
max_dist = config["cleaning"]["trip_distance"]["max"]
min_fare = config["cleaning"]["fare_amount"]["min"]

# Columns that must exist for the cleaning SQL to work (and optional type hint: substring to match)
REQUIRED_COLUMNS = {
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
}
# Optional: require these types to contain the given substring (e.g. TIMESTAMP, DOUBLE)
EXPECTED_TYPE_HINT = {
    "tpep_pickup_datetime": "TIMESTAMP",
    "tpep_dropoff_datetime": "TIMESTAMP",
    "trip_distance": "DOUBLE",
    "fare_amount": "DOUBLE",
}


class SchemaValidationError(Exception):
    """Raised when raw data schema does not match expectations."""

    pass


def get_actual_schema(con: duckdb.DuckDBPyConnection, raw_path: str) -> list[tuple[str, str]]:
    """Return list of (column_name, column_type) for the parquet file."""
    rows = con.execute(f"DESCRIBE SELECT * FROM '{raw_path}'").fetchall()
    return [(r[0], r[1]) for r in rows]


def validate_schema(
    actual: list[tuple[str, str]],
    required_columns: set[str],
    type_hints: dict[str, str] | None = None,
) -> list[str]:
    """
    Check that required columns exist and (optionally) types match.
    Returns list of error messages; empty means valid.
    """
    errors = []
    actual_names = {name for name, _ in actual}
    actual_type_by_name = {name: typ for name, typ in actual}

    for col in required_columns:
        if col not in actual_names:
            errors.append(f"Missing required column: {col}")
            continue
        if type_hints and col in type_hints:
            hint = type_hints[col]
            actual_type = actual_type_by_name.get(col, "")
            if hint not in actual_type.upper():
                errors.append(f"Column '{col}' has type '{actual_type}', expected to contain '{hint}'")

    return errors


def build_filename(year: int, month: int) -> str:
    return f"yellow_tripdata_{year}-{month:02d}.parquet"


def run_transform(year: int, month: int):
    os.makedirs("data/cleaned", exist_ok=True)

    filename = build_filename(year, month)

    raw_path = os.path.join("data/raw", filename)
    cleaned_path = os.path.join("data/cleaned", filename)

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    print(f"Transforming {filename}...")

    con = duckdb.connect(database=":memory:")

    # Schema validation: fail fast before cleaning
    actual_schema = get_actual_schema(con, raw_path)
    schema_errors = validate_schema(
        actual_schema,
        REQUIRED_COLUMNS,
        type_hints=EXPECTED_TYPE_HINT,
    )
    if schema_errors:
        report_path = f"data/cleaned/cleaning_report_{year}-{month:02d}.txt"
        os.makedirs("data/cleaned", exist_ok=True)
        with open(report_path, "w") as f:
            f.write(f"Schema validation FAILED - {year}-{month:02d}\n")
            f.write(f"Generated at: {datetime.utcnow().isoformat()} UTC\n\n")
            f.write("Errors:\n")
            for e in schema_errors:
                f.write(f"  - {e}\n")
            f.write("\nActual schema (column, type):\n")
            for name, typ in actual_schema:
                f.write(f"  {name}: {typ}\n")
        print("Schema validation failed:")
        for e in schema_errors:
            print(f"  - {e}")
        raise SchemaValidationError("; ".join(schema_errors))

    print("Schema validation: passed")

    # Count raw rows
    raw_count = con.execute(
        f"SELECT COUNT(*) FROM '{raw_path}'"
    ).fetchone()[0]

    print(f"Raw rows: {raw_count}")

    # Apply cleaning rules
    con.execute(f"""
        COPY (
            SELECT *
            FROM '{raw_path}'
            WHERE trip_distance >= {min_dist}
              AND trip_distance <= {max_dist}
              AND fare_amount >= {min_fare}
              AND tpep_dropoff_datetime >= tpep_pickup_datetime
        )
        TO '{cleaned_path}'
        (FORMAT PARQUET);
    """)

    # Count cleaned rows
    cleaned_count = con.execute(
        f"SELECT COUNT(*) FROM '{cleaned_path}'"
    ).fetchone()[0]

    removed_count = raw_count - cleaned_count
    removed_ratio = removed_count / raw_count if raw_count > 0 else 0

    print(f"Cleaned rows: {cleaned_count}")
    print(f"Removed rows: {removed_count} ({removed_ratio:.4%})")

    # Write cleaning report
    report_path = f"data/cleaned/cleaning_report_{year}-{month:02d}.txt"

    with open(report_path, "w") as f:
        f.write(f"Cleaning Report - {year}-{month:02d}\n")
        f.write(f"Generated at: {datetime.utcnow().isoformat()} UTC\n\n")
        f.write("Schema validation: passed\n\n")
        f.write(f"Raw rows: {raw_count}\n")
        f.write(f"Cleaned rows: {cleaned_count}\n")
        f.write(f"Removed rows: {removed_count}\n")
        f.write(f"Removed ratio: {removed_ratio:.4%}\n\n")
        f.write("Applied Rules:\n")
        f.write("- trip_distance > 0\n")
        f.write("- trip_distance < 100\n")
        f.write("- fare_amount >= 0\n")
        f.write("- dropoff >= pickup\n")

    print(f"Cleaning report written to: {report_path}")
    print(f"Transform completed for {filename}")