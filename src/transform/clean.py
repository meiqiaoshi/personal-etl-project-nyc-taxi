import os
import duckdb
from datetime import datetime


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
            WHERE trip_distance > 0
              AND trip_distance < 100
              AND fare_amount >= 0
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