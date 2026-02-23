import os
import duckdb


def build_filename(year: int, month: int) -> str:
    return f"yellow_tripdata_{year}-{month:02d}.parquet"


def run_load(year: int, month: int):
    os.makedirs("data/warehouse", exist_ok=True)

    filename = build_filename(year, month)
    cleaned_path = os.path.join("data/cleaned", filename)

    if not os.path.exists(cleaned_path):
        raise FileNotFoundError(
            f"Cleaned file not found: {cleaned_path}. Run transform first."
        )

    db_path = os.path.join("data/warehouse", "taxi.duckdb")
    con = duckdb.connect(db_path)

    # Create a table for this month (separate table is simplest & explicit)
    table_name = f"yellow_{year}_{month:02d}"

    # Replace table to keep idempotent for the same month
    con.execute(f"DROP TABLE IF EXISTS {table_name};")
    con.execute(
        f"CREATE TABLE {table_name} AS SELECT * FROM '{cleaned_path}';"
    )

    # Basic sanity checks (fast & practical)
    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
    min_pickup, max_pickup = con.execute(
        f"SELECT MIN(tpep_pickup_datetime), MAX(tpep_pickup_datetime) FROM {table_name};"
    ).fetchone()

    print(f"Warehouse DB: {db_path}")
    print(f"Loaded table: {table_name}")
    print(f"Rows: {row_count}")
    print(f"Pickup time range: {min_pickup} -> {max_pickup}")

    con.close()