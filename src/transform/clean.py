import os
import duckdb


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

    # Skeleton transform (currently pass-through)
    con.execute(f"""
        COPY (
            SELECT *
            FROM '{raw_path}'
        )
        TO '{cleaned_path}'
        (FORMAT PARQUET);
    """)

    print(f"Cleaned file written to: {cleaned_path}")