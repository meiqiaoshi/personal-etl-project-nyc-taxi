import os
import duckdb


def list_month_tables(con: duckdb.DuckDBPyConnection):
    rows = con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name LIKE 'yellow_%'
        ORDER BY table_name;
    """).fetchall()
    return [r[0] for r in rows]


def run_mart_daily_summary():
    db_path = os.path.join("data", "warehouse", "taxi.duckdb")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Warehouse DB not found: {db_path}")

    os.makedirs("data/marts", exist_ok=True)
    out_parquet = os.path.join("data", "marts", "mart_daily_summary.parquet")

    con = duckdb.connect(db_path)

    tables = list_month_tables(con)
    if not tables:
        raise RuntimeError("No monthly tables found (expected tables like yellow_YYYY_MM).")

    # Union all months into a view
    union_sql = " UNION ALL ".join([f"SELECT * FROM {t}" for t in tables])
    con.execute("DROP VIEW IF EXISTS v_all_trips;")
    con.execute(f"CREATE VIEW v_all_trips AS {union_sql};")

    # Build mart table
    con.execute("DROP TABLE IF EXISTS mart_daily_summary;")
    con.execute("""
        CREATE TABLE mart_daily_summary AS
        SELECT
            date_trunc('day', tpep_pickup_datetime) AS pickup_day,
            COUNT(*) AS trips,
            SUM(total_amount) AS total_revenue,
            AVG(total_amount) AS avg_total_amount,
            AVG(fare_amount) AS avg_fare_amount,
            AVG(trip_distance) AS avg_trip_distance
        FROM v_all_trips
        GROUP BY 1
        ORDER BY 1;
    """)

    # Export mart to parquet
    con.execute(f"""
        COPY mart_daily_summary
        TO '{out_parquet}'
        (FORMAT PARQUET);
    """)

    # quick sanity output
    n = con.execute("SELECT COUNT(*) FROM mart_daily_summary;").fetchone()[0]
    min_d, max_d = con.execute("""
        SELECT MIN(pickup_day), MAX(pickup_day) FROM mart_daily_summary;
    """).fetchone()

    print("Created mart_daily_summary")
    print(f"Rows: {n}")
    print(f"Day range: {min_d} -> {max_d}")
    print(f"Exported to: {out_parquet}")

    con.close()