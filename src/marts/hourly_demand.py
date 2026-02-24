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


def run_mart_hourly_demand():
    db_path = os.path.join("data", "warehouse", "taxi.duckdb")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Warehouse DB not found: {db_path}")

    os.makedirs("data/marts", exist_ok=True)
    out_parquet = os.path.join("data", "marts", "mart_hourly_demand.parquet")

    con = duckdb.connect(db_path)

    tables = list_month_tables(con)
    if not tables:
        raise RuntimeError("No monthly tables found (expected tables like yellow_YYYY_MM).")

    # Union all months into a view
    union_sql = " UNION ALL ".join([f"SELECT * FROM {t}" for t in tables])
    con.execute("DROP VIEW IF EXISTS v_all_trips;")
    con.execute(f"CREATE VIEW v_all_trips AS {union_sql};")

    # Build mart table
    con.execute("DROP TABLE IF EXISTS mart_hourly_demand;")
    con.execute("""
        CREATE TABLE mart_hourly_demand AS
        SELECT
            date_trunc('hour', tpep_pickup_datetime) AS pickup_hour,
            COUNT(*) AS trips,
            SUM(total_amount) AS total_revenue,
            AVG(trip_distance) AS avg_trip_distance
        FROM v_all_trips
        GROUP BY 1
        ORDER BY 1;
    """)

    # Export mart to parquet
    con.execute(f"""
        COPY mart_hourly_demand
        TO '{out_parquet}'
        (FORMAT PARQUET);
    """)

    # quick sanity output
    n = con.execute("SELECT COUNT(*) FROM mart_hourly_demand;").fetchone()[0]
    min_h, max_h = con.execute("""
        SELECT MIN(pickup_hour), MAX(pickup_hour) FROM mart_hourly_demand;
    """).fetchone()

    print("Created mart_hourly_demand")
    print(f"Rows: {n}")
    print(f"Hour range: {min_h} -> {max_h}")
    print(f"Exported to: {out_parquet}")

    con.close()