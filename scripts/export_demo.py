import os
import duckdb

DB_PATH = "data/warehouse/taxi.duckdb"
OUT_PATH = "data/demo/mart_daily_sample.csv"

os.makedirs("data/demo", exist_ok=True)

if not os.path.exists(DB_PATH):
    raise FileNotFoundError(f"DuckDB warehouse not found: {DB_PATH}")

con = duckdb.connect(DB_PATH)

tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
if "mart_daily_summary" not in tables:
    raise RuntimeError(
        f"mart_daily_summary not found in {DB_PATH}. "
        f"Available tables: {tables[:20]}... "
        "Run: python main.py --year 2023 --month 1 --stage mart_daily"
    )

query = """
SELECT *
FROM mart_daily_summary
ORDER BY pickup_day
LIMIT 100
"""

df = con.execute(query).df()
df.to_csv(OUT_PATH, index=False)

con.close()
print(f"Demo dataset exported to: {OUT_PATH}")