import duckdb

file_path = "data/raw/yellow_tripdata_2023-01.parquet"

con = duckdb.connect(database=":memory:")

# 1. number of rows
row_count = con.execute(f"""
    SELECT COUNT(*) FROM '{file_path}'
""").fetchone()[0]

print("Row count:", row_count)

# 2. schema
schema = con.execute(f"""
    DESCRIBE SELECT * FROM '{file_path}'
""").fetchall()

print("\nSchema:")
for col in schema:
    print(col)

# 3. NULL check
null_check = con.execute(f"""
    SELECT
        SUM(CASE WHEN trip_distance IS NULL THEN 1 ELSE 0 END) AS null_trip_distance,
        SUM(CASE WHEN fare_amount IS NULL THEN 1 ELSE 0 END) AS null_fare
    FROM '{file_path}'
""").fetchall()

print("\nNull check:", null_check)

# 4. num range check
range_check = con.execute(f"""
    SELECT
        MIN(trip_distance),
        MAX(trip_distance),
        MIN(fare_amount),
        MAX(fare_amount)
    FROM '{file_path}'
""").fetchall()

print("\nRange check:", range_check)

# 5. time check
time_check = con.execute(f"""
    SELECT COUNT(*)
    FROM '{file_path}'
    WHERE tpep_dropoff_datetime < tpep_pickup_datetime
""").fetchone()[0]

print("\nInvalid time records:", time_check)