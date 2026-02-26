import os
from datetime import datetime
import duckdb


def _count(con: duckdb.DuckDBPyConnection, parquet_path: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]


def _null_stats(con: duckdb.DuckDBPyConnection, parquet_path: str, cols: list[str]) -> list[tuple[str, int, float]]:
    total = _count(con, parquet_path)
    results: list[tuple[str, int, float]] = []
    for c in cols:
        nulls = con.execute(
            f"SELECT SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) FROM '{parquet_path}'"
        ).fetchone()[0]
        nulls = int(nulls or 0)
        rate = (nulls / total) if total else 0.0
        results.append((c, nulls, rate))
    return results


def _min_max(con: duckdb.DuckDBPyConnection, parquet_path: str, col: str):
    return con.execute(
        f"SELECT MIN({col}), MAX({col}) FROM '{parquet_path}'"
    ).fetchone()


def _anomaly_counts(con: duckdb.DuckDBPyConnection, parquet_path: str, cfg: dict) -> dict:
    min_dist = cfg["cleaning"]["trip_distance"]["min"]
    max_dist = cfg["cleaning"]["trip_distance"]["max"]
    min_fare = cfg["cleaning"]["fare_amount"]["min"]
    require_time = cfg["cleaning"].get("require_valid_time_order", True)

    counts = {}

    counts["fare_negative"] = con.execute(
        f"SELECT COUNT(*) FROM '{parquet_path}' WHERE fare_amount < {min_fare}"
    ).fetchone()[0]

    counts["distance_out_of_range"] = con.execute(
        f"SELECT COUNT(*) FROM '{parquet_path}' WHERE trip_distance < {min_dist} OR trip_distance > {max_dist}"
    ).fetchone()[0]

    if require_time:
        counts["invalid_time_order"] = con.execute(
            f"""
            SELECT COUNT(*) FROM '{parquet_path}'
            WHERE tpep_dropoff_datetime < tpep_pickup_datetime
            """
        ).fetchone()[0]
    else:
        counts["invalid_time_order"] = None

    return counts


def generate_dq_report(
    year: int,
    month: int,
    raw_path: str,
    cleaned_path: str,
    cfg: dict,
    out_dir: str = "data/cleaned",
) -> str:
    """
    Generate a markdown DQ report comparing raw vs cleaned datasets.
    Returns the report path.
    """
    os.makedirs(out_dir, exist_ok=True)
    report_path = os.path.join(out_dir, f"dq_report_{year}-{month:02d}.md")

    con = duckdb.connect(database=":memory:")

    raw_rows = _count(con, raw_path)
    cleaned_rows = _count(con, cleaned_path)
    removed = raw_rows - cleaned_rows
    removed_ratio = (removed / raw_rows) if raw_rows else 0.0

    key_cols = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "PULocationID",
        "DOLocationID",
    ]

    raw_nulls = _null_stats(con, raw_path, key_cols)
    cleaned_nulls = _null_stats(con, cleaned_path, key_cols)

    # Ranges (raw and cleaned)
    raw_dist_min, raw_dist_max = _min_max(con, raw_path, "trip_distance")
    raw_fare_min, raw_fare_max = _min_max(con, raw_path, "fare_amount")

    cln_dist_min, cln_dist_max = _min_max(con, cleaned_path, "trip_distance")
    cln_fare_min, cln_fare_max = _min_max(con, cleaned_path, "fare_amount")

    # Anomalies
    raw_anom = _anomaly_counts(con, raw_path, cfg)

    con.close()

    ts = datetime.utcnow().isoformat() + "Z"

    def fmt_pct(x: float) -> str:
        return f"{x:.4%}"

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"# Data Quality Report — {year}-{month:02d}\n\n")
        f.write(f"Generated at: {ts}\n\n")

        f.write("## Summary\n\n")
        f.write(f"- Raw rows: **{raw_rows:,}**\n")
        f.write(f"- Cleaned rows: **{cleaned_rows:,}**\n")
        f.write(f"- Removed: **{removed:,}** ({fmt_pct(removed_ratio)})\n\n")

        f.write("## Cleaning Rules (from config)\n\n")
        f.write(f"- trip_distance in [{cfg['cleaning']['trip_distance']['min']}, {cfg['cleaning']['trip_distance']['max']}]\n")
        f.write(f"- fare_amount >= {cfg['cleaning']['fare_amount']['min']}\n")
        f.write(f"- require_valid_time_order = {cfg['cleaning'].get('require_valid_time_order', True)}\n\n")

        f.write("## Raw Anomaly Counts\n\n")
        f.write(f"- fare_negative: **{raw_anom['fare_negative']:,}**\n")
        f.write(f"- distance_out_of_range: **{raw_anom['distance_out_of_range']:,}**\n")
        if raw_anom["invalid_time_order"] is not None:
            f.write(f"- invalid_time_order: **{raw_anom['invalid_time_order']:,}**\n")
        f.write("\n")

        f.write("## Ranges (Raw vs Cleaned)\n\n")
        f.write("| Metric | Raw | Cleaned |\n")
        f.write("|---|---:|---:|\n")
        f.write(f"| trip_distance min | {raw_dist_min} | {cln_dist_min} |\n")
        f.write(f"| trip_distance max | {raw_dist_max} | {cln_dist_max} |\n")
        f.write(f"| fare_amount min | {raw_fare_min} | {cln_fare_min} |\n")
        f.write(f"| fare_amount max | {raw_fare_max} | {cln_fare_max} |\n\n")

        f.write("## Null Rates (Key Columns)\n\n")
        f.write("| Column | Raw nulls | Raw null rate | Cleaned nulls | Cleaned null rate |\n")
        f.write("|---|---:|---:|---:|---:|\n")
        cleaned_map = {c: (n, r) for c, n, r in cleaned_nulls}
        for c, rn, rr in raw_nulls:
            cn, cr = cleaned_map[c]
            f.write(f"| {c} | {rn:,} | {fmt_pct(rr)} | {cn:,} | {fmt_pct(cr)} |\n")

    return report_path