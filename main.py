import argparse
from dataclasses import dataclass

from src.extract.download import run_extract
from src.transform.clean import run_transform
from src.load.build_warehouse import run_load
from src.marts.hourly_demand import run_mart_hourly_demand
from src.marts.daily_summary import run_mart_daily_summary
from src.pipeline.registry import ProcessingRegistry


@dataclass(frozen=True)
class YearMonth:
    year: int
    month: int


def parse_year_month(s: str) -> YearMonth:
    """
    Parse 'YYYY-MM' into YearMonth.
    """
    try:
        year_str, month_str = s.strip().split("-")
        year = int(year_str)
        month = int(month_str)
        if month < 1 or month > 12:
            raise ValueError("month out of range")
        return YearMonth(year, month)
    except Exception as e:
        raise argparse.ArgumentTypeError(f"Invalid year-month '{s}'. Use YYYY-MM.") from e


def iter_months(start: YearMonth, end: YearMonth):
    """
    Inclusive month iteration from start to end.
    """
    y, m = start.year, start.month
    end_key = end.year * 12 + end.month
    while (y * 12 + m) <= end_key:
        yield YearMonth(y, m)
        m += 1
        if m == 13:
            m = 1
            y += 1


def run_stage(stage: str, ym: YearMonth):
    if stage == "extract":
        run_extract(ym.year, ym.month)
    elif stage == "transform":
        run_transform(ym.year, ym.month)
    elif stage == "load":
        run_load(ym.year, ym.month)
    elif stage == "mart_hourly":
        run_mart_hourly_demand()
    elif stage == "mart_daily":
        run_mart_daily_summary()
    else:
        raise ValueError(f"Unknown stage: {stage}")


def main():
    parser = argparse.ArgumentParser(description="NYC Taxi ETL Pipeline")

    # Single-month mode
    parser.add_argument("--year", type=int, help="Year for single-month run")
    parser.add_argument("--month", type=int, help="Month (1-12) for single-month run")

    # Multi-month mode
    parser.add_argument("--start", type=parse_year_month, help="Start month in YYYY-MM (inclusive)")
    parser.add_argument("--end", type=parse_year_month, help="End month in YYYY-MM (inclusive)")

    parser.add_argument(
        "--stage",
        choices=["extract", "transform", "load", "all", "mart_hourly", "mart_daily"],
        required=True,
        help="Pipeline stage to run",
    )

    args = parser.parse_args()

    # Determine mode
    multi_month = args.start is not None or args.end is not None
    single_month = args.year is not None or args.month is not None

    if multi_month and single_month:
        raise SystemExit("Error: Use either (--year --month) OR (--start --end), not both.")

    if multi_month:
        if args.start is None or args.end is None:
            raise SystemExit("Error: Multi-month mode requires both --start and --end.")
        months = list(iter_months(args.start, args.end))
    else:
        if args.year is None or args.month is None:
            raise SystemExit("Error: Single-month mode requires both --year and --month.")
        if args.month < 1 or args.month > 12:
            raise SystemExit("Error: --month must be between 1 and 12.")
        months = [YearMonth(args.year, args.month)]

    stages = ["extract", "transform", "load"] if args.stage == "all" else [args.stage]

    registry = ProcessingRegistry("data/registry/processed_months.json")

    for ym in months:
        month_key = f"{ym.year}-{ym.month:02d}"

        # Only skip when running the full pipeline, because partial stages
        # may be intentionally rerun.
        if args.stage == "all" and registry.is_processed(month_key):
            print(f"\n=== Skipping {month_key} (already processed) ===")
            continue

        print(f"\n=== Processing {month_key} | stages: {', '.join(stages)} ===")

        try:
            for st in stages:
                run_stage(st, ym)
        except Exception as e:
            # Do NOT mark processed if anything failed
            print(f"!!! Failed processing {month_key}: {e}")
            raise

        # Mark processed only after all stages succeed
        if args.stage == "all":
            registry.mark_processed(month_key)
            print(f"Marked processed: {month_key}")

    print("\nDone.")


if __name__ == "__main__":
    main()