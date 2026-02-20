import argparse
from src.extract.download import run_extract


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)

    args = parser.parse_args()

    run_extract(args.year, args.month)


if __name__ == "__main__":
    main()