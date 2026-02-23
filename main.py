import argparse
from src.extract.download import run_extract
from src.transform.clean import run_transform
from src.load.build_warehouse import run_load


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--stage", choices=["extract", "transform", "load"], required=True)

    args = parser.parse_args()

    if args.stage == "extract":
        run_extract(args.year, args.month)
    elif args.stage == "transform":
        run_transform(args.year, args.month)
    elif args.stage == "load":
        run_load(args.year, args.month)


if __name__ == "__main__":
    main()