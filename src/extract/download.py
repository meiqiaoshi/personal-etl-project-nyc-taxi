import os
import requests
import duckdb


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def build_filename(year: int, month: int) -> str:
    return f"yellow_tripdata_{year}-{month:02d}.parquet"


def build_url(year: int, month: int) -> str:
    filename = build_filename(year, month)
    return f"{BASE_URL}/{filename}"


def download_file(url: str, output_path: str):
    with requests.get(url, stream=True) as response:
        if response.status_code != 200:
            raise Exception(f"Download failed with status {response.status_code}")

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)


def generate_metadata(file_path: str, year: int, month: int):
    con = duckdb.connect(database=':memory:')
    row_count = con.execute(f"SELECT COUNT(*) FROM '{file_path}'").fetchone()[0]

    column_info = con.execute(f"DESCRIBE SELECT * FROM '{file_path}'").fetchall()
    column_count = len(column_info)

    metadata_path = f"data/raw/metadata_{year}-{month:02d}.txt"

    with open(metadata_path, "w") as f:
        f.write(f"File: {file_path}\n")
        f.write(f"Rows: {row_count}\n")
        f.write(f"Columns: {column_count}\n\n")
        f.write("Schema:\n")
        for col in column_info:
            f.write(f"{col[0]} ({col[1]})\n")

    print(f"Metadata written to {metadata_path}")


def run_extract(year: int, month: int):
    os.makedirs("data/raw", exist_ok=True)

    filename = build_filename(year, month)
    output_path = os.path.join("data/raw", filename)

    if os.path.exists(output_path):
        print("File already exists. Skipping download.")
        return

    url = build_url(year, month)

    print(f"Downloading {filename}...")
    print(f"Source: {url}")

    download_file(url, output_path)

    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Download completed: {output_path}")
    print(f"File size: {file_size_mb:.2f} MB")
    generate_metadata(output_path, year, month)