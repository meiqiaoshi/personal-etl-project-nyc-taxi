from src.extract.download import BASE_URL, build_filename, build_url


def test_build_filename():
    assert build_filename(2023, 1) == "yellow_tripdata_2023-01.parquet"
    assert build_filename(2020, 12) == "yellow_tripdata_2020-12.parquet"
    assert build_filename(2024, 6) == "yellow_tripdata_2024-06.parquet"


def test_build_url():
    assert build_url(2023, 1) == f"{BASE_URL}/yellow_tripdata_2023-01.parquet"
    assert build_url(2020, 12) == f"{BASE_URL}/yellow_tripdata_2020-12.parquet"
