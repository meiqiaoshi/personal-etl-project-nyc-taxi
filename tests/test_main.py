import argparse
from unittest.mock import patch

import pytest

from main import YearMonth, iter_months, parse_year_month, run_stage


class TestParseYearMonth:
    def test_valid(self):
        assert parse_year_month("2023-01") == YearMonth(2023, 1)
        assert parse_year_month("2020-12") == YearMonth(2020, 12)
        assert parse_year_month("  2024-06  ") == YearMonth(2024, 6)

    def test_invalid_month_too_low(self):
        with pytest.raises(argparse.ArgumentTypeError) as exc_info:
            parse_year_month("2023-00")
        assert "Invalid year-month" in str(exc_info.value)

    def test_invalid_month_too_high(self):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_year_month("2023-13")

    def test_invalid_format(self):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_year_month("2023/01")
        with pytest.raises(argparse.ArgumentTypeError):
            parse_year_month("not-a-date")
        with pytest.raises(argparse.ArgumentTypeError):
            parse_year_month("2023")


class TestIterMonths:
    def test_single_month(self):
        start = YearMonth(2023, 5)
        end = YearMonth(2023, 5)
        assert list(iter_months(start, end)) == [YearMonth(2023, 5)]

    def test_multiple_months_same_year(self):
        start = YearMonth(2023, 1)
        end = YearMonth(2023, 3)
        assert list(iter_months(start, end)) == [
            YearMonth(2023, 1),
            YearMonth(2023, 2),
            YearMonth(2023, 3),
        ]

    def test_cross_year(self):
        start = YearMonth(2023, 11)
        end = YearMonth(2024, 2)
        assert list(iter_months(start, end)) == [
            YearMonth(2023, 11),
            YearMonth(2023, 12),
            YearMonth(2024, 1),
            YearMonth(2024, 2),
        ]


class TestRunStage:
    def test_extract_calls_run_extract(self):
        with patch("main.run_extract") as m:
            run_stage("extract", YearMonth(2023, 1))
            m.assert_called_once_with(2023, 1)

    def test_transform_calls_run_transform(self):
        with patch("main.run_transform") as m:
            run_stage("transform", YearMonth(2023, 1))
            m.assert_called_once_with(2023, 1)

    def test_load_calls_run_load(self):
        with patch("main.run_load") as m:
            run_stage("load", YearMonth(2023, 1))
            m.assert_called_once_with(2023, 1)

    def test_mart_hourly_calls_run_mart_hourly_demand(self):
        with patch("main.run_mart_hourly_demand") as m:
            run_stage("mart_hourly", YearMonth(2023, 1))
            m.assert_called_once_with()

    def test_mart_daily_calls_run_mart_daily_summary(self):
        with patch("main.run_mart_daily_summary") as m:
            run_stage("mart_daily", YearMonth(2023, 1))
            m.assert_called_once_with()

    def test_unknown_stage_raises(self):
        with pytest.raises(ValueError, match="Unknown stage"):
            run_stage("unknown", YearMonth(2023, 1))
