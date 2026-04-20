"""
Module: test_cleaner
Purpose: Testing cleaner

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-03-29"


import pandas as pd
import pytest
from datetime import datetime
from pathlib import Path
from logging import DEBUG

import src.errors as errors
import src.context as context
import src.logging_setup as logging_setup
from src.cleaner import (
    str_to_bool,
    col_to_date,
    col_to_datetime,
    col_to_time,
    col_to_numeric,
    col_to_decimal_float_real,
    col_to_int,
    col_to_money,
)


def setup_context() -> context.ETLContext:
    etl_context = context.ETLContext(
        log_dir=Path("."),
        data_dir=Path("."),
        output_dir=Path("."),
        config_dir=Path("."),
    )
    etl_context.strict_validation = False
    etl_context.currency_symbol = "R"
    etl_context.day_first_format = True
    etl_context.row_offset = 2
    run_date = datetime.now()
    etl_context.run_date = run_date.date()
    # all log files for the same run have the same timestamp
    etl_context.run_timestamp = run_date.strftime("%Y%m%d_%H%M_%S")
    etl_context.log_level = DEBUG
    logger = logging_setup.get_logger(etl_context, __name__)

    etl_context.datetime_parser = etl_context.get_datetime_parser(logger)
    etl_context.datetime_parser.debug_trace = True
    return etl_context


def test_str_to_bool_true_values():
    assert str_to_bool("true") is True
    assert str_to_bool("YES") is True
    assert str_to_bool("1") is True


def test_str_to_bool_false_values():
    assert str_to_bool("false") is False
    assert str_to_bool("no") is False
    assert str_to_bool("") is False


def test_str_to_bool_non_string():
    assert str_to_bool(True) is True
    assert str_to_bool(False) is False


def test_col_to_date():
    series = pd.Series(
        [
            "Jan 13, 2026",
            "Jan 13, 2026",
            "13-Jan-2026",
            "13/01/2026",
            "13-01-2026",
            "2026-01-13 00:00",
            "2026-01-13 00:00:00",
            "31/02/2026",
            "apple",
            " ",
            None,
        ],
        dtype="string",
    )

    result = col_to_date(
        series,
        table_name="test",
        col_name="test",
        etl_context=etl_context,
    )

    expected = pd.Series(
        [
            "2026-01-13",
            "2026-01-13",
            "2026-01-13",
            "2026-01-13",
            "2026-01-13",
            "2026-01-13",
            "2026-01-13",
            None,
            None,
            None,
            None,
        ]
    )
    expected = pd.to_datetime(expected)

    print(expected)

    pd.testing.assert_series_equal(result, expected)


def test_col_to_date_strict_row_number():
    series = pd.Series(["13-Jan-2026", "31/02/2026"], dtype="string")
    strict_context = etl_context
    strict_context.strict_validation = True

    with pytest.raises(errors.CleanerError) as exc_info:
        col_to_date(
            series, table_name="test", col_name="test", etl_context=strict_context
        )

    etl_context.strict_validation = False

    error = exc_info.value

    # row index = 0, offset = 2 → expected row = 3
    assert "row 2" in str(error)


def test_col_to_time():
    series = pd.Series(
        [
            "9:45",
            "09:45",
            "09:45pm",
            "9:45 PM",
            "21:45",
            "09:45:44",
            "21:45",
            "09:45:99",
            "25:25",
            "",
            None,
            "banana",
        ],
        dtype="string",
    )
    result = col_to_time(
        series, table_name="test", col_name="test", etl_context=etl_context
    )

    expected = pd.Series(
        [
            datetime(1900, 1, 1, 9, 45),
            datetime(1900, 1, 1, 9, 45),
            datetime(1900, 1, 1, 21, 45),
            datetime(1900, 1, 1, 21, 45),
            datetime(1900, 1, 1, 21, 45),
            datetime(1900, 1, 1, 9, 45, 44),
            datetime(1900, 1, 1, 21, 45),
            None,
            None,
            None,
            None,
            None,
        ]
    )
    expected = pd.to_datetime(expected, errors="coerce")

    pd.testing.assert_series_equal(result, expected)


def test_col_to_datetime():
    series = pd.Series(
        [
            "1 Jan 2026",
            "1 Jan 2026",
            "2026-01-01 00:00",
            "2026-01-01",
            "2026-01-01 9:45",
            "1 Jan 2026 09:45",
            "1-1-2026 09:45pm",
            "2026-28-02 9:45 PM",
            "2026-01-01 21:45",
            "2026-01-01 09:45:44",
            "2026-01-01 09:45:99",
            "2026-01-01 25:25",
            "",
            None,
            "banana",
        ],
        dtype="string",
    )
    result = col_to_datetime(
        series, table_name="test", col_name="test", etl_context=etl_context
    )

    expected = pd.Series(
        [
            datetime(2026, 1, 1, 0, 0),
            datetime(2026, 1, 1, 0, 0),
            datetime(2026, 1, 1, 0, 0),
            datetime(2026, 1, 1, 0, 0),
            datetime(2026, 1, 1, 9, 45),
            datetime(2026, 1, 1, 9, 45),
            datetime(2026, 1, 1, 21, 45),
            datetime(2026, 2, 28, 21, 45),
            datetime(2026, 1, 1, 21, 45),
            datetime(2026, 1, 1, 9, 45, 44),
            None,
            None,
            None,
            None,
            None,
        ]
    )
    expected = pd.to_datetime(expected, errors="coerce")

    pd.testing.assert_series_equal(result, expected)


def test_col_to_money():
    series = pd.Series(
        ["x", 1, "2.0", "3,0", "4:44", "R5", "ZAR6", "R 7", "banana", None],
        dtype="string",
    )

    result = col_to_money(
        series, table_name="test", col_name="test", etl_context=etl_context
    )

    expected = pd.Series(
        [None, 1.00, 2.00, None, None, 5.00, None, 7.00, None, None], dtype="Float64"
    )

    pd.testing.assert_series_equal(result, expected)


def test_col_to_numeric():
    series = pd.Series(
        ["x", 1.0, "2", "3,0", "4:44", "R5", "ZAR6", "R 7", "banana", None],
        dtype="string",
    )

    result = col_to_numeric(
        series, table_name="test", col_name="test", etl_context=etl_context
    )

    expected = pd.Series(
        [None, 1, 2, None, None, None, None, None, None, None], dtype="Float64"
    )

    pd.testing.assert_series_equal(result, expected)


def test_col_to_int():
    series = pd.Series(
        ["x", 1, "2", "3,0", "4:44", "R5", "ZAR6", "R 7", "banana", None],
        dtype="string",
    )

    result = col_to_int(
        series, table_name="test", col_name="test", etl_context=etl_context
    )

    expected = pd.Series(
        [None, 1, 2, None, None, None, None, None, None, None], dtype="Int64"
    )

    pd.testing.assert_series_equal(result, expected)


def test_col_to_decimal_float_real():
    series = pd.Series(
        ["x", 1, "2.0", "3.55", "4:44", "R5", "ZAR6", "R 7", "banana", None],
        dtype="string",
    )

    result = col_to_decimal_float_real(
        series, table_name="test", col_name="test", etl_context=etl_context
    )

    expected = pd.Series(
        [None, 1.0, 2.0, 3.55, None, None, None, None, None, None], dtype="Float64"
    )

    pd.testing.assert_series_equal(result, expected)


etl_context = setup_context()
