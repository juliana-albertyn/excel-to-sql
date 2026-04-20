"""
Module: test_validator
Purpose: Testing validator functions

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

import src.context as context
import src.logging_setup as logging_setup
from src.validator import (
    patterns,
    email_rule,
    null_allowed_rule,
    empty_string_allowed_rule,
    str_length_rule,
    str_format_rule,
    normalise_date,
    date_range_rule,
    numeric_range_rule,
)

etl_context: context.ETLContext


def setup_context() -> context.ETLContext:
    result = context.ETLContext(
        log_dir=Path("."),
        data_dir=Path("."),
        output_dir=Path("."),
        config_dir=Path("."),
    )
    result.strict_validation = False
    result.currency_symbol = "R"
    result.day_first_format = True
    result.row_offset = 2
    run_date = datetime.now()
    result.run_date = run_date.date()
    # all log files for the same run have the same timestamp
    result.run_timestamp = run_date.strftime("%Y%m%d_%H%M_%S")
    result.log_level = DEBUG
    logger = logging_setup.get_logger(result, __name__)

    result.datetime_parser = result.get_datetime_parser(logger)
    result.datetime_parser.debug_trace = True
    return result


def test_email_rule():
    series = pd.Series(
        [
            "abc_def@yahoo.com",
            "abc.def@gmail.com",
            "test.sample.com",
            " abc.def@hotmail.com ",
            "abc|def@yahoo.com",
            "  ",
            None,
        ],
        dtype="string",
    )

    result = email_rule(
        series,
        check_deliverability=True,
        pattern=patterns["email"],
        strict_validation=False,
    )

    expected = pd.Series(
        [
            True,
            True,
            False,
            True,
            True,
            False,
            False,
        ]
    )
    pd.testing.assert_series_equal(result, expected)


def test_null_allowed_rule_true():
    series = pd.Series([1, "abc", 2.0, None, ""])

    result = null_allowed_rule(series, allow_nulls=True)

    expected = pd.Series([True, True, True, True, True], dtype="boolean")

    pd.testing.assert_series_equal(result, expected)


def test_null_allowed_rule_false():
    series = pd.Series([1, "abc", 2.0, None, "  "])

    result = null_allowed_rule(series, allow_nulls=False)

    expected = pd.Series(
        [True, True, True, False, True], dtype="boolean"
    )  # empty string is not null

    pd.testing.assert_series_equal(result, expected)


def test_empty_string_allowed_rule_true():
    series = pd.Series(["1", "abc", 2.0, None, ""])

    result = empty_string_allowed_rule(series, allow_empty=True)

    expected = pd.Series([True, True, True, True, True], dtype="boolean")

    pd.testing.assert_series_equal(result, expected)


def test_empty_string_allowed_rule_false():
    series = pd.Series(["1", "abc", 2.0, None, "  "])

    result = empty_string_allowed_rule(series, allow_empty=False)

    expected = pd.Series(
        [True, True, True, True, False], dtype="boolean"
    )  # None is not an empty string

    pd.testing.assert_series_equal(result, expected)


def test_null_and_empty_string_allowed_rule_false():
    series = pd.Series(["1", "abc", 2.0, None, "  "])

    result_1 = null_allowed_rule(series, allow_nulls=False)
    result_2 = empty_string_allowed_rule(series, allow_empty=False)

    result = result_1 & result_2

    expected = pd.Series([True, True, True, False, False], dtype="boolean")

    pd.testing.assert_series_equal(result, expected)


@pytest.mark.parametrize(
    "dtype, expected",
    [
        ("varchar(4)", [False, False, True, True, True]),
        ("varchar(6)", [True, True, True, True, True]),
        ("char", [False, False, False, False, False]),
        ("int64", [True, True, True, True, True]),
    ],
)
def test_str_length_rule(dtype, expected):
    series = pd.Series(["abcde", "12345", "defg", "", None], dtype="string")

    result = str_length_rule(series, dtype)
    expected_series = pd.Series(expected, dtype="boolean")

    pd.testing.assert_series_equal(result, expected_series)


@pytest.mark.parametrize(
    "format, expected",
    [
        ("person_name", [True, False, False, False, False, False, False]),
        ("product_name", [True, True, True, False, False, False, False]),
        ("E.164", [False, False, True, False, False, False, False]),
    ],
)
def test_str_format_rule(format, expected):
    series = pd.Series(
        [
            "Peter Smith",
            "Product Version 4.1+",
            "+271234567890",
            "@#$@#",
            "",
            None,
            "abc@yahoo.com",
        ],
        dtype="string",
    )

    result = str_format_rule(series, patterns[format])

    expected_series = pd.Series(expected, dtype="boolean")

    pd.testing.assert_series_equal(result, expected_series)


def test_normalise_date_today():
    value = "today"

    dt = datetime.now().date()
    etl_context.run_date = dt

    normalised = normalise_date(value, etl_context=etl_context)

    assert normalised == pd.Timestamp(dt)


@pytest.fixture
def runtime_context() -> context.ETLContext:
    return etl_context


@pytest.mark.parametrize("allow_null", [True, False])
@pytest.mark.parametrize(
    "min_date, max_date, expected",
    [
        (datetime(1900, 1, 1), "today", [True, False, False]),
        (datetime(1900, 1, 1), None, [True, False, True]),
        (datetime(1900, 1, 1), datetime(2026, 3, 25), [True, False, False]),
        (datetime(2026, 1, 1), datetime(2026, 1, 31), [True, False, False]),
        (None, None, [True, True, True]),
        (None, "today", [True, True, False]),
    ],
)
def test_date_range_rule(allow_null, min_date, max_date, expected, runtime_context):
    series = pd.Series(["2026-01-01", "1860-01-01", "2026-12-12"])

    result = date_range_rule(
        series,
        allow_null=allow_null,
        min_date=min_date,
        max_date=max_date,
        etl_context=runtime_context,
    )

    expected_series = pd.Series(expected, dtype="boolean")

    pd.testing.assert_series_equal(result, expected_series)


@pytest.mark.parametrize("allow_null", [True, False])
@pytest.mark.parametrize(
    "min_value, max_value, expected",
    [
        (10, 20, [False, False, False, False, False, False]),
        (0, 100, [False, True, True, True, True, True]),
        (None, None, [True, True, True, True, True, True]),
        (55.3, 55.4, [False, False, False, True, False, False]),
    ],
)
def test_numeric_range_rule(allow_null, min_value, max_value, expected):
    series = pd.Series([-10, 1, 44, 55.3, 60, "100"])

    result = numeric_range_rule(
        series, allow_null=allow_null, min_val=min_value, max_val=max_value
    )

    # adjust expected for allow_null=False: None values in series become False
    if not allow_null:
        # Assuming the second value in series could be None in some real data,
        # here we simulate that any originally True because of null should now be False
        expected = [val if val is not None else False for val in expected]

    expected_series = pd.Series(expected, dtype="boolean")

    pd.testing.assert_series_equal(result, expected_series)


etl_context = setup_context()
