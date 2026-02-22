"""
Module: validator
Purpose: Validates imported data

Responsibilities:
Check primary key is not null
Check primary key is unique
Required columns exist
Data types
Length constraints
Nullable rules
Value ranges
Validdate datetime
Check allowed range
Validator does NOT fix.

todo: composite primary key

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"


from typing import Any
import pandas as pd
from pandas import DataFrame
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype, is_string_dtype
from email_validator import validate_email, EmailNotValidError
from logging import Logger
from enum import IntEnum, auto
from dataclasses import dataclass
import re

import src.excel_to_sql.logging_setup as logging_setup


class Severity(IntEnum):
    WARN = auto()
    FAIL = auto()
    DROP = auto()


@dataclass
class ValidationIssue:
    column: str
    rows: list[int]
    severity: Severity
    msg: str


class ValidationResult:
    def __init__(self) -> None:
        self.issues: list[ValidationIssue] = []

    def add_issue(
        self, column: str, rows: list[int], severity: Severity, msg: str
    ) -> None:
        issue = ValidationIssue(column=column, rows=rows, severity=severity, msg=msg)
        self.issues.append(issue)

    def has_severity(self, severity: Severity) -> bool:
        return any(issue.severity == severity for issue in self.issues)

    def get_by_severity(self, severity: Severity) -> list[ValidationIssue]:
        return [issue for issue in self.issues if issue.severity == severity]

    def rows_to_drop(self) -> set[int]:
        return {
            row for r in self.issues if r.severity == Severity.DROP for row in r.rows
        }


def safe_validate_email(email, check_deliverability=False) -> bool:
    try:
        return (
            validate_email(email, check_deliverability=check_deliverability).normalized
            != ""
        )
    except EmailNotValidError:
        return False


def validate_column_len(value_str: str, sql_type: str) -> bool:
    # prepare
    sql_type = sql_type.strip().lower()

    # search for the brackets
    m = re.search(r"\((\d+)\)", sql_type)

    # might be text, no length specified
    if m is None:
        return not sql_type.startswith(("char", "varchar", "nchar", "nvarchar"))

    # get the max length
    limit = int(m.group(1))

    # check the lengths of the value
    lengths = 0
    if sql_type.startswith(("char", "varchar")):
        # byte length
        lengths = len(value_str.encode("utf-8"))
    elif sql_type.startswith(("nchar", "nvarchar")):
        # character length
        lengths = len(value_str)

    return lengths <= limit


def validate_data(
    df: DataFrame,
    validation_rules: dict[str, Any],
    locale: str,
    context: dict[str, Any],
) -> DataFrame:
    """Global and per-column validations"""
    # save attributes
    file_name = df.file_name
    table_name = df.table_name

    # log start
    logger = logging_setup.get_logger(context, __name__)
    logger.info("Validating data for *** {table_name} ***")

    # set up
    results = ValidationResult()
    PATTERNS = {
        "person_name": r"^\p{L}+(?:[ '-]\p{L}+)*$",
        "phone": r"\+\d{8,15}",
        "product_name": r"^[\p{L}\p{N}./()]+(?:[ '-][\p{L}\p{N}./()]+)*$",
        "email_address": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
    }

    # get validation rules
    validated_cols = []
    for col_name, config in validation_rules.items():
        # set vars
        cleaned_col = col_name + "_cleaned"
        validation = config.get("validation", "").lower()
        on_error_str = config.get("on_error", "fail")
        if on_error_str == "drop":
            on_error = Severity.DROP
        elif on_error_str == "warn":
            on_error = Severity.WARN
        else:
            on_error = Severity.FAIL

        # if no validation continue to next column
        if len(validation) == 0:
            continue

        # keep list of columns validated
        validated_cols.append(col_name)

        # missing required or primary key columns
        mask_missing = None
        if (
            validation.get("required", False)
            or config.get("primary_key", False)
            or config.get("foreign_key", False)
        ):
            if col_name not in df.columns:
                results.add_issue(col_name, [], on_error, "Required column missing")

            mask_missing = df[col_name].isna()

            # If string column, also check empty values
            if pd.api.types.is_string_dtype(df[col_name]):
                mask_missing |= df[col_name].astype("string").str.strip() == ""

            if mask_missing.any():
                rows = df.index[mask_missing].tolist()
                results.add_issue(
                    col_name, rows, on_error, "Missing values for required column"
                )

        # primary key must be unique
        if config.get("primary_key", False):
            mask_duplicates = df[col_name].duplicated(keep=False)

            if mask_duplicates.any():
                duplicate_rows = df.index[mask_duplicates].tolist()
                duplicate_values = df.loc[mask_duplicates, col_name].unique().tolist()

                results.add_issue(
                    col_name,
                    duplicate_rows,
                    on_error,
                    f"Primary key contains duplicate values: {duplicate_values}",
                )

        # check format of strings / min and max values of dates and numbers
        mask_invalid = None
        validation_format = ""
        if is_string_dtype(df[col_name]):
            # check string length
            mask_invalid = (
                ~df[cleaned_col]
                .astype("string")
                .apply(
                    validate_column_len,
                    sql_type=validation_rules["data_type"].strip().aslower(),
                )
            )
            if mask_invalid is not None:
                invalid_rows = df.loc[mask_invalid, cleaned_col]
                for row in invalid_rows.index:
                    results.add_issue(
                        col_name,
                        [row],
                        on_error,
                        f"Exceeds length: {invalid_rows[row]}",
                    )

            # check string format
            pattern = validation["format"]
            validation_format = f"Invalid {pattern}"
            if pattern == "email" and config.get("check_deliverability", False):
                mask_invalid = (
                    ~df[cleaned_col]
                    .astype("string")
                    .apply(safe_validate_email, check_deliverability=True)
                )
            else:
                mask_invalid = (
                    ~df[cleaned_col].astype("string").str.fullmatch(pat=pattern)
                )
        else:  # int | float | date | datetime
            mask_invalid = pd.Series(False, index=df.index)
            # check against min and max
            max_val = validation.get("max_value")
            min_val = validation.get("min_value")
            if is_datetime64_any_dtype(df[col_name]):
                if min_val == "today":
                    min_val = context["run_date"]
                if max_val == "today":
                    max_val = context["run_date"]

            if max_val is not None:
                mask_invalid |= df[cleaned_col] > max_val
                validation_format = f"max={max_val}"

            if min_val is not None:
                mask_invalid |= df[cleaned_col] < min_val
                validation_format = f"min={min_val} " + validation_format

        if mask_invalid is not None:
            invalid_rows = df.loc[mask_invalid, cleaned_col]
            for row in invalid_rows.index:
                results.add_issue(
                    col_name,
                    [row],
                    on_error,
                    f"{validation_format}: {invalid_rows[row]}",
                )

        # log all columns validated
        logger.info(f"Validated columns on {table_name}: {validated_cols}")
        # first list all fails and warning, then apply validation_mode and column on_error
        if results.has_severity(Severity.FAIL):
            # log fails
            for issue in results.get_by_severity(Severity.FAIL):
                issue_str = f"col={issue.column}, rows={issue.rows}, msg={issue.msg}"
                logger.error(issue_str)

        if results.has_severity(Severity.WARN):
            # log warnings
            for issue in results.get_by_severity(Severity.WARN):
                issue_str = f"col={issue.column}, rows={issue.rows}, msg={issue.msg}"
                logger.warning(issue_str)

        if context["validation_mode"] == "strict" and results.has_severity(
            Severity.FAIL
        ):
            # strict mode means any errors cause exception
            raise ValueError(f"ETL pipeline validation failed for {table_name}")
        if on_error == Severity.DROP:
            # permissive mode - apply column error_on == drop
            if results.rows_to_drop():
                logger.warning(f"Dropped rows {results.rows_to_drop} from {table_name}")
                df = df.drop(index=results.rows_to_drop)

    # set up attributes up again
    file_name = df.file_name
    table_name = df.table_name

    # return dataframe
    return df
