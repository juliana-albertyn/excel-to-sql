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

class ValidationMode(IntEnum):
    STRICT = auto()
    PERMISSIVE = auto()


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

def str_to_severity(severity : str) -> Severity:
    if severity == "drop":
        return Severity.DROP
    elif severity == "warn":
        return Severity.WARN
    else:
        return Severity.FAIL
    
def str_to_validation_mode(validation_mode: str) -> ValidationMode:
    if validation_mode.strip().lower() == "strict":
        return ValidationMode.STRICT
    else:
        return ValidationMode.PERMISSIVE    

def action_validations(results : ValidationResult, validation_mode: ValidationMode, df: pd.DataFrame | None, logger: Logger) -> None:
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

    if validation_mode == ValidationMode.STRICT and results.has_severity(
        Severity.FAIL
    ):
        # strict mode means any errors cause exception
        raise ValueError(f"ETL pipeline validation failed")
    if df is not None and results.has_severity(Severity.DROP):
        # permissive mode - apply column error_on == drop
        if results.rows_to_drop():
            logger.warning(f"Dropped rows {results.rows_to_drop}")
            df = df.drop(index=results.rows_to_drop)

def validate_data(
    df: DataFrame,
    columns_config: dict[str, Any],
    context: dict[str, Any],
) -> DataFrame:
    """Global and per-column validations"""
    # save attributes
    file_name = df.file_name
    table_name = df.table_name

    # log start
    logger = logging_setup.get_logger(context, __name__)
    logger.info(f"Validating data for *** {table_name} ***")

    # validation mode
    validation_mode = str_to_validation_mode(context["validation_mode"])
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

    # loop through columns
    for col_name, config in columns_config.items():
        # get the validation configuration
        validation_config = config.get("validation", None)

        # if no validation continue to next column
        if validation_config is None:
            logger.info(f"No validation rules specified for column {col_name}")
            continue

        # set vars
        cleaned_col = col_name + "_cleaned"
        on_error = str_to_severity(validation_config.get("on_error", "fail"))

        # keep list of columns validated
        validated_cols.append(col_name)

        # missing required or primary key columns
        mask_missing = None
        if (
            config.get("required", False)
            or config.get("primary_key", False)
        ):
            if col_name not in df.columns:
                results.add_issue(col_name, [], on_error, "Required column missing")
                continue

        # empty primary key columns or non-nullable columns
        if (
            validation_config.get("nullable", True)
            or config.get("primary_key", False)
        ):
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
            sql_type = config.get("data_type", None)
            if sql_type is not None:
                sql_type = sql_type.strip().aslower()
                mask_invalid = (
                    ~df[cleaned_col]
                    .astype("string")
                    .apply(
                        validate_column_len,
                        sql_type=sql_type,
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
            pattern = validation_config.get("format", None)
            if pattern is not None:
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
            max_val = validation_config.get("max_value", None)
            min_val = validation_config.get("min_value", None)
            if is_datetime64_any_dtype(df[col_name]):
                if min_val is not None and min_val == "today":
                    min_val = context["run_date"]
                if  max_val is not None and max_val == "today":
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

        action_validations(results, validation_mode, df, logger)

    # set up attributes up again
    df.file_name = file_name
    df.table_name = table_name

    # return dataframe
    return df

def validate_foreign_keys(columns_config: dict[str, Any], tables: dict[str, pd.DataFrame], context: dict[str, Any]) -> None:
    # setup logger
    logger = logging_setup.get_logger(context, __name__ + "_foreign")
    logger.info(f"Validating foreign keys")

    # store results
    results = ValidationResult()
    # foreign key checks
    for table_name, cols_config in columns_config.items():
        # find configuration per table
        if cols_config is None:
            logger.warning(f"No columns set up for {table_name}")
            continue

        # check foreign keys per table    
        for col_name, col_config in cols_config.items():
            # check columns for foreign keys, and check    
            validation_config = col_config.get("validation", None)
            on_error = str_to_severity(validation_config.get("on_error", "fail"))
            if validation_config is not None:
                foreign_key = validation_config.get("foreign_key", None)

                if foreign_key is not None:
                    # has the foreign key been specified correctly?
                    if isinstance(foreign_key, str):
                        foreign_table, foreign_col = str(foreign_key).strip().lower().split(".")
                        if not foreign_table in columns_config.keys():
                            results.add_issue(col_name, [], on_error, f"Foreign key table {foreign_table} not found")
                            continue
                        if not foreign_col in columns_config[foreign_table]:
                            results.add_issue(col_name, [], on_error, f"Foreign key column {foreign_col} not found")
                            continue
                        # check that the values in the foreign key exist in the table/foreign column
                        valid_targets = tables[foreign_table][foreign_col].dropna().unique()
                        mask_valid = tables[table_name][col_name].isin(valid_targets)
                        invalid_rows = tables[table_name][~mask_valid]
                        results.add_issue(col_name, invalid_rows, on_error, f"Invalid foreign key values")
                    else:
                        results.add_issue(col_name, [], on_error, f"Foreign key {foreign_key} must be in format table_name.column_name")
                break
            else:
                logger.info(f"No validation set up for {table_name}")
                continue # to the next column
    action_validations(results, ValidationMode.STRICT, None, logger)
