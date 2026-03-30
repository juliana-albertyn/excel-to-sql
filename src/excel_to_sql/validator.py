"""
Validation engine for the Fynbyte Excel-to-SQL pipeline.

Runs global and per-column checks on imported data before loading.
Covers required fields, primary keys, string rules, numeric and date
ranges, formats, and foreign key integrity. Collects all issues and
applies strict or permissive behaviour based on ETLContext settings.

Does not modify values. Only reports, drops (when allowed), or raises.

todo: composite primary key

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"


from typing import Any, Optional, Union
import regex
import pandas as pd
from pandas import DataFrame
from pandas.api.types import is_datetime64_any_dtype, is_string_dtype
from email_validator import validate_email, EmailNotValidError
from logging import Logger
from enum import IntEnum, auto
from dataclasses import dataclass, field
import datetime
import re

import src.excel_to_sql.logging_setup as logging_setup
import src.excel_to_sql.errors as errors
import src.excel_to_sql.context as context


class Severity(IntEnum):
    WARN = auto()
    FAIL = auto()
    DROP = auto()


patterns = {
    "person_name": regex.compile(r"^\p{L}+(?:[ '-]\p{L}+)*$"),
    # international phone number validation
    "E.164": regex.compile(r"\+\d{8,15}"),
    "product_name": regex.compile(r"^[\p{L}\p{N}./()+]+(?:[ '-][\p{L}\p{N}./()+]+)*$"),
    "email": regex.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"),
}

DateMinMaxType = Union[
    datetime.date, datetime.datetime, str, None
]  # str because 'today' can be used in the yaml
NumericMinMaxType = Union[int, float, None]


@dataclass
class ValidationIssue:
    """Data Class to describe a valdation issue"""

    column: str = ""
    rows: list[int] = field(default_factory=list)
    severity: Severity = Severity.FAIL
    msg: str = ""


class ValidationResult:
    """Class to store the validation results"""

    def __init__(self) -> None:
        """Initialise an instance of ValidationResult"""
        self.issues: list[ValidationIssue] = []

    def add_issue(
        self, column: str, rows: list[int], severity: Severity, msg: str
    ) -> None:
        """Add a ValidationIssue to the list of results"""
        issue = ValidationIssue(column=column, rows=rows, severity=severity, msg=msg)
        self.issues.append(issue)

    def has_severity(self, severity: Severity) -> bool:
        """Check if the ValidationResult has an issue with Severity"""
        return any(issue.severity == severity for issue in self.issues)

    def get_by_severity(self, severity: Severity) -> list[ValidationIssue]:
        """Get the issues with Severity"""
        return [issue for issue in self.issues if issue.severity == severity]

    def rows_to_drop(self) -> set[int]:
        """Return the rows for issues with Severity DROP"""
        return {
            row for r in self.issues if r.severity == Severity.DROP for row in r.rows
        }

    def fail_count(self) -> int:
        return len(self.get_by_severity(Severity.FAIL))

    def warn_count(self) -> int:
        return len(self.get_by_severity(Severity.WARN))

    def drop_count(self) -> int:
        """Return the number of rows for issues with Severity DROP"""
        return len(self.rows_to_drop())


def safe_validate_email(email, check_deliverability=False) -> bool:
    """
    Validate an email address without raising exceptions.
    Returns True for syntactically valid emails; otherwise False.
    """
    try:
        return (
            validate_email(email, check_deliverability=check_deliverability).normalized
            != ""
        )
    except (EmailNotValidError, TypeError):
        return False


def validate_str_len(value: Any, sql_type: str) -> bool:
    """
    Validate a string’s length against a SQL type definition.
    Returns True if the value fits the declared limit.
    """
    sql_type = sql_type.lower().strip()

    # Non-string SQL types → always valid
    if not sql_type.startswith(
        ("char", "varchar", "nchar", "nvarchar", "text", "ntext")
    ):
        return True

    # Text types → always valid
    if sql_type.startswith(("text", "ntext")):
        return True

    # Extract length from parentheses
    m = re.search(r"\((\d+)\)", sql_type)
    if m is None:
        # char/varchar/nchar/nvarchar MUST have a length
        return False

    limit = int(m.group(1))

    # Null values → treat as empty string
    if value is None:
        value_str = ""
    else:
        value_str = str(value)

    # Length rules
    if sql_type.startswith(("char", "varchar")):
        # byte length
        length = len(value_str.encode("utf-8"))
    else:
        # nchar/nvarchar → character length
        length = len(value_str)

    return length <= limit


def str_to_severity(severity: str) -> Severity:
    """
    Convert a string to a Severity enum.
    Defaults to FAIL for unknown values.
    """
    if severity == "drop":
        return Severity.DROP
    elif severity == "warn":
        return Severity.WARN
    else:
        return Severity.FAIL


def action_validations(
    results: ValidationResult,
    table_name: str,
    strict_validation: bool,
    df: pd.DataFrame | None,
    logger: Logger,
) -> pd.DataFrame | None:
    """
    Apply accumulated validation results.
    Logs warnings and errors, raises in strict mode, or drops rows in permissive mode.
    Returns the possibly modified DataFrame.
    """
    result = df
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

    if strict_validation:
        if results.has_severity(Severity.FAIL):
            # strict mode means any errors cause exception
            raise errors.ValidatorError(
                f"Data validation failed with {results.fail_count()} errors. See log file",
                errors.ErrorContext(
                    table_name=table_name,
                    details={
                        "fail_count": results.fail_count(),
                        "warn_count": results.warn_count(),
                    },
                ),
            )
    else:
        if df is not None and results.has_severity(Severity.DROP):
            # permissive mode - apply column error_on == drop
            rows_to_drop = results.rows_to_drop()
            if len(rows_to_drop) != 0:
                logger.warning(f"Dropped rows {rows_to_drop}")
                df = df.drop(index=list(rows_to_drop))
                result = df
    return result


def null_allowed_rule(series: pd.Series, allow_nulls: bool = True) -> pd.Series:
    """
    Return a boolean mask indicating which values satisfy the allow-null rule.
    Nulls fail when allow_nulls=False.
    """
    if allow_nulls:
        return pd.Series(True, index=series.index, dtype="boolean")

    # Nulls are not allowed → nulls fail
    mask = series.notna()
    return mask.astype("boolean")


def empty_string_allowed_rule(series: pd.Series, allow_empty: bool = True):
    """
    Return a boolean mask enforcing empty-string rules for string columns.
    Empty strings fail when allow_empty=False.
    """
    if allow_empty:
        return pd.Series(True, index=series.index, dtype="boolean")

    # Empty strings are not allowed and therefore fail
    mask = series.str.strip() != ""
    return mask.astype("boolean")


def str_length_rule(series: pd.Series, sql_type: str) -> pd.Series:
    """
    Validate string lengths for a Series using SQL type rules.
    Returns a boolean mask.
    """
    mask = (
        series.astype("string").str.strip().apply(validate_str_len, sql_type=sql_type)
    )
    return mask.astype("boolean")


def str_format_rule(
    series: pd.Series, pattern: Union[re.Pattern, regex.Pattern]
) -> pd.Series:
    """
    Validate string format using a compiled regex pattern.
    Returns a boolean mask.
    """
    mask = pd.Series(False, index=series.index)
    s = series.astype("string").astype(object)
    mask |= s.apply(lambda v: bool(pattern.fullmatch(v)) if pd.notna(v) else False)
    return mask.astype("boolean")


def email_rule(
    series: pd.Series,
    check_deliverability: bool,
    pattern: Union[re.Pattern, regex.Pattern],
    strict_validation: bool,
) -> pd.Series:
    """
    Validate email values using either strict email validation or pattern matching.
    Returns a boolean mask.
    """
    mask = pd.Series(False, index=series.index)

    series = series.astype("string").str.strip()

    if strict_validation or check_deliverability:
        mask |= series.apply(
            safe_validate_email, check_deliverability=check_deliverability
        )
        return mask
    else:
        return str_format_rule(series, pattern=pattern)


def normalise_date(
    value: Any, etl_context: context.ETLContext
) -> Optional[pd.Timestamp]:
    """
    Normalise a config-supplied date value to a date/datetime.
    Supports strings, “today”, and native date types.
    Raises ValidatorError for unsupported or invalid values.
    """
    if value is None:
        return None
    if isinstance(value, (datetime.date, datetime.datetime)):
        return pd.to_datetime(value)
    if isinstance(value, str):
        v = value.strip()
        if v == "":
            return None
        if v.lower() == "today":
            run_date = etl_context.run_date
            if run_date is None:
                raise errors.ValidatorError(
                    "Run date not supplied in etl_context", errors.ErrorContext()
                )
            else:
                return pd.to_datetime(run_date)
        try:
            return pd.to_datetime(v)
        except Exception as e:
            raise errors.ValidatorError(
                f"Invalid date in config: {value}",
                errors.ErrorContext(original_exception=e),
            )
    raise errors.ValidatorError(
        f"Unsupported date type: {type(value)}", errors.ErrorContext()
    )


def date_range_rule(
    series: pd.Series,
    min_date: DateMinMaxType,
    max_date: DateMinMaxType,
    etl_context: context.ETLContext,
) -> pd.Series:
    """
    Validate a Series of dates against optional min/max bounds.
    Unparsable values fail.
    Returns a boolean mask.
    """
    s = pd.to_datetime(series, errors="coerce")

    mindate = normalise_date(min_date, etl_context)
    maxdate = normalise_date(max_date, etl_context)

    mask = pd.Series(True, index=series.index, dtype="boolean")

    if mindate is not None:
        mask &= s >= mindate
    if maxdate is not None:
        mask &= s <= maxdate

    # any unparsable dates (NaT) should fail
    mask &= s.notna()

    return mask


def numeric_range_rule(
    series: pd.Series, min_val: NumericMinMaxType, max_val: NumericMinMaxType
) -> pd.Series:
    """
    Validate numeric values against optional min/max bounds.
    Unparsable values fail.
    Returns a boolean mask.
    """
    s = pd.to_numeric(series, errors="coerce")
    mask = pd.Series(True, index=series.index, dtype="boolean")

    if min_val is not None:
        mask &= s >= min_val
    if max_val is not None:
        mask &= s <= max_val

    # any unparsable dates (NaT) should fail
    mask &= s.notna()

    return mask


def _is_sql_numeric_type(datatype: str) -> bool:
    return datatype in [
        "tinyint",
        "smallint",
        "int",
        "bigint",
        "bit",
        "decimal",
        "numeric",
        "money",
        "smallmoney",
        "float",
        "real",
    ]


def _is_sql_date_time_type(datatype: str) -> bool:
    return datatype in ["date", "time", "datetime"]


def _is_sql_string_type(datatype: str) -> bool:
    return datatype.startswith(
        (
            "char",
            "varchar",
            "text",
            "nchar",
            "nvarchar",
            "ntext",
            "binary",
            "varbinary",
            "image",
        )
    )


def validate_data(
    df: DataFrame,
    table_name: str,
    columns_config: dict[str, Any],
    etl_context: context.ETLContext,
) -> pd.DataFrame:
    """
    Run all per-column validation rules for a table.
    Collects issues, applies strict/permissive behaviour, and returns the cleaned DataFrame.
    Assumes config and schema have already been validated.
    """
    # log start
    logger = logging_setup.get_logger(etl_context, __name__)
    logger.info(f"Validating data for *** {table_name} ***")
    cleaned_suffix = etl_context.cleaned_suffix

    # validation mode
    strict_validation = etl_context.strict_validation

    # set up
    results = ValidationResult()

    # get the row offset from the context
    row_offset = etl_context.row_offset

    # loop through columns
    for col_name, config in columns_config.items():
        # log
        logger.info(f"Validating column {col_name}")

        # get the validation configuration
        validation_config = config.get("validation", None)

        # if no validation continue to next column
        if validation_config is None:
            logger.info(f"No validation rules specified for column {col_name}")
            continue

        # set vars
        cleaned_col = f"{col_name}{cleaned_suffix}"
        on_error = str_to_severity(validation_config.get("on_error", "fail"))

        # override the schema setup for primary keys and non-nullable columns -
        # they always fail
        if config.get("primary_key", False) or not config.get("allow_null", True):
            on_error = Severity.FAIL

        # missing required or primary key columns
        if config.get("required", False) or config.get("primary_key", False):
            if col_name not in df.columns:
                results.add_issue(col_name, [], on_error, "Required column missing")
                continue

        # empty primary key columns or non-nullable columns
        if not config.get("allow_null", True) or config.get("primary_key", False):
            mask_missing = df[col_name].isna()

            # If string column, also check empty values
            if pd.api.types.is_string_dtype(df[col_name]):
                mask_missing |= df[col_name].astype("string").str.strip() == ""
            if mask_missing.any():
                rows = (df.index[mask_missing] + row_offset).tolist()
                results.add_issue(
                    col_name, rows, on_error, "Missing values for non-nullable column"
                )

        # primary key must be unique
        if config.get("primary_key", False):
            mask_duplicates = df[col_name].notna() & df[col_name].duplicated(keep=False)

            if mask_duplicates.any():
                duplicate_rows = (df.index[mask_duplicates] + row_offset).tolist()
                duplicate_values = df.loc[mask_duplicates, col_name].unique().tolist()

                results.add_issue(
                    col_name,
                    duplicate_rows,
                    on_error,
                    f"Primary key contains duplicate values: {duplicate_values}",
                )

        # check format of strings / min and max values of dates and numbers
        sql_type = config.get("data_type", None)
        if sql_type is None:
            # this error was already raised in pipeline.py
            raise errors.ValidatorError(
                f"Data type not specified for {col_name}",
                errors.ErrorContext(table_name=table_name, column_name=col_name),
            )

        if _is_sql_date_time_type(sql_type):  # date | datetime
            # check against min and max
            min_date = validation_config.get("min_value")
            max_date = validation_config.get("max_value")
            if min_date is not None or max_date is not None:
                mask_date = date_range_rule(
                    df[cleaned_col],
                    min_date=min_date,
                    max_date=max_date,
                    etl_context=etl_context,
                )
                mask_invalid_date = ~mask_date
                if mask_invalid_date.any():
                    rows = (df.index[mask_invalid_date] + row_offset).tolist()
                    results.add_issue(
                        col_name,
                        rows,
                        on_error,
                        f"Date out of range ({min_date} -> {max_date})",
                    )

        elif _is_sql_numeric_type(sql_type):
            max_val = validation_config.get("max_value", None)
            min_val = validation_config.get("min_value", None)
            if min_val is not None or max_val is not None:
                mask_invalid_numeric = numeric_range_rule(
                    df[cleaned_col], min_val=min_val, max_val=max_val
                )
                if mask_invalid_numeric.any():
                    rows = (df.index[mask_invalid_numeric] + row_offset).tolist()
                    results.add_issue(
                        col_name,
                        rows,
                        on_error,
                        f"Value out of range ({min_val} -> {max_val})",
                    )
        elif _is_sql_string_type(sql_type):
            # check string length
            # check if non-nullable or empty string
            mask_allow_null = null_allowed_rule(
                df[cleaned_col], config.get("allow_null", True)
            )
            mask_allow_empty = empty_string_allowed_rule(
                df[cleaned_col], config.get("allow_null", True)
            )
            mask_valid = mask_allow_null & mask_allow_empty
            mask_invalid_empty = ~mask_valid
            if mask_invalid_empty.any():
                rows = (df.index[mask_invalid_empty] + row_offset).tolist()
                results.add_issue(
                    col_name,
                    rows,
                    on_error,
                    "Empty string or null not allowed for non-nullable string column",
                )

            # check if string length valid
            if sql_type.startswith(("char", "varchar", "nchar", "nvarchar")):
                mask_valid_len = str_length_rule(df[cleaned_col], sql_type=sql_type)
                mask_invalid_len = ~mask_valid_len
                if mask_invalid_len.any():
                    rows = (df.index[mask_invalid_len] + row_offset).tolist()
                    results.add_issue(
                        col_name,
                        rows,
                        on_error,
                        "Length of value exceeds allowed length",
                    )

            # check string format
            if sql_type.startswith(
                ("char", "varchar", "nchar", "nvarchar", "text", "ntext")
            ):
                format = validation_config.get("format", None)
                if format is not None:
                    pattern = patterns[format]
                    # validate email
                    if format == "email":
                        mask_email = email_rule(
                            df[cleaned_col],
                            check_deliverability=config.get(
                                "check_deliverability", False
                            ),
                            strict_validation=strict_validation,
                            pattern=pattern,
                        )
                        mask_invalid_email = ~mask_email
                        if mask_invalid_email.any():
                            rows = (df.index[mask_invalid_email] + row_offset).tolist()
                            results.add_issue(
                                col_name,
                                rows,
                                on_error,
                                "Invalid email address",
                            )
                    elif pattern is not None:
                        mask_valid_format = str_format_rule(df[cleaned_col], pattern)
                        mask_invalid_format = ~mask_valid_format
                        if mask_invalid_format.any():
                            rows = (df.index[mask_invalid_format] + row_offset).tolist()
                            results.add_issue(
                                col_name,
                                rows,
                                on_error,
                                f"Format should be {format}",
                            )

    # action_validations might drop rows is the error handling is permissive. Handle that
    result = action_validations(
        results, table_name, strict_validation=strict_validation, df=df, logger=logger
    )

    # return dataframe
    if result is not None:
        df = result
    return df


def validate_foreign_keys(
    columns_config: dict[str, Any],
    tables: dict[str, pd.DataFrame],
    etl_context: context.ETLContext,
) -> None:
    """
    Validate foreign key relationships across all tables.
    Checks referenced tables/columns and ensures values exist in the target set.
    Raises or logs issues depending on validation mode.
    """
    # setup logger
    logger = logging_setup.get_logger(etl_context, "foreign_keys")
    logger.info("Validating foreign keys")

    cleaned_suffix = etl_context.cleaned_suffix
    row_offset = etl_context.row_offset
    strict_validation = etl_context.strict_validation

    # store results
    results = ValidationResult()
    # foreign key checks
    for table_name, cols_config in columns_config.items():
        # check foreign keys per table
        for col_name, col_config in cols_config.items():
            cleaned_col = f"{col_name}{cleaned_suffix}"
            # check columns for foreign keys
            validation_config = col_config.get("validation", None)
            on_error = Severity.FAIL
            if validation_config is not None:
                on_error = str_to_severity(validation_config.get("on_error", "fail"))
            foreign_key = col_config.get("foreign_key", None)

            if foreign_key is not None:
                # has the foreign key been specified correctly?
                if isinstance(foreign_key, str):
                    foreign_table, foreign_col = (
                        str(foreign_key).strip().lower().split(".")
                    )
                    cleaned_foreign_col = f"{foreign_col}{cleaned_suffix}"
                    # check that the values in the foreign key exist in the table/foreign column
                    if foreign_table not in tables:
                        results.add_issue(
                            col_name,
                            [],
                            on_error,
                            f"Foreign key table {foreign_table} does not exist",
                        )
                        continue
                    if foreign_col not in tables[foreign_table].columns:
                        results.add_issue(
                            col_name,
                            [],
                            on_error,
                            f"Foreign key column {foreign_col} does not exist in table {foreign_table}",
                        )
                        continue

                    valid_targets = set(
                        tables[foreign_table][cleaned_foreign_col].dropna()
                    )

                    col_raw = tables[table_name][cleaned_col]  # keep original dtype
                    col_str = col_raw.astype("string")  # safe for string ops

                    is_empty = col_str.str.strip() == ""

                    if col_config.get("allow_null", True):
                        mask_valid = (
                            col_raw.isna()  # TRUE NaN detection
                            | is_empty  # empty string treated as NULL
                            | col_raw.isin(valid_targets)
                        )
                    else:
                        mask_valid = col_raw.isin(valid_targets)

                    mask_invalid = ~mask_valid

                    if mask_invalid.any():
                        invalid_rows = (
                            tables[table_name].index[~mask_valid] + row_offset
                        ).tolist()
                        results.add_issue(
                            col_name,
                            invalid_rows,
                            on_error,
                            "Invalid foreign key values",
                        )
    action_validations(
        results,
        table_name="",
        strict_validation=strict_validation,
        df=None,
        logger=logger,
    )
