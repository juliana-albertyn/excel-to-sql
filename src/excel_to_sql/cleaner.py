"""
Module: cleaner
Purpose: Clean data imported into pandas from excel.

Responsibilities:
Drop empty rows
Trim whitespace
Replace NaN
Normalize case
Strip special characters
Standardize formats
Normalize date formats
Convert strings to datetime
Remove obvious junk
Apply locale rules
Standardize timezone if needed

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-08"

from typing import Any, Optional
import pandas as pd
from pandas import DataFrame
from datetime import date, datetime
from logging import Logger

import src.excel_to_sql.logging_setup as logging_setup


def str_to_bool(value: str | bool) -> bool:
    """
    Convert common truthy strings to a boolean.
    Accepts 'true', 'yes', '1' (case-insensitive).
    Returns True for those, False otherwise.
    """
    if isinstance(value, str):
        return value.strip().lower() in ("true", "yes", "1")
    return bool(value)  # fallback for non-strings


def str_to_int(value: str | int) -> int:
    "if int, return as-is, else if a str, convert to an int"
    if isinstance(value, int):
        return value
    elif isinstance(value, str) and value.isnumeric():
        return int(value)
    else:
        return 0


def str_to_float(value: str | float) -> float:
    "if float, return as-is, else if a str, convert to an float"
    if isinstance(value, float):
        return value
    elif isinstance(value, str) and value.isnumeric():
        return float(value)
    else:
        return 0.0


def str_to_iso_date(value: str | date, day_first_format: bool) -> Optional[date]:
    "if date, return as-is, else if a str, convert to a date in iso format YYYY-MM-DD"
    if isinstance(value, date):
        return value
    elif isinstance(value, str):
        value = value.strip()
        format_list = [
            "%d-%b-%Y",
            "%d %b %Y",
            "%b %d, %Y",
            "%d-%B-%Y",
            "%d %B %Y",
            "%Y%m%d",
            "%Y/%m/%d",
            "%B %d, %Y",
        ]
        if day_first_format:
            format_list.extend(["%d%m%Y", "%d/%m/%Y"])
        else:
            format_list.extend(["%m%d%Y", "%m/%d/%Y"])

        for fmt in format_list:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return
    else:
        return


def trim_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """trim whitespace"""
    for col in df.select_dtypes(include="object"):
        df[col] = df[col].str.strip()
    return df


def standardise_nulls(df: pd.DataFrame) -> pd.DataFrame:
    "standardise nulls to pd.NA"
    return df.apply(
        lambda col: (
            col.replace(r"^(NA|N/A|NULL|None)$", pd.NA, regex=True)
            if col.dtype in ["object", "string"]
            else col
        )
    )


def remove_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove empty rows"""
    return df.dropna(how="all")


def log_column_changes(
    df: pd.DataFrame, table_name, context: dict[str, Any], logger: Logger
) -> None:
    """log only changed values per table"""
    logger.info(f"Audit of cleaned values for *** {table_name} ***:")
    cleaned_suffix = context.get("cleaned_suffix", "")
    # get the row offset from the context
    row_offset = context.get("row_offset", 1)

    for col in df.columns:
        if col.endswith("_cleaned"):
            original_col = col.replace(cleaned_suffix, "")

            left_num = pd.to_numeric(df[original_col], errors="coerce")
            right_num = pd.to_numeric(df[col], errors="coerce")

            numeric_diff = left_num.ne(right_num)

            string_diff = df[original_col].astype("string").ne(df[col].astype("string"))

            mask = numeric_diff & string_diff

            if not mask.any():
                logger.info(f"No cleaned values for column {original_col}")
            else:
                for idx in df.index[mask]:
                    logger.info(
                        f"{original_col} | Row {idx + row_offset} | "
                        f"{df.at[idx, original_col]} -> {df.at[idx, col]}"
                    )


def clean_data(
    df: DataFrame,
    cleaning_rules: dict[str, Any],
    table_name: str,
    column_config: dict[str, Any],
    context: dict[str, Any],
) -> pd.DataFrame:
    """Clean data using cleaning rules from yaml."""
    # do automatic convert of data types e.g. object -> str
    df = df.convert_dtypes()

    # log before info
    logger = logging_setup.get_logger(context, __name__)
    logger.info(f"Cleaning data for *** {table_name} ***. Shape: {df.shape}")
    cleaned_suffix = context.get("cleaned_suffix", "")

    # global cleaning rules
    if cleaning_rules.get("trim_whitespace", False):
        df = trim_whitespace(df)
        logger.info(f"Trimmed whitespace for *** {table_name} ***")

    if cleaning_rules.get("standardise_nulls", False):
        df = standardise_nulls(df)
        logger.info(f"Standardised nulls for *** {table_name} ***")

    if cleaning_rules.get("remove_blank_rows", False):
        empty_rows, _ = df[df.isna().all(axis=1)].shape
        if empty_rows > 0:
            df = remove_empty_rows(df)
            logger.info(f"Removed {empty_rows} empty rows for *** {table_name} ***")

    currency_symbol = cleaning_rules.get("currency_symbol", "")
    day_first_format = cleaning_rules.get("day_first_format", True)

    # configuration for all columns in a table/worksheet are passed in
    for col_name, col_config in column_config.items():
        cleaned_col = f"{col_name}{cleaned_suffix}"
        logger.info(f"Cleaning column: {col_name}->{cleaned_col}")
        # data type - required
        dtype = col_config.get("data_type", "")
        dtype = dtype.split("(", 1)[0].strip()  # cater for varchar(10)
        if dtype == "":
            logger.error(f"Data type not specified for {col_name}")
            continue
        # data types and formats
        if dtype in ["char", "varchar", "text", "nchar", "nvarchar", "ntext"]:
            # string case
            str_case = col_config.get("case", "asis").lower()
            if str_case == "lower":
                df[cleaned_col] = df[col_name].astype(str).str.strip().str.lower()
            elif str_case == "upper":
                df[cleaned_col] = df[col_name].astype(str).str.strip().str.upper()
            elif str_case == "title":
                df[cleaned_col] = df[col_name].astype(str).str.strip().str.title()
            else:
                df[cleaned_col] = df[col_name]
            # get the column values
            series = df[cleaned_col]
            # Mask empty strings and "nan" strings
            mask = (series == "") | (series.str.lower() == "nan")
            series = series.mask(mask, pd.NA)
            # make all non-empty strings string type. Empty are NaN
            df[cleaned_col] = series.astype("string")
        elif dtype == "bit":
            df[cleaned_col] = df[col_name].apply(str_to_bool)
        elif dtype in ["date", "time", "datetime"]:
            # date format - standard formats
            df[cleaned_col] = pd.to_datetime(
                df[col_name], errors="coerce", format="%Y-%m-%d"
            )
            mask = df[cleaned_col].isna()
            if day_first_format:
                df.loc[mask, cleaned_col] = pd.to_datetime(
                    df[col_name], errors="coerce", format="%d-%m-%Y"
                )
            else:
                df.loc[mask, cleaned_col] = pd.to_datetime(
                    df[col_name], errors="coerce", format="%m-%d-%Y"
                )
            # Use helper function looking for several other formats
            mask = df[cleaned_col].isna()
            df.loc[mask, cleaned_col] = df.loc[mask, col_name].apply(
                str_to_iso_date, day_first_format=day_first_format
            )
        else:
            df[cleaned_col] = df[col_name]
            if dtype in ["smallmoney", "money"]:
                # currency
                df[cleaned_col] = df[cleaned_col].astype(str).str.replace(
                    currency_symbol, "", regex=False
                ).str.strip()
            df[cleaned_col] = pd.to_numeric(df[cleaned_col], errors="coerce")

    # log only cleaned columns
    log_column_changes(df, table_name, context, logger)

    return df
