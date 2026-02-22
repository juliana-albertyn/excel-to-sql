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

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"


from typing import Any, Optional
import regex
import pandas as pd
from pandas import DataFrame
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype, is_string_dtype
from email_validator import validate_email, EmailNotValidError
from logging import Logger

import src.excel_to_sql.logging_setup as logging_setup


def validated_email(email: str, mandatory: bool = False) -> Optional[str]:
    """validate email addresses"""
    if not isinstance(email, str):
        email = str(email)
    email = email.strip()
    if email == "":
        if not mandatory:
            return email
        else:
            return None
    try:
        return validate_email(
            email, check_deliverability=False
        ).normalized  # returns normalized info if valid
    except EmailNotValidError:
        return None


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
    logger.info("Validating data for {table_name}")

    # set up
    PATTERNS = {
        "person_name": r"^\p{L}+(?:[ '-]\p{L}+)*$",
        "phone": r"\+\d{8,15}",
        "product_name": r"^[\p{L}\p{N}./()]+(?:[ '-][\p{L}\p{N}./()]+)*$",
    }
    # get validation rules
    validated_cols = []
    for col_name, config in validation_rules.items():
        # set vars
        cleaned_col = col_name + "_cleaned"
        validation = config.get("validation", "").lower()
        # if no validation continue to next column
        if len(validation) == 0:
            continue
        mask_invalid = None
        if is_string_dtype(df[col_name]):
            pattern = validation["format"]
            mask_invalid = ~df[cleaned_col].astype("string").str.fullmatch(pat=pattern)
        else:  # int | float | date | datetime
            mask_invalid = pd.Series(False, index=df.index)

            max_val = validation.get("max_value")
            min_val = validation.get("min_value")
            if is_datetime64_any_dtype(df[col_name]):
                if min_val == "today":
                    min_val = context["run_date"]
                if max_val == "today":
                    max_val = context["run_date"]

            if max_val is not None:
                mask_invalid |= df[col_name] > max_val

            if min_val is not None:
                mask_invalid |= df[col_name] < min_val

        if mask_invalid is not None:
            invalid_rows = df.loc[mask_invalid, cleaned_col]
            for idx, value in invalid_rows.items():
                logger.warning(f"Invalid value in {col_name} at row {idx}: {value}")

    # set up attributes up again
    file_name = df.file_name
    table_name = df.table_name

    # return dataframe
    return df
