"""
Module: validator
Purpose: Validates imported data

Responsibilities:
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


from typing import Any
import regex
import pandas
from pandas import DataFrame
import src.excel_to_sql.logging_setup as logging_setup


NAME_PATTERN = regex.compile(r"^\p{L}+([ '-]\p{L}+)*$")


def is_valid_name(name: str) -> bool:
    return bool(NAME_PATTERN.fullmatch(name))


def validate_data(
    df: DataFrame, validation_rules: dict[str, Any], locale: str, context: dict[str, Any]
) -> DataFrame:
    """ 
    Per-column and global validations
    """
    logger = logging_setup.get_logger(context, __name__)
    logger.info("Validating data")


    return df
