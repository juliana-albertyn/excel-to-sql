"""
Module: transformer
Purpose: Short Transform the data loaded into dataframe from excel

Responsibilities:
Rename columns (e.g., Excel Cust Name → customer_name)
Compute derived columns (e.g., total_price = quantity * unit_price)
Reorder columns if needed
Possibly typecast to the target datatype
It does not check values against regex, null rules, or foreign keys — that’s the job of validation.

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"

from typing import Any
import pandas
from pandas import DataFrame
import src.excel_to_sql.logging_setup as logging_setup


def apply_mappings(
    df: DataFrame, mappings: dict[str, Any], context: dict[str, Any]
) -> DataFrame:
    """Use mapping configuration to transform the data"""

    logger = logging_setup.get_logger(context, __name__)
    logger.info("Transforming data")

    return df
