"""
Module: finaliser
Purpose: Drops the original columns, and renames the _cleaned columns
to the original names. The dataframe columns and schema columns
will now correspond

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-28"

from typing import Any
from pandas import DataFrame
from logging import Logger


def finalise(
    dataframes: list[str, DataFrame], context: dict[str, Any], logger: Logger
) -> None:
    logger.info("Finalising ETL pipeline")
    # # Drop original columns
    # df = df.drop(columns=original_columns)

    # # Rename cleaned columns back to original names
    # df = df.rename(columns={f"{col}_cleaned": col for col in original_columns})
