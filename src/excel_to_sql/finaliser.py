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
import pandas as pd
from pandas import DataFrame
from logging import Logger


def finalise(
    dataframes: dict[str, DataFrame], context: dict[str, Any], logger: Logger
) -> dict[str, DataFrame]:
    logger.info("Finalising ETL pipeline")
    cleaned_suffix = context.get("cleaned_suffix")
    if cleaned_suffix is not None:
        for df in dataframes.values():
            # Drop original columns
            original_cols = [c for c in df.columns if not c.endswith(cleaned_suffix)]
            df = df.drop(columns=original_cols)

            # Rename cleaned columns back to original names
            df = df.rename(
                columns={f"{col}{cleaned_suffix}": col for col in original_cols}
            )
    return dataframes
