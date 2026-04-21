"""
Finalisation step for the Fynbyte Exce-to-SQL pipeline.

Drops original source columns and renames cleaned columns back to their
schema names so the final DataFrames match the expected SQL structure.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-28"

from typing import Any
import pandas as pd
from pandas import DataFrame
from logging import Logger

import src.context as context


def finalise(
    dataframes: dict[str, DataFrame], etl_context: context.ETLContext, logger: Logger
) -> dict[str, DataFrame]:
    """
    Replace original columns with their cleaned equivalents.

    Drops all non-cleaned columns and renames cleaned columns back to their
    original names using the configured cleaned_suffix. Returns the updated
    DataFrame mapping.
    """
    logger.info("Finalising ETL pipeline")
    cleaned_suffix = etl_context.cleaned_suffix
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
