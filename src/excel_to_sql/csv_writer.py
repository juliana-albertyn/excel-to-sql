"""
Module: csv_writer
Purpose: Write dataframes to csv files

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"

from typing import Any
import pandas as pd
from logging import Logger

def to_csv(
    tables: dict[str, pd.DataFrame],
    context: dict[str, Any],
    logger: Logger,
) -> None:
    """Writing dataframes to csv files"""
    for table_name, table in tables.items():
        output_dir = context.get("output_dir", ".")
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_name = f"{table_name}.csv"
        csv_path = output_dir / csv_name
        logger.info(f"Writing {table_name} to {csv_path}")
        try:
            table.to_csv(csv_path, index=False)
        except Exception as e:
            logger.error(f"Error {e} writing {table_name} to {csv_name}")
    logger.info("Finished writing dataframes to CSV files")
