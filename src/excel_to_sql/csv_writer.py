"""
CSV writer for the Fynbyte Excel-to-SQL pipeline.

Writes cleaned and validated tables to CSV files in the configured
output directory. Creates the directory if needed and logs each write
operation.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"

from typing import Any
import pandas as pd
from logging import Logger

import src.excel_to_sql.context as context


def to_csv(
    tables: dict[str, pd.DataFrame],
    etl_context: context.ETLContext,
    logger: Logger,
) -> None:
    """
    Write all tables to CSV files.

    Creates the output directory if required, writes each DataFrame using
    its table name, and logs successes or write errors.
    """
    output_dir = etl_context.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    for table_name, table in tables.items():
        csv_name = f"{table_name}.csv"
        csv_path = output_dir / csv_name
        logger.info(f"Writing *** {table_name} *** to '{csv_path}'")
        try:
            table.to_csv(csv_path, index=False)
        except Exception as e:
            logger.error(f"Error {e} writing {table_name} to {csv_name}")
    logger.info("Finished writing dataframes to CSV files")
