"""
Module: sql_writer
Purpose: Write dataframe to sql server tables

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"

from typing import Any
import pandas
from pandas import DataFrame
import src.excel_to_sql.logging_setup as logging_setup


def load_to_sql(
    df: DataFrame,
    target: dict[str, Any],
    target_table: str,
    context: dict[str, Any],
) -> None:
    """Write dataframe to SQL table"""
    logger = logging_setup.get_logger(context, __name__)
    logger.info(f"Source: {df.file_name} / {df.sheet_name}")
    logger.info(f"Writing to table {target_table}")

    # engine = create_engine(connection_string, fast_executemany=True)
    # df.to_sql(
    #     table_name,
    #     engine,
    #     schema=schema,
    #     if_exists=if_exists,
    #     index=False,
    #     chunksize=batch_size,
    # )
