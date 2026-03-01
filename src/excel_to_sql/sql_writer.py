"""
Module: sql_writer
Purpose: Write dataframes to sql server tables

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"

from typing import Any
import pandas
from pandas import DataFrame
from logging import Logger
import src.excel_to_sql.logging_setup as logging_setup


def connect_sql(target: dict[str, Any]) -> None:
    pass


def create_db(target: dict[str, Any]) -> None:
    pass


def create_tables(target: dict[str, Any], schema: dict[str, Any]) -> None:
    pass


def to_sql(
    target: dict[str, Any],
    schema: dict[str, Any],
    tables: dict[str, Any],
    logger: Logger,
) -> None:
    """Write dataframe to SQL table"""
    db_name = target.get("database")
    logger.info(f"Creating database {db_name}")
    connect_sql(target)
    create_db(target)
    create_tables(target, schema)
    for table, df in tables.items():
        logger.info(f"Writing to table {table}")

    # engine = create_engine(connection_string, fast_executemany=True)
    # df.to_sql(
    #     table_name,
    #     engine,
    #     schema=schema,
    #     if_exists=if_exists,
    #     index=False,
    #     chunksize=batch_size,
    # )
