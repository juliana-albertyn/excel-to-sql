"""
SQL writer for the Fynbyte Excel-to-SQL pipeline.

Defines the interface for connecting to the target database, creating
the database and tables, and writing DataFrames to SQL. Functions are
stubs and will be implemented in the next phase.
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
    """
    Connect to the target SQL database.

    Stub. The concrete implementation should establish a database
    connection using the settings in the target configuration.
    """    
    pass


def create_db(target: dict[str, Any]) -> None:
    """
    Create the target database if required.

    Stub. The concrete implementation should create the database when it
    does not already exist, based on the target configuration.
    """    
    pass


def create_tables(target: dict[str, Any], schema: dict[str, Any]) -> None:
    """
    Create SQL tables based on the validated schema.

    Stub. The concrete implementation should generate and execute the
    DDL required to create all tables defined in the schema.
    """    
    pass


def to_sql(
    target: dict[str, Any],
    schema: dict[str, Any],
    tables: dict[str, Any],
    logger: Logger,
) -> None:
    """
    Write all tables to the target SQL database.

    Calls the connection, database creation, and table creation stubs, then
    logs each table write. Actual insert logic will be implemented in the
    database-specific writer.
    """
    db_name = target.get("database")
    logger.info(f"Writing to database *** {db_name} ***")
    connect_sql(target)
    create_db(target)
    create_tables(target, schema)
    for table, df in tables.items():
        logger.info(f"Writing to table *** {table} ***")

    # engine = create_engine(connection_string, fast_executemany=True)
    # df.to_sql(
    #     table_name,
    #     engine,
    #     schema=schema,
    #     if_exists=if_exists,
    #     index=False,
    #     chunksize=batch_size,
    # )
