"""
Module: loader
Purpose: Write a dataframe to target database. Optionally also writes to csv or
excel for manual inspection, debugging, client handovers and snapshots

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-28"


from typing import Any, Callable
import pandas as pd
from pandas import DataFrame
from logging import Logger

import src.excel_to_sql.logging_setup as logging_setup
import src.excel_to_sql.sql_writer as sql_writer
import src.excel_to_sql.csv_writer as csv_writer
import src.excel_to_sql.excel_writer as excel_writer


class DatabaseLoader:
    def __init__(
        self,
        target: dict[str, Any],
        schema: dict[str, Any],
        tables: dict[str, Any],
        context: dict[str, Any],
        logger: Logger,
        sql_writer: Callable[..., None],
        csv_writer: Callable[..., None] | None = None,
        excel_writer: Callable[..., None] | None = None,
    ):
        """Initialise an instance of DatabaseLoader"""
        self.sql_writer = sql_writer
        self.csv_writer = csv_writer
        self.excel_writer = excel_writer
        self.target = target
        self.schema = schema
        self.tables = tables
        self.logger = logger

    def load(self):
        """load the data to the various output types"""
        try:
            self.sql_writer(self.target, self.tables, self.schema)
            if self.csv_writer is not None:
                self.csv_writer(self.tables)
            if self.excel_writer is not None:
                self.excel_writer(self.tables)
        except:
            raise


def load_database(
    project: dict[str, Any],
    target: dict[str, Any],
    schema: dict[str, Any],
    tables: dict[str, pd.DataFrame],
    context: dict[str, Any],
    logger: Logger,
):
    logger = logging_setup.get_logger(context, __name__)
    exports: list[str] = project.get("exports", [])
    load_csv = None
    load_excel = None
    if "csv" in exports:
        load_csv = csv_writer.to_csv
    if "excel" in exports:
        load_excel = excel_writer.to_excel

    db_loader = DatabaseLoader(
        target,
        schema,
        tables,
        context, 
        logger,
        sql_writer.to_sql,
        csv_writer=load_csv,
        excel_writer=load_excel,
    )
    db_loader.load()
