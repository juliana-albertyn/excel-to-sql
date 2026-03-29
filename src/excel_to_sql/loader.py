"""
Loader for the Fynbyte Excel-to-SQL pipeline.

Writes cleaned and validated tables to the target database and optionally
exports CSV or Excel snapshots for inspection, debugging, or client
handover.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-28"


from typing import Any, Callable
import pandas as pd
from logging import Logger

import src.excel_to_sql.logging_setup as logging_setup
import src.excel_to_sql.sql_writer as sql_writer
import src.excel_to_sql.csv_writer as csv_writer
import src.excel_to_sql.excel_writer as excel_writer
import src.excel_to_sql.context as context


class DatabaseLoader:
    """
    Orchestrates writing tables to the configured output targets.

    Uses the provided writer functions to load data into the database and,
    optionally, to CSV or Excel depending on project settings.
    """    
    def __init__(
        self,
        target: dict[str, Any],
        schema: dict[str, Any],
        tables: dict[str, Any],
        etl_context: context.ETLContext,
        logger: Logger,
        sql_writer: Callable[..., None],
        csv_writer: Callable[..., None] | None = None,
        excel_writer: Callable[..., None] | None = None,
    ):
        """
        Initialise a DatabaseLoader instance.

        Stores writer functions, schema, tables, and context needed for the
        loading stage.
        """        
        self.sql_writer = sql_writer
        self.csv_writer = csv_writer
        self.excel_writer = excel_writer
        self.target = target
        self.schema = schema
        self.tables = tables
        self.etl_context = etl_context
        self.logger = logger

    def load(self):
        """
        Load tables to all configured output types.

        Always writes to the database. Writes CSV and Excel snapshots when the
        corresponding writer functions are supplied.
        """        
        try:
            self.sql_writer(self.target, self.schema, self.tables, self.logger)
            if self.csv_writer is not None:
                self.csv_writer(self.tables, self.etl_context, self.logger)
            if self.excel_writer is not None:
                self.excel_writer(self.tables, self.etl_context, self.logger)
        except:
            raise


def load_database(
    project: dict[str, Any],
    target: dict[str, Any],
    schema: dict[str, Any],
    tables: dict[str, pd.DataFrame],
    etl_context: context.ETLContext,
    logger: Logger,
) -> None:
    """
    Configure and run the database loading step.

    Selects optional CSV and Excel exporters based on project settings,
    creates a DatabaseLoader instance, and triggers the load process.
    """    
    logger = logging_setup.get_logger(etl_context, __name__)
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
        etl_context,
        logger,
        sql_writer.to_sql,
        csv_writer=load_csv,
        excel_writer=load_excel,
    )
    db_loader.load()
