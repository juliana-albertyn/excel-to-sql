"""
SQL writer for the Fynbyte Excel-to-SQL pipeline.

"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"

from typing import Any
import pandas
from pandas import DataFrame
from logging import Logger
import src.logging_setup as logging_setup
from config.pipeline_config import PipelineConfig
import src.sql_writer.engine_factory as engine_factory




def to_sql(
    target: dict[str, Any],
    schema: dict[str, Any],
    tables: dict[str, Any],
    logger: Logger,
) -> None:
    """
    Write all tables to the target SQL database.
    """
    pipeline_config = PipelineConfig()
    db_config = pipeline_config.get_active_target()
    engine = engine_factory.get_engine(db_config)
