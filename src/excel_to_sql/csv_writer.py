"""
Module: csv_writer
Purpose: Write dataframes to csv files

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"

from typing import Any
import pandas
from pandas import DataFrame
from logging import Logger

def to_csv(
    tables: dict[str, Any],
    logger: Logger,
) -> None:
    """Write dataframes to csv files"""
    logger.info("Writing dataframes to csv files")
