"""
Module: extractor
Purpose: Extracts data from excel workbook with worksheets to pandas dataframe

This module is part of the Fynbyte toolkit.
"""


__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"


from typing import Any
import pandas as pd
from pandas import DataFrame
import src.excel_to_sql.logging_setup as logging_setup


def load_excel(
    src_config: dict[str, Any], sheet_name: str, context: dict[str, Any]
) -> DataFrame:
    """load data from excel into dataframe"""
    logger = logging_setup.get_logger(context, __name__)
    logger.info(f"Pandas version {pd.__version__}")
    logger.info(f"Extracting data from {src_config['file']}")
