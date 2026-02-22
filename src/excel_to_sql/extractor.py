"""
Module: extractor
Purpose: Extracts data from excel workbook with worksheets to pandas dataframe

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"


from typing import Any
from pathlib import Path
import pandas as pd
from pandas import DataFrame
import src.excel_to_sql.logging_setup as logging_setup


def load_excel(
    src_config: dict[str, Any],
    table_name: str,
    sheet_name: str,
    column_config: dict[str, Any],
    context: dict[str, Any],
) -> DataFrame:
    """load data from excel into dataframe"""
    logger = logging_setup.get_logger(context, __name__)
    logger.info(f"Extracting data for {table_name}")
    logger.info(f"Pandas version {pd.__version__}")
    file_name = Path(context["data_dir"]) / src_config["file"]
    df = pd.read_excel(
        io=file_name,
        sheet_name=sheet_name,
        header=src_config.get("header_row", 0),
        dtype=str,
    )

    # store attributes for logging in other units
    df.file_name = file_name
    df.table_name = table_name

    # return the dataframe
    return df
