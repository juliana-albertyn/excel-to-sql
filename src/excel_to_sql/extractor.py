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
    logger.info(f"Extracting data for *** {table_name} ***")
    file_name = Path(context["data_dir"]) / src_config["file"]
    # header row for excel validation starts at 1
    header_row = src_config.get("header_row", 1)
    # column header row for load from excel is zero based
    if header_row > 0:
        column_header_row = header_row - 1
    else:
        column_header_row = None
    df = pd.read_excel(
        io=file_name,
        sheet_name=sheet_name,
        header=column_header_row,
        dtype=str,
    )

    # make sure all column headers are str
    df.columns = df.columns.map(str)

    # Step 2: rename all the columns to the sql column names specified in the yaml file
    rename_map = {
        cfg["source_column"]: col_name for col_name, cfg in column_config.items()
    }
    df = df.rename(columns=rename_map)

    # check for unknown column headers in the excel spreadsheet
    expected = set(col_name for col_name in column_config.keys())  # from YAML
    actual = set(df.columns)
    unexpected = actual - expected
    if len(unexpected) > 0:
        logger.warning(f"Unexpected column names loaded from excel for {table_name}: {unexpected}")
        raise ValueError(f"Unexpected column names loaded from excel for {table_name}: {unexpected}")
    # return the dataframe
    return df
