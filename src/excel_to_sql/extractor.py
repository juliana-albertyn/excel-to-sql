"""
Excel extractor for the Fynbyte Excel-to-SQL pipeline.

Opens the configured workbook, loads a worksheet into a DataFrame,
validates sheet existence, checks column headers, and applies the
source-to-target column mapping defined in the schema.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"


from typing import Any
from pathlib import Path
import pandas as pd
from pandas import DataFrame

import src.excel_to_sql.logging_setup as logging_setup
import src.excel_to_sql.errors as errors
import src.excel_to_sql.context as context


def load_excel(
    src_config: dict[str, Any],
    table_name: str,
    sheet_name: str,
    column_config: dict[str, Any],
    etl_context: context.ETLContext,
) -> DataFrame:
    """
    Load a worksheet from an Excel file into a DataFrame.

    Validates file access, sheet existence, header row settings, and
    unexpected column names. Renames source columns to their configured SQL
    column names and returns the loaded DataFrame.
    """    
    logger = logging_setup.get_logger(etl_context, __name__)
    logger.info(f"Extracting data for *** {table_name} *** from sheet '{sheet_name}'")
    file_name = Path(etl_context.data_dir) / src_config["file"]
    # header row for excel validation starts at 1
    header_row = src_config.get("header_row", 1)
    # column header row for load from excel is zero based
    if header_row > 0:
        column_header_row = header_row - 1
    else:  # if header row is 0, that means no header
        column_header_row = None

    # Step 1: open the workbook without loading any sheet data
    try:
        excel_file = pd.ExcelFile(file_name)
    except Exception as e:
        error_context = errors.ErrorContext()
        error_context.original_exception = e
        error_context.table_name = table_name
        error_context.details = {"file_name": file_name}
        raise errors.ExtractorError(
            f"Unable to open Excel file '{file_name}'", error_context
        )

    # Step 2: check if the sheet exists
    if sheet_name not in excel_file.sheet_names:
        error_context = errors.ErrorContext()
        error_context.table_name = table_name
        error_context.details = {
            "file_name": file_name,
            "sheet_name": sheet_name,
            "available_sheets": excel_file.sheet_names,
        }
        raise errors.ExtractorError(
            f"Worksheet '{sheet_name}' not found in Excel file '{file_name}'",
            error_context,
        )

    # Step 3: it's safe to load the worksheet now
    try:
        df = pd.read_excel(
            io=excel_file,
            sheet_name=sheet_name,
            header=column_header_row,
            dtype=str,
        )
    except Exception as e:
        error_context = errors.ErrorContext()
        error_context.original_exception = e
        error_context.table_name = table_name
        error_context.details = {
            "file_name": file_name,
            "sheet_name": sheet_name,
            "header_row": column_header_row,
        }
        raise errors.ExtractorError(
            f"Error loading Excel file '{file_name}' into dataframe", error_context
        )

    # Step 4: make sure all column headers are str
    df.columns = df.columns.map(str)

    # Step 5: check for unknown column headers in the excel spreadsheet
    # check actual source_column names against the loaded excel column names
    expected = set()

    for col_name, col in column_config.items():
        source_col = col.get("source_column")
        if source_col is None:
            continue

        if isinstance(source_col, list):
            expected.update(source_col)
        else:
            expected.add(source_col)

    actual = set(df.columns)
    unexpected = actual - expected
    if len(unexpected) > 0:
        if etl_context.strict_validation:
            error_context = errors.ErrorContext()
            error_context.table_name = table_name
            error_context.details = {
                "file_name": file_name,
                "sheet_name": sheet_name,
                "header_row": column_header_row,
                "unexpected_cols": unexpected,
            }

            raise errors.ExtractorError(
                f"Unexpected column names loaded from Excel for {table_name}: {unexpected}",
                error_context,
            )
        else:
            logger.warning(
                f"Unexpected column names loaded from Excel for {table_name}: {unexpected}"
            )

    # Step 6: rename all the columns to the sql column names specified in the yaml file
    rename_map = {
        cfg["source_column"]: col_name for col_name, cfg in column_config.items()
    }
    df = df.rename(columns=rename_map)

    # Step 7: return the dataframe
    return df
