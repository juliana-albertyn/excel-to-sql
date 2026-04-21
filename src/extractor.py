"""
Excel extractor for the Fynbyte Excel-to-SQL pipeline.

Opens the configured workbook, loads a worksheet into a DataFrame,
validates sheet existence, checks column headers, and applies the
source-to-target column mapping defined in the schema.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"


from pathlib import Path
import pandas as pd
from pandas import DataFrame

import src.logging_setup as logging_setup
import src.errors as errors
import src.context as context

from config.project_config import ProjectConfig
from schemas.table_schema import TableSchema


class Extractor:

    def __init__(
        self,
        project_config: ProjectConfig,
        etl_context: context.ETLContext,
    ):

        self.project_config = project_config
        self.etl_context = etl_context
        self.logger = logging_setup.get_logger(self.etl_context, __name__)

    def from_excel(
        self, table_name: str, sheet_name: str, table_schema: TableSchema
    ) -> DataFrame:
        """
        Load a worksheet from an Excel file into a DataFrame.

        Validates file access, sheet existence, header row settings, and
        unexpected column names. Renames source columns to their configured SQL
        column names and returns the loaded DataFrame.
        """
        self.logger.info(
            f"Extracting data for *** {table_name} *** from sheet '{sheet_name}'"
        )
        header_rows = None
        file_name = ""
        # not raising error here, this is already checked in project_config.py
        if self.project_config.source is not None:
            file_name = (
                Path(self.etl_context.data_dir) / self.project_config.source.file_name
            )
            header_rows = self.project_config.source.header_rows
        # column header row for load from excel is zero based
        # header row for excel validation starts at 1
        if header_rows is not None and header_rows > 0:
            column_header_row = header_rows - 1
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

        for col in table_schema.columns:
            if col.source_column is None:
                continue

            if isinstance(col.source_column, list):
                expected.update(col.source_column)
            else:
                expected.add(col.source_column)

        actual = set(df.columns)

        unexpected = actual - expected
        if self.project_config.runtime is not None:
            if len(unexpected) > 0:
                if self.project_config.runtime.strict_validation:
                    error_context = errors.ErrorContext()
                    error_context.table_name = table_name
                    error_context.details = {
                        "file_name": file_name,
                        "sheet_name": sheet_name,
                        "header_row": column_header_row,
                        "unexpected_cols": unexpected,
                    }

                    raise errors.ExtractorError(
                        f"Unexpected column names loaded from Excel for {table_name}'': expected - {expected}, not expected - {unexpected}",
                        error_context,
                    )
                else:
                    self.logger.warning(
                        f"Unexpected column names loaded from Excel for '{table_name}': expected - {expected}, not expected - {unexpected}"
                    )

        # Step 6: rename all the columns to the sql column names specified in the yaml file
        rename_map = dict()
        for col in table_schema.columns:
            rename_map[col.source_column] = col.column_name
        df = df.rename(columns=rename_map)

        # Step 7: return the dataframe
        return df
