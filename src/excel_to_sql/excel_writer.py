"""
Excel writer for the Fynbyte Excel-to-SQL pipeline.

Writes cleaned and validated tables to a single Excel workbook, creating
one worksheet per table. Creates the output directory if needed and
names the workbook based on the source file.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"

from typing import Any
import pandas as pd
from logging import Logger
from pathlib import Path

import src.excel_to_sql.context as context


def to_excel(
    tables: dict[str, pd.DataFrame],
    etl_context: context.ETLContext,
    logger: Logger,
) -> None:
    """
    Write all tables to an Excel workbook.

    Creates the output directory, builds an output filename based on the
    source file and cleaned suffix, and writes each DataFrame to its own
    worksheet. Logs each write operation.
    """
    output_dir = etl_context.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    source_name = etl_context.source_file
    if source_name is not None:
        cleaned_suffix = etl_context.cleaned_suffix
        p = Path(source_name)
        output_name = f"{p.stem}{cleaned_suffix}{p.suffix}"
        output_path = output_dir / output_name
        logger.info(f"Writing dataframes to '{output_path}'")

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            for table_name, df in tables.items():
                logger.info(f"Writing *** {table_name} *** to '{output_name}'")
                df.to_excel(
                    writer, sheet_name=table_name[:31], index=False
                )  # worksheet names max of 31 chars

    logger.info("Finished writing dataframes to Excel file")
