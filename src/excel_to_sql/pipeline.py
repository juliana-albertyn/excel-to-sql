"""
Module: pipeline
Purpose: Pipeline engine

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-17"


from typing import Dict, Any
import pandas as pd
from logging import Logger

import src.excel_to_sql.logging_setup as logging_setup
import src.excel_to_sql.extractor as extractor
import src.excel_to_sql.cleaner as cleaner
import src.excel_to_sql.transformer as transformer
import src.excel_to_sql.validator as validator
import src.excel_to_sql.sql_writer as sql_writer


def str_to_bool(value: str | bool) -> bool:
    """
    Convert common truthy strings to a boolean.
    Accepts 'true', 'yes', '1' (case-insensitive).
    Returns True for those, False otherwise.
    """
    if isinstance(value, str):
        return value.strip().lower() in ("true", "yes", "1")
    return bool(value)  # fallback for non-strings


def config_to_schema(columns_config: Dict[str, Any]) -> dict[str, Any]:
    """Takes the column configuration and extracts only the schema"""
    schema = dict()
    for col_name, config in columns_config.items():
        schema["columns"] = {
            "name": col_name,
            "data_type": config["data_type"],
            "primary_key": str_to_bool(config["primary_key"]),
            "required": str_to_bool(config["required"]),
        }
    return schema


def update_nan_stats(
    nan_stats: dict[str, Any], description: str, df: pd.DataFrame
) -> None:
    first_time = not df.columns[-1].endswith("_cleaned")
    for col_name in df.columns:
        if first_time:
            # first time
            nan_stats[col_name] = {"description": description,  "NaNs": df[col_name].isna().sum()}
        else:
            # ignore orginal columns
            if col_name.endswith("_cleaned"):
                original_col = "_".join(col_name.split("_")[:-1])
                nan_stats[original_col]. = {"description": description,  "NaNs": df[col_name].isna().sum()}

def log_nan_stats(table_name: str, nan_stats: dict[str, Any], logger: Logger) -> None:
    logger.info(f"Summary of NaNs for *** {table_name} ***")
    log_str = ""
    for col_name, stats in nan_stats.items():
        log_str = f"\ncolumn | "
        for description in stats.keys():
            log_str += f"{description} | "
        log_str += f"\n{col_name}"
        for value in stats.values():
            log_str += f"{value} | "
    logger.info(log_str)


def run_etl(config: Dict[str, Any], context: Dict[str, Any]) -> None:
    """
    Run the ETL pipeline

    Args:
    config (dict[str, Any]): configuration loaded from the yaml file
    context (dict[str, Any]): run time context used by all units
    """
    logger = logging_setup.get_logger(context, __name__)
    logger.info(f"Start ETL pipeline for {config["source"]["file"]}")

    # get local from context
    # locale = context["locale"]

    tables = dict()
    schema = dict()

    # go through table by table
    for table_name, mapping in config["mappings"].items():
        # Step 1: extract sheet
        df = extractor.load_excel(
            config["source"],
            table_name,
            mapping["sheet_name"],
            config["columns"][table_name],
            context,
        )

        # keeps track of number of NaN/NaT
        nan_stats = dict()
        update_nan_stats(nan_stats, "After loading", df)
        # Step 2: clean
        df = cleaner.clean_data(
            df, config["cleaning"], config["columns"][table_name], context
        )
        update_nan_stats(nan_stats, "After cleaning", df)

        # Step 3: transform
        df = transformer.transform_data(df, config["columns"][table_name], context)
        update_nan_stats(nan_stats, "After transforming", df)

        # Step 4: validate
        df = validator.validate_data(df, config["columns"][table_name], context)
        update_nan_stats(nan_stats, "After validation", df)

        # log NaN summary
        log_nan_stats(table_name, nan_stats, logger)

        # Step 4: add to tables dict
        tables[table_name] = df
        schema[table_name] = config_to_schema(config["columns"][table_name])

    # Step 6: validate foreign keys
    df = validator.validate_foreign_keys(config["columns"], tables, context)

    # Step 7: load
    sql_writer.load_to_sql(config["target"], tables, schema, context)
