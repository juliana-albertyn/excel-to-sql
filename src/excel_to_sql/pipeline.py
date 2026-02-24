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


def validate_schema(columns_config: Dict[str, Any]) -> dict[str, Any]:
    """Validates the schema, and returns normalised schema"""
    # validate the yaml configuration - any errors here must raise exception

    # return the schema
    schema = dict()
    for col_name, config in columns_config.items():
        data_type = config["data_type"]
        primary_key = config.get("primary_key", False)
        required = config.get("required", False)
        schema["columns"] = {
            "name": col_name,
            "data_type": data_type,
            "primary_key": str_to_bool(primary_key),
            "required": str_to_bool(required),
        }
    return schema


def update_nan_stats(
    nan_stats: list[dict[str, Any]], stage: int, description: str, df: pd.DataFrame
) -> None:
    first_time = not df.columns[-1].endswith("_cleaned")
    for col_name in df.columns:
        if first_time:
            # first time
            stats = {
                "col_name": col_name,
                "stage": stage,
                "description": description,
                "NaNs": df[col_name].isna().sum(),
            }
            nan_stats.append(stats)
        else:
            # ignore orginal columns
            if col_name.endswith("_cleaned"):
                original_col = "_".join(col_name.split("_")[:-1])
                stats = {
                    "col_name": original_col,
                    "stage": stage,
                    "description": description,
                    "NaNs": df[col_name].isna().sum(),
                }
                nan_stats.append(stats)


def log_nan_stats(
    table_name: str,
    col_names: list[str],
    nan_stats: list[dict[str, Any]],
    logger: Logger,
) -> None:
    col_order = {col_name: i for i, col_name in enumerate(col_names)}
    stats_sorted = sorted(
        nan_stats, key=lambda d: (col_order[d["col_name"]], d["stage"])
    )

    df_stats = pd.DataFrame(nan_stats)
    summary = (
        df_stats.pivot(index="col_name", columns="stage", values="NaNs")
        .rename(
            columns={
                1: "after_loading",
                2: "after_cleaning",
                3: "after_transforming",
                4: "after_validation",
            }
        )
        .reset_index()
    )
    logger.info(
        f"\nSummary of NaNs for *** {table_name} ***\n{summary.to_string(index=False)}"
    )


def run_etl(config: Dict[str, Any], context: Dict[str, Any]) -> None:
    """
    Run the ETL pipeline

    Args:
    config (dict[str, Any]): configuration loaded from the yaml file
    context (dict[str, Any]): run time context used by all units
    """
    logger = logging_setup.get_logger(context, __name__)
    logger.info(f"Start ETL pipeline for '{config["source"]["file"]}'")

    tables = dict()
    schema = dict()

    # validate schema per table, and fail on any errors
    for table_name, mapping in config["mappings"].items():
        schema[table_name] = validate_schema(config["columns"][table_name])

    # go through table by table
    for table_name, mapping in config["mappings"].items():
        # Step 1: extract sheet
        col_configs = config.get("columns", None)
        if col_configs is None:
            raise ValueError("Columns not set up")
        table_cols = col_configs.get(table_name, None)
        if table_cols is None:
            raise ValueError(f"Columns not set up for {table_name}")
        df = extractor.load_excel(
            config["source"],
            table_name,
            mapping["sheet_name"],
            table_cols,
            context,
        )
        # Step 2: add table name attribute to df for logging purposes
        df.table_name = table_name

        # Step 3: rename all the columns to the sql column names specified in the yaml file
        rename_map = {
            cfg["source_column"]: col_name for col_name, cfg in table_cols.items()
        }
        df = df.rename(columns=rename_map)

        # Step 4: keep track of number of NaN/NaT
        nan_stats: list[dict[str, Any]] = []
        update_nan_stats(nan_stats, 1, "After loading", df)

        # Step 5: clean
        df = cleaner.clean_data(
            df, config["cleaning"], config["columns"][table_name], context
        )
        update_nan_stats(nan_stats, 2, "After cleaning", df)

        # Step 6: transform
        df = transformer.transform_data(df, config["columns"][table_name], context)
        update_nan_stats(nan_stats, 3, "After transforming", df)

        # Step 7: validate
        df = validator.validate_data(df, config["columns"][table_name], context)
        update_nan_stats(nan_stats, 4, "After validation", df)

        # Step 8: log NaN summary
        log_nan_stats(table_name, df.columns, nan_stats, logger)

        # Step 9: add df to tables dict
        tables[table_name] = df

    # Step 10: validate foreign keys
    df = validator.validate_foreign_keys(config["columns"], tables, context)

    # Step 11: load
    sql_writer.load_to_sql(config["target"], tables, schema, context)
