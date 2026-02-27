"""
Module: pipeline
Purpose: Pipeline engine

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-17"


import yaml
from typing import Dict, Any
import pandas as pd
from logging import Logger
from pathlib import Path
from datetime import datetime
import re

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


def validate_schema(pipeline_config: Dict[str, Any], logger: Logger) -> dict[str, Any]:
    """
    Validates the schema, and returns the target schema

    Raises:
    ValueError: Any missing required fields or setups, or any incorrect setups
    """

    def add_invalid(
        required_fields: list[str], config: dict[str, Any], invalid_fields: list[str]
    ) -> list[str]:
        for field in required_fields:
            value = config.get(field)
            if value is None:
                invalid_fields.append(f"Required field {field} not given")
        return invalid_fields

    # set up the required fields
    required_fields: dict[str, list[str]] = dict()
    required_fields["target"] = [
        "db_type",
        "driver",
        "server",
        "database",
        "authentication",
        "schema",
        "if_exists",
        "batch_size",
        "fast_executemany",
    ]
    required_fields["authentication"] = ["type", "username", "password"]
    required_fields["source"] = ["type", "file", "header_row"]
    required_fields["cleaning"] = [
        "trim_whitespace",
        "standardise_nulls",
        "remove_blank_rows",
    ]
    required_fields["mappings"] = ["sheet_name", "target_table"]
    required_fields["columns"] = ["source_column", "data_type", "nullable"]

    # set up allowed datatypes per target database type
    valid_data_types: dict[str, list[str]] = dict()
    valid_data_types["mssql"] = [
        "char",
        "varchar",
        "text",
        "nchar",
        "nvarchar",
        "ntext" "tinyint",
        "smallint",
        "int",
        "bigint",
        "bit",
        "decimal",
        "numeric",
        "money",
        "smallmoney",
        "float",
        "real",
        "date",
        "time",
        "datetime",
        "binary",
        "varbinary",
        "image",
    ]

    # some str data types need a length given
    strlen_data_types: dict[str, list[str]] = dict()
    strlen_data_types["mssql"] = ["char", "varchar", "nchar", "nvarchar"]

    # dates must always have a min and max
    date_data_types: dict[str, list[str]] = dict()
    date_data_types["mssql"] = ["date", "time", "datetime"]

    # set up the list for invalid values
    invalid: list[str] = []
    schema: dict[str, dict[str, Any]] = dict()

    source = pipeline_config.get("source")
    if source is None:
        invalid.append("Source configuration not given")
    else:
        invalid = add_invalid(required_fields["source"], source, invalid)
    target = pipeline_config.get("target")
    if target is None:
        invalid.append("Target configuration not given")
    else:
        invalid = add_invalid(required_fields["target"], target, invalid)
        authentication = target.get("authentication")
        if authentication is not None:
            invalid = add_invalid(
                required_fields["authentication"], authentication, invalid
            )
    cleaning = pipeline_config.get("cleaning")
    if cleaning is None:
        invalid.append("Cleaning configuration not given")
    else:
        invalid = add_invalid(required_fields["cleaning"], cleaning, invalid)
    mapping = pipeline_config.get("mappings")
    if mapping is None:
        invalid.append("Mappings configuration not given")
    else:
        invalid = add_invalid(required_fields["mappings"], mapping, invalid)
    columns = pipeline_config.get("columns")
    if columns is None:
        invalid.append("Columns configuration not given")
    else:
        target_db = None
        if target is not None:
            target_db = target.get("db_type")
        for table_name, table_config in columns.items():
            invalid = add_invalid(
                required_fields["columns"], columns[table_name], invalid
            )
            has_primary_key = False
            for col_name, col_config in table_config.items():
                desc = f"Table {table_name} Column {col_name}:"
                data_type = col_config.get("data_type")
                if (
                    target_db is not None and data_type is not None
                ):  # otherwise reported under required fields
                    valid_dtypes = valid_data_types.get("sql")
                    if data_type not in valid_dtypes:
                        invalid.append(
                            f"{desc} Data type {data_type} is not a valid for {target_db}"
                        )
                    elif data_type in strlen_data_types:
                        # search for the brackets
                        m = re.search(r"\((\d+)\)", data_type)
                        if m is None:
                            invalid.append(
                                f"{desc} Data type {data_type} length not given"
                            )
                        else:
                            # get the max length
                            limit = int(m.group(1))
                            if limit <= 0:
                                invalid.append(
                                    f"{desc} Data type {data_type} length must be a positive value"
                                )
                primary_key = str_to_bool(col_config.get("primary_key", False))
                nullable = str_to_bool(col_config.get("nullable", False))
                has_primary_key = has_primary_key or primary_key

                value_mapping = col_config.get("value_mapping")
                if value_mapping is not None:
                    for normalised, raw_list in value_mapping.items():
                        if raw_list is None:
                            invalid.append(f"{desc} No value mappings for {normalised}")

                validation = col_config.get("validation")
                if validation is not None:
                    if data_type in date_data_types:
                        min_value = validation.get("min_value")
                        max_value = validation.get("max_value")
                        if min_value is None or max_value is None:
                            invalid.append(
                                f"{desc} Data type {data_type} must have min and max values"
                            )

                    validation_format = validation.get("format")
                    if validation_format == "E.164":
                        if str_to_bool(validation.get("allow_local")):
                            dialling_prefix = validation.get("dialling_prefix")
                            if dialling_prefix is None:
                                invalid.append(
                                    "{desc} Dialling prefix must be given if 'allow local' is set to true"
                                )
                            elif not dialling_prefix.startswith("+"):
                                invalid.append(
                                    "{desc} Country dialling prefix must start with '+'"
                                )

                    derived_from = validation.get("derived_from")
                    if derived_from is not None:
                        if (
                            derived_from["formula"] is None
                            or derived_from["depends_on"] is None
                        ):
                            invalid.append(
                                "{desc} Formula and 'depends on' must be given for derived columns"
                            )
                table_schema = {
                    "data_type": data_type,
                    "primary_key": primary_key,
                    "nullable": nullable,
                }
                schema[table_name] = table_schema

            if not has_primary_key:
                invalid.append(f"{table_name} does not have a primary key")

        # check that foreign keys point to valid table and column
        for table_name, table_config in schema.items():
            for col_name, col_config in table_config[table_name]:
                desc = f"Table {table_name} column {col_name}:"
                foreign_key = col_config.get("foreign_key")
                if foreign_key is not None:
                    foreign_key_parts = foreign_key.split(separator=".", maxsplit=2)
                    if len(foreign_key_parts < 2):
                        invalid.append(
                            f"{desc} Foreign key {foreign_key} not set up correctly"
                        )
                    else:
                        f_table_name = foreign_key_parts[0]
                        f_col_name = foreign_key_parts[1]
                        f_table_cols = schema.get(f_table_name)
                        if f_table_cols is None:
                            invalid.append(
                                f"{desc} Foreign key table {f_table_name} does not exist"
                            )
                        else:
                            if not f_col_name in f_table_cols:
                                invalid.append(
                                    f"{desc} Foreign key column {f_col_name} does not exist in table {f_table_name}"
                                )

    if len(invalid) != 0:
        logger.error("\n".join(invalid))
        raise ValueError("Schema incomplete/incorrect")

    return schema


def update_nan_stats(
    nan_stats: list[dict[str, Any]], stage: int, description: str, df: pd.DataFrame
) -> None:
    """Update the nan stats summary for a table"""
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
    """Log the summary nan stats per table"""
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


def lower_keys(d: dict[str, Any]) -> dict[str, Any]:
    """Make all the entries in the dictionary lowercase"""
    new = {}
    for k, v in d.items():
        key = k.lower() if isinstance(k, str) else k
        if isinstance(v, dict):
            new[key] = lower_keys(v)
        else:
            new[key] = v
    return new


def load_pipeline_config(
    project_config: dict[str, Any], context: dict[str, Any]
) -> dict[str, Any]:
    """Load the pipeline configuration specified in the project configuration file"""
    pipeline_config_file = project_config.get("config_file", None)
    if pipeline_config_file is None:
        raise ValueError("Pipeline configuration file not specified")

    # open the pipeline configuration file
    pipeline_config_file = context["config_dir"] / pipeline_config_file
    with open(pipeline_config_file) as f:
        raw_pipeline_config = yaml.safe_load(f)
    pipeline_config = lower_keys(raw_pipeline_config)

    # load configurations
    source_config = pipeline_config["source"]
    target_config = pipeline_config["target"]
    cleaning_rules = pipeline_config["cleaning"]
    mapping_config = pipeline_config["mappings"]
    columns_config = pipeline_config["columns"]

    etl_config = {
        "source": source_config,
        "target": target_config,
        "cleaning": cleaning_rules,
        "mappings": mapping_config,
        "columns": columns_config,
    }
    return etl_config


def load_project_config(project_config_file: Path) -> dict[str, Any]:
    """Load the project configuration"""
    with open(project_config_file) as f:
        raw_config = yaml.safe_load(f)
    return lower_keys(raw_config)


def setup_context(
    log_dir: Path, data_dir: Path, config_dir: Path, run_date: datetime
) -> dict[str, Any]:
    """Set up the context with the minumum information"""
    context = {
        "environment": "development",
        "log_dir": log_dir,
        "log_level": "INFO",
        "max_logs": 5,  # keep last 5 log files per module
        "run_date": run_date.date(),  # datetime.date object
        # all log files for the same run have the same timestamp
        "run_timestamp": run_date.strftime("%Y-%m-%d_%H-%M-%S"),
        "data_dir": data_dir,
        "config_dir": config_dir,
    }
    return context


def extend_context(
    context: dict[str, Any], pipeline_config: dict[str, Any]
) -> dict[str, Any]:
    """Add to the context from the  configuration"""

    context["project_name"] = pipeline_config.get("project_name", "excel_to_sql")
    context["validation_mode"] = pipeline_config.get("validation_mode", "strict")

    return context


def run_etl() -> None:
    """
    Run the ETL pipeline
    """
    # figure out paths
    project_root = Path(__file__).resolve().parents[2]
    log_dir = project_root / "logs"
    config_dir = project_root / "config"
    data_dir = project_root / "data"

    # load project config
    project_config_file = config_dir / "project_config.yaml"
    project_config = load_project_config(project_config_file)

    # setup the run time context - get the current date time once for the run
    context = setup_context(
        log_dir=log_dir,
        data_dir=data_dir,
        config_dir=config_dir,
        run_date=datetime.now(),
    )

    # get logger and log start
    logger = logging_setup.get_logger(context, "main")
    logger.info("Loading ETL pipeline configuration")

    # now load the pipeline configuration
    pipeline_config = load_pipeline_config(project_config, context)

    # add more attributes to the runtime context
    context = extend_context(context, pipeline_config)

    # validate schema, and fail on any errors
    logger.info(f"Validating ETL pipeline configuration")
    schema = validate_schema(pipeline_config, logger)

    table_dataframe: dict[str, pd.DataFrame] = dict()
    # go through table by table
    for table_name, mapping in pipeline_config["mappings"].items():
        # Step 1: extract sheet
        col_configs = pipeline_config.get("columns", None)
        if col_configs is None:
            continue  # won't get here because error already logged and raised
        table_cols = col_configs.get(table_name, None)
        if table_cols is None:
            continue  # won't get here because error already logged and raised
        df = extractor.load_excel(
            pipeline_config["source"],
            table_name,
            mapping["sheet_name"],
            table_cols,
            context,
        )

        # Step 2: rename all the columns to the sql column names specified in the yaml file
        rename_map = {
            cfg["source_column"]: col_name for col_name, cfg in table_cols.items()
        }
        df = df.rename(columns=rename_map)

        # Step 3: keep track of number of NaN/NaT
        nan_stats: list[dict[str, Any]] = []
        update_nan_stats(nan_stats, 1, "After loading", df)

        # Step 4: clean
        df = cleaner.clean_data(
            df,
            pipeline_config["cleaning"],
            table_name,
            pipeline_config["columns"][table_name],
            context,
        )
        update_nan_stats(nan_stats, 2, "After cleaning", df)

        # Step 5: transform
        df = transformer.transform_data(
            df, table_name, pipeline_config["columns"][table_name], context
        )
        update_nan_stats(nan_stats, 3, "After transforming", df)

        # Step 6: validate
        df = validator.validate_data(
            df, table_name, pipeline_config["columns"][table_name], context
        )
        update_nan_stats(nan_stats, 4, "After validation", df)

        # Step 7: log NaN summary
        log_nan_stats(table_name, df.columns, nan_stats, logger)

        # Step 8: add df to tables dict
        table_dataframe[table_name] = df

    # Step 9: validate foreign keys
    df = validator.validate_foreign_keys(
        pipeline_config["columns"], table_dataframe, context
    )

    # Step 11: load
    sql_writer.load_to_sql(pipeline_config["target"], table_dataframe, schema, context)
    logger.info("ETL pipeline finished successfully")
