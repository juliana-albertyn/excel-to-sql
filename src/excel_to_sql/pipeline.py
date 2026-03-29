"""
ETL pipeline engine for the Fynbyte Excel-to-SQL toolkit.

Loads project and pipeline configuration, validates schema, extracts
worksheets, applies cleaning and transformation rules, validates data,
finalises column names, and writes results to the configured outputs.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-17"


import yaml
from typing import Dict, Any
import pandas as pd
from logging import Logger, INFO
from pathlib import Path
from datetime import datetime
from enum import IntEnum
import re

import src.excel_to_sql.logging_setup as logging_setup
import src.excel_to_sql.extractor as extractor
import src.excel_to_sql.cleaner as cleaner
import src.excel_to_sql.transformer as transformer
import src.excel_to_sql.validator as validator
import src.excel_to_sql.finaliser as finaliser
import src.excel_to_sql.loader as loader
import src.excel_to_sql.errors as errors
import src.excel_to_sql.context as context


class ETLStages(IntEnum):
    """
    Enumeration of ETL processing stages used for NaN tracking and summaries.
    """    
    LOADED = 1
    CLEANED = 2
    TRANSFORMED = 3
    VALIDATED = 4


def str_to_bool(value: str | bool) -> bool:
    """
    Convert common truthy strings to a boolean.

    Accepts 'true', 'yes', '1' (case-insensitive). Falls back to Python
    truthiness for non-string values.
    """    
    if isinstance(value, str):
        return value.strip().lower() in ("true", "yes", "1")
    return bool(value)  # fallback for non-strings


def validate_schema(pipeline_config: Dict[str, Any], logger: Logger) -> dict[str, Any]:
    """
    Validate the pipeline schema configuration.

    Checks required fields, data types, primary keys, foreign keys, and
    validation rules. Raises SchemaError on any incorrect or incomplete
    definitions. Returns a simplified schema structure.
    """
    def add_invalid(
        required_fields: list[str], config: dict[str, Any], invalid_fields: list[str]
    ) -> list[str]:
        for field in required_fields:
            value = config.get(field)
            if value is None:
                invalid_fields.append(f"Required field '{field}' not given")
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
    required_fields["columns"] = ["source_column", "data_type", "allow_null"]

    # set up allowed datatypes per target database type
    valid_data_types: dict[str, list[str]] = dict()
    valid_data_types["mssql"] = [
        "char",
        "varchar",
        "text",
        "nchar",
        "nvarchar",
        "ntext",
        "tinyint",
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

    # set up the list for invalid entries
    invalid_entries: list[str] = []
    schema: dict[str, dict[str, Any]] = dict()

    source = pipeline_config.get("source")
    if source is None:
        invalid_entries.append("Source configuration not given")
    else:
        invalid_entries = add_invalid(
            required_fields["source"], source, invalid_entries
        )
        header_row = source.get("header_row")
        if header_row is not None and source.get("header_row") < 0:
            invalid_entries.append(
                "Header row must be zero (no header) or a positive number"
            )
    target = pipeline_config.get("target")
    if target is None:
        invalid_entries.append("Target configuration not given")
    else:
        invalid_entries = add_invalid(
            required_fields["target"], target, invalid_entries
        )
        authentication = target.get("authentication")
        if authentication is not None:
            invalid_entries = add_invalid(
                required_fields["authentication"], authentication, invalid_entries
            )
    cleaning = pipeline_config.get("cleaning")
    if cleaning is None:
        invalid_entries.append("Cleaning configuration not given")
    else:
        invalid_entries = add_invalid(
            required_fields["cleaning"], cleaning, invalid_entries
        )
    mappings = pipeline_config.get("mappings")
    if mappings is None:
        invalid_entries.append("Mappings configuration not given")
    else:
        for table_name, table_config in mappings.items():
            invalid_entries = add_invalid(
                required_fields["mappings"], table_config, invalid_entries
            )
    columns = pipeline_config.get("columns")
    if columns is None:
        invalid_entries.append("Columns configuration not given")
    else:
        target_db = None
        if target is not None:
            target_db = target.get("db_type")
        for table_name, table_config in columns.items():
            has_primary_key = False
            col_setup: dict[str, dict[str, Any]] = dict()
            for col_name, col_config in table_config.items():
                desc = f"{table_name} - {col_name}:"
                invalid_entries = add_invalid(
                    required_fields["columns"], col_config, invalid_entries
                )
                data_type = col_config.get("data_type")
                if (
                    target_db is not None and data_type is not None
                ):  # otherwise reported under required fields
                    valid_dtypes = valid_data_types.get(target_db)
                    starts_with = data_type.split("(", 2)[0]
                    if starts_with not in valid_dtypes:
                        invalid_entries.append(
                            f"{desc} Data type {data_type} is not a valid for {target_db}"
                        )
                    elif starts_with in strlen_data_types:
                        # search for the brackets
                        m = re.search(r"\((\d+)\)", data_type)
                        if m is None:
                            invalid_entries.append(
                                f"{desc} Data type {data_type} length not given"
                            )
                        else:
                            # get the max length
                            limit = int(m.group(1))
                            if limit <= 0:
                                invalid_entries.append(
                                    f"{desc} Data type {data_type} length must be a positive value"
                                )
                primary_key = str_to_bool(col_config.get("primary_key", False))
                nullable = str_to_bool(col_config.get("nullable", False))
                foreign_key = col_config.get("foreign_key")
                has_primary_key = has_primary_key or primary_key

                value_mapping = col_config.get("value_mapping")
                if value_mapping is not None:
                    for normalised, raw_list in value_mapping.items():
                        if raw_list is None:
                            invalid_entries.append(
                                f"{desc} No value mappings for {normalised}"
                            )

                validation = col_config.get("validation")
                if validation is not None:
                    if data_type in date_data_types:
                        min_value = validation.get("min_value")
                        max_value = validation.get("max_value")
                        if min_value is None or max_value is None:
                            invalid_entries.append(
                                f"{desc} Data type {data_type} must have min and max values"
                            )

                    validation_format = validation.get("format")
                    if validation_format == "E.164":
                        if str_to_bool(validation.get("allow_local")):
                            dialling_prefix = validation.get("dialling_prefix")
                            if dialling_prefix is None:
                                invalid_entries.append(
                                    f"{desc} Dialling prefix must be given if 'allow local' is set to true"
                                )
                            elif not dialling_prefix.startswith("+"):
                                invalid_entries.append(
                                    f"{desc} Country dialling prefix must start with '+'"
                                )

                    derived_from = validation.get("derived_from")
                    if derived_from is not None:
                        depends_on = derived_from.get("depends_on")
                        formula = derived_from.get("formula")
                        if formula is None or depends_on is None:
                            invalid_entries.append(
                                f"{desc} Formula and 'depends on' must be given for derived columns"
                            )
                            for col in depends_on:
                                if col not in table_config.keys():
                                    invalid_entries.append(
                                        f"{desc} {col} used by {formula} missing"
                                    )
                col_setup[col_name] = {
                    "data_type": data_type,
                    "primary_key": primary_key,
                    "nullable": nullable,
                    "foreign_key": foreign_key,
                }
            schema[table_name] = col_setup
            if not has_primary_key:
                invalid_entries.append(f"{table_name} does not have a primary key")

        # check that foreign keys point to valid table and column
        for table_name, table_config in schema.items():
            for col_name, col_config in table_config.items():
                desc = f"{table_name} - {col_name}:"
                foreign_key = col_config.get("foreign_key")
                if foreign_key is not None:
                    foreign_key_parts = foreign_key.split(".", 2)
                    if len(foreign_key_parts) < 2:
                        invalid_entries.append(
                            f"{desc} Foreign key {foreign_key} not set up correctly"
                        )
                    else:
                        f_table_name = foreign_key_parts[0]
                        f_col_name = foreign_key_parts[1]
                        f_table_cols = schema.get(f_table_name)
                        if f_table_cols is None:
                            invalid_entries.append(
                                f"{desc} Foreign key table {f_table_name} does not exist"
                            )
                        else:
                            if f_col_name not in f_table_cols:
                                invalid_entries.append(
                                    f"{desc} Foreign key column {f_col_name} does not exist in table {f_table_name}"
                                )

    if len(invalid_entries) != 0:
        error_context = errors.ErrorContext()
        err_str = "\n".join(invalid_entries)
        raise errors.SchemaError(
            f"\nSchema incomplete/incorrect {err_str}", error_context
        )

    return schema


def update_nan_stats(
    nan_stats: list[dict[str, Any]],
    stage: int,
    description: str,
    etl_context: context.ETLContext,
    df: pd.DataFrame,
) -> None:
    """
    Record NaN counts for a DataFrame at a specific ETL stage.

    Adds per-column NaN statistics to the running summary, using cleaned
    column names when available.
    """    
    cleaned_suffix = etl_context.cleaned_suffix
    if cleaned_suffix is not None:
        first_time = True
        for col in reversed(df.columns):
            if col.endswith(cleaned_suffix):
                first_time = False
                break
        for col_name in df.columns:
            if first_time:
                stats = {
                    "col_name": col_name,
                    "stage": stage,
                    "description": description,
                    "NaNs": df[col_name].isna().sum(),
                }
                nan_stats.append(stats)
            else:
                # ignore orginal columns
                if col_name.endswith(cleaned_suffix):
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
    """
    Log a summary table of NaN counts across ETL stages for a single table.
    """    
    df_stats = pd.DataFrame(nan_stats)
    summary = (
        df_stats.pivot(index="col_name", columns="stage", values="NaNs")
        .rename(
            columns={
                ETLStages.LOADED: "LOADED",
                ETLStages.CLEANED: "CLEANED",
                ETLStages.TRANSFORMED: "TRANSFORMED",
                ETLStages.VALIDATED: "VALIDATED",
            }
        )
        .reset_index()
    )
    logger.info(
        f"\nSummary of NaNs for *** {table_name} ***\n{summary.to_string(index=False)}"
    )


def lower_keys(d: dict[str, Any]) -> dict[str, Any]:
    """
    Return a dictionary with all string keys converted to lowercase.

    Applies recursively to nested dictionaries.
    """
    new = {}
    for k, v in d.items():
        key = k.lower() if isinstance(k, str) else k
        if isinstance(v, dict):
            new[key] = lower_keys(v)
        else:
            new[key] = v
    return new


def load_pipeline_config(
    project_config: dict[str, Any], etl_context: context.ETLContext
) -> dict[str, Any]:
    """
    Load and normalise the pipeline configuration file.

    Reads the YAML file referenced in the project configuration, validates
    required sections, and returns a lowercase-keyed configuration dict.
    """    
    pipeline_config_file = project_config.get("config_file")
    if pipeline_config_file is None:
        error_context = errors.ErrorContext()
        raise errors.ConfigError(
            "Pipeline configuration file not specified", error_context
        )
    try:
        # open the pipeline configuration file
        pipeline_config_file = etl_context.config_dir / pipeline_config_file
        with open(pipeline_config_file) as f:
            raw_pipeline_config = yaml.safe_load(f)
        pipeline_config = lower_keys(raw_pipeline_config)

        # load configurations
        project_name = pipeline_config["project_name"]
        strict_validation = pipeline_config["strict_validation"]
        source_config = pipeline_config["source"]
        target_config = pipeline_config["target"]
        cleaning_rules = pipeline_config["cleaning"]
        mapping_config = pipeline_config["mappings"]
        columns_config = pipeline_config["columns"]

        etl_config = {
            "project_name": project_name,
            "strict_validation": strict_validation,
            "source": source_config,
            "target": target_config,
            "cleaning": cleaning_rules,
            "mappings": mapping_config,
            "columns": columns_config,
        }
        return lower_keys(etl_config)
    except Exception as e:
        error_context = errors.ErrorContext(original_exception=e)
        raise errors.ConfigError("Error loading pipeline configuration", error_context)


def load_project_config(project_config_file: Path) -> dict[str, Any]:
    """
    Load the top-level project configuration from YAML.

    Returns a lowercase-keyed configuration dict. Raises ConfigError on
    read or parse failures.
    """
    try:
        with open(project_config_file) as f:
            raw_config = yaml.safe_load(f)
        return lower_keys(raw_config)
    except Exception as e:
        error_context = errors.ErrorContext(original_exception=e)
        raise errors.ConfigError("Error loading project configuration", error_context)


def setup_context() -> context.ETLContext:
    """
    Create a minimal ETLContext with default paths and run metadata.

    Initialises log, config, data, and output directories and sets the run
    timestamp used for log file naming.
    """
    # figure out paths
    try:
        project_root = Path(__file__).resolve().parents[2]
        log_dir = project_root / "logs"
        config_dir = project_root / "config"
        data_dir = project_root / "data" / "raw"
        output_dir = project_root / "data" / "processed"

        etl_context = context.ETLContext(
            log_dir=log_dir,
            data_dir=data_dir,
            output_dir=output_dir,
            config_dir=config_dir,
        )
        run_date = datetime.now()
        etl_context.run_date = run_date.date()
        # all log files for the same run have the same timestamp
        etl_context.run_timestamp = run_date.strftime("%Y%m%d_%H%M_%S")
        etl_context.log_level = INFO
        return etl_context
    except Exception as e:
        error_context = errors.ErrorContext(original_exception=e)
        raise errors.ConfigError("Error setting up run-time context", error_context)


def extend_context(
    etl_context: context.ETLContext, pipeline_config: dict[str, Any]
) -> context.ETLContext:
    """
    Extend the ETLContext with pipeline-specific settings.

    Adds project name, strictness, date parsing rules, currency symbol,
    row offset, and source file information.
    """
    try:
        etl_context.project_name = pipeline_config.get("project_name", "excel_to_sql")
        etl_context.strict_validation = pipeline_config.get("strict_validation", True)
        etl_context.day_first_format = pipeline_config.get("day_first_format", True)
        etl_context.currency_symbol = pipeline_config.get("currency_symbol", "")
        header_row = pipeline_config.get("header_row", 1)
        etl_context.row_offset = 1 + header_row
        source = pipeline_config.get("source")
        if source is None:
            raise ValueError("Source must be given")
        source_file = source.get("file")
        if source_file is None:
            raise ValueError("Source file must be given")
        etl_context.source_file = source_file
        return etl_context
    except Exception as e:
        error_context = errors.ErrorContext(original_exception=e)
        raise errors.ConfigError("Error extending run-time context", error_context)


def run_etl() -> None:
    """
    Run the full ETL pipeline.

    Loads configuration, validates schema, extracts sheets, cleans,
    transforms, validates, finalises, and writes all tables. Logs progress
    and raises PipelineError on failure.
    """    
    # set up the context
    etl_context = setup_context()

    # get logger and log start
    logger = logging_setup.get_logger(etl_context, __name__)
    logger.info("Loading ETL pipeline configuration")
    logger.info(f"Pandas version {pd.__version__}")

    try:
        # load project config
        project_config_file = etl_context.config_dir / "project_config.yaml"
        project_config = load_project_config(project_config_file)

        # now load the pipeline configuration
        pipeline_config = load_pipeline_config(project_config, etl_context)

        # add more attributes to the runtime context
        etl_context = extend_context(etl_context, pipeline_config)
        if etl_context.strict_validation:
            logger.info("Applying STRICT validation")
        else:
            logger.info("Applying PERMISSIVE validation")

        # validate schema, and fail on any errors
        logger.info("Validating ETL pipeline configuration")
        schema = validate_schema(pipeline_config, logger)

        tables: dict[str, pd.DataFrame] = dict()
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
                etl_context,
            )

            # Step 2: keep track of number of NaN/NaT
            nan_stats: list[dict[str, Any]] = []
            update_nan_stats(
                nan_stats, ETLStages.LOADED, "After loading", etl_context, df
            )

            # Step 3: clean
            df = cleaner.clean_data(
                df,
                pipeline_config["cleaning"],
                table_name,
                pipeline_config["columns"][table_name],
                etl_context,
            )
            update_nan_stats(
                nan_stats, ETLStages.CLEANED, "After cleaning", etl_context, df
            )

            # Step 4: transform
            df = transformer.transform_data(
                df, table_name, pipeline_config["columns"][table_name], etl_context
            )
            update_nan_stats(
                nan_stats, ETLStages.TRANSFORMED, "After transforming", etl_context, df
            )

            # Step 5: validate
            df = validator.validate_data(
                df, table_name, pipeline_config["columns"][table_name], etl_context
            )
            update_nan_stats(
                nan_stats, ETLStages.VALIDATED, "After validation", etl_context, df
            )

            # Step 6: log NaN summary
            log_nan_stats(table_name, df.columns, nan_stats, logger)

            # Step 7: add df to tables dict
            tables[table_name] = df

        # Step 8: validate foreign keys
        validator.validate_foreign_keys(pipeline_config["columns"], tables, etl_context)

        # Step 9: finalise
        finaliser.finalise(tables, etl_context, logger)

        # Step 10: load
        loader.load_database(
            project_config,
            pipeline_config["target"],
            schema,
            tables,
            etl_context,
            logger,
        )
        logger.info("ETL pipeline finished successfully")
    except errors.PipelineError as e:
        logger.error(str(e))
        logger.debug(e.to_dict())
        raise e

    finally:
        logging_setup.shutdown()
