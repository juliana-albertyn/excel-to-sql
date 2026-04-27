"""
ETL pipeline engine for the Fynbyte Excel-to-SQL toolkit.

Loads project and pipeline configuration, validates schema, extracts
worksheets, applies cleaning and transformation rules, validates data,
finalises column names, and writes results to the configured outputs.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-17"


from typing import Any
import pandas as pd
from logging import Logger, INFO
from pathlib import Path
from datetime import datetime
from enum import IntEnum

import src.logging_setup as logging_setup
import src.transformer as transformer
import src.validator as validator
import src.finaliser as finaliser
import src.loader as loader
import src.errors as errors
import src.context as context

from config.project_config import ProjectConfig
from config.pipeline_config import PipelineConfig
from src.extractor import Extractor
from src.cleaner import Cleaner
from src.transformer import Transformer

from schemas.table_schema import TableSchema


class ETLStages(IntEnum):
    """
    Enumeration of ETL processing stages used for NaN tracking and summaries.
    """

    LOADED = 1
    CLEANED = 2
    TRANSFORMED = 3
    VALIDATED = 4


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
        logger.info(f"Loading project configuration from {project_config_file}")
        project_config = ProjectConfig.from_yaml(project_config_file)

        # load the pipeline configuration
        pipeline_config_file = (
            etl_context.config_dir / project_config.runtime.pipeline_config_file
        )
        logger.info(f"Loading pipeline configuration from {pipeline_config_file}")
        pipeline_config = PipelineConfig.from_yaml(pipeline_config_file)

        if project_config.runtime is not None:
            if project_config.runtime.strict_validation:
                logger.info("Applying STRICT validation")
            else:
                logger.info("Applying PERMISSIVE validation")

        extractor = Extractor(project_config, etl_context)

        # load the schemas
        if project_config.mappings is not None:
            for table_name, mapping in project_config.mappings:
                schema_file = Path(etl_context.config_dir / mapping.schema_file)
                logger.info(f"Loading {table_name} schema from {schema_file}")
                schema = TableSchema.from_yaml(schema_file)
                logger.info(f"Validating {table_name} schema from {schema_file}")
                schema.validate(engine_type=pipeline_config.active_target)

                # Step 1: extract sheet
                df = extractor.from_excel(
                    table_name=table_name,
                    sheet_name=mapping.sheet_name,
                    table_schema=schema,
                )

                # Step 2: keep track of number of NaN/NaT
                nan_stats: list[dict[str, Any]] = []
                update_nan_stats(
                    nan_stats, ETLStages.LOADED, "After loading", etl_context, df
                )

                # Step 3: clean
                cleaner = Cleaner(
                    df=df,
                    project_config=project_config,
                    table_schema=schema,
                    etl_context=etl_context,
                )
                df = cleaner.clean_data()

                update_nan_stats(
                    nan_stats, ETLStages.CLEANED, "After cleaning", etl_context, df
                )

                # Step 4: transform
                transformer = Transformer(
                    df=df,
                    project_config=project_config,
                    table_schema=schema,
                    etl_context=etl_context,
                )

                df = transformer.transform_data()

                update_nan_stats(
                    nan_stats,
                    ETLStages.TRANSFORMED,
                    "After transforming",
                    etl_context,
                    df,
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
            validator.validate_foreign_keys(
                pipeline_config["columns"], tables, etl_context
            )

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
