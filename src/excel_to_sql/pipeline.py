"""
Module: pipeline
Purpose: Pipeline engine

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-17"


from typing import Dict, Any

import src.excel_to_sql.logging_setup as logging_setup
import src.excel_to_sql.extractor as extractor
import src.excel_to_sql.cleaner as cleaner
import src.excel_to_sql.transformer as transformer
import src.excel_to_sql.validator as validator
import src.excel_to_sql.sql_writer as sql_writer


def run_etl(config: Dict[str, Any], context: Dict[str, Any]):
    """
    Run the ETL pipeline

    Args:
    config (dict[str, Any]): configuration loaded from the yaml file
    context (dict[str, Any]): run time context used by all units
    """
    logger = logging_setup.get_logger(context, __name__)
    logger.info(f"Start ETL pipeline for {config["file"]}")

    # get local from context
    locale = context["locale"]

    for table_name, mapping in config["mappings"].items():
        # Step 1: extract sheet
        df = extractor.load_excel(config["source"], table_name, mapping["sheet_name"], config["columns"][table_name], context)

        # Step 2: clean
        df = cleaner.clean_data(
            df, config["cleaning"], config["columns"][table_name], locale, context
        )

        # Step 3: transform
        df = transformer.apply_mappings(df, config["columns"][table_name], context)

        # Step 4: validate
        df = validator.validate_data(df, config["columns"][table_name], locale, context)

        # Step 5: load
        sql_writer.load_to_sql(df, config["target"], table_name, context)
