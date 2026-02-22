"""
Module: main
Purpose: Load project configuration and calls pipeline

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-17"

import yaml
from pathlib import Path
from datetime import datetime

from src.excel_to_sql import pipeline
from src.excel_to_sql import logging_setup

# resolve directories
project_root = Path(__file__).resolve().parent
config_file = project_root / "config/pipeline_config.yaml"
log_dir = project_root / "logs/"

with open(config_file) as f:
    cfg = yaml.safe_load(f)

# runtime context
# Get current datetime once
now = datetime.now()

context = {
    "project_name": cfg.get("project_name", "excel_to_sql"),
    "locale": cfg["source"].get("localisation", "en_ZA"),
    "environment": "development",
    "data_dir": Path(project_root / cfg.get("data_dir", "")),
    "log_dir": log_dir,
    "log_level": "INFO",
    "max_logs": 5,  # keep last 5 log files per module
    "run_date": now.date(),  # datetime.date object
    "run_timestamp": now.strftime(
        "%Y-%m-%d_%H-%M-%S"
    ),  # all log files for the same run have the same timestamp
}

# get logger
logger = logging_setup.get_logger(context, "main")
logger.info("Loading ETL pipeline configuration")

# load configuration

source_config = cfg["source"]
cleaning_rules = cfg["cleaning"]
mapping_config = cfg["mappings"]
columns_config = cfg["columns"]
target_config = cfg["target"]

etl_config = {
    "source": source_config,
    "cleaning": cleaning_rules,
    "mappings": mapping_config,
    "columns": columns_config,
    "target": target_config,
}

# Pass the configuration object and context to pipeline
try:
    pipeline.run_etl(etl_config, context)
    logger.info("ETL pipeline finished successfully")
except Exception as e:
    print(f"ETL pipeline error: {e}. Check log files")
