"""
Logging utilities for the Fynbyte Excel-to-SQL pipeline.

Creates timestamped loggers with file and console handlers, applies
pipeline-wide log settings, and automatically prunes old log files.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"

import logging
import os
from glob import glob

import src.excel_to_sql.context as context


def get_logger(etl_context: context.ETLContext, name: str) -> logging.Logger:
    """
    Return a configured logger for the given module.

    Creates timestamped file and console handlers, applies log level and
    format settings from ETLContext, and removes older log files when the
    limit is exceeded.
    """    
    # get the last part of the name
    words = name.split(".")
    if len(words) > 1:
        name = words[-1]

    # Log configuration
    log_dir = etl_context.log_dir
    os.makedirs(log_dir, exist_ok=True)
    log_level = etl_context.log_level
    log_format = "%(asctime)s [%(levelname)s]: %(message)s"

    max_logs = etl_context.max_logs

    # Timestamped log file
    timestamp = etl_context.run_timestamp
    log_file_name = f"{name}_{timestamp}.log"
    log_file = os.path.join(log_dir, log_file_name)

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # Avoid adding handlers multiple times
    if not logger.handlers:
        formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")

        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # Clean up old logs if exceeding max_logs
    old_logs = sorted(glob(os.path.join(log_dir, f"{name}_*.log")))
    if len(old_logs) > max_logs:
        for old_log in old_logs[:-max_logs]:
            os.remove(old_log)

    return logger


def shutdown():
    """
    Flush and close all logging handlers.
    """    
    logging.shutdown()
