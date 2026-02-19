"""
Module: logging_setup
Purpose: Configurable logger

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"

import logging
import os
from datetime import datetime
from typing import Dict
from glob import glob


def get_logger(context: Dict, name: str) -> logging.Logger:
    """
    Returns a logger configured with timestamped file and console handlers.
    Automatically keeps only the last N log files per module.

    Args:
        context (dict): Should contain 'log_level', 'log_format', 'log_dir', 'max_logs' (optional).
        name (str): Logger name (typically module name).

    Returns:
        logging.Logger: Configured logger.
    """
    # Log configuration
    log_dir = context.get("log_dir", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_level = context.get("log_level", "INFO")
    log_format = context.get(
        "log_format", "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    max_logs = context.get("max_logs", 10)  # keep last 10 logs by default

    # Timestamped log file
    timestamp = context.get(
        "run_timestamp", datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    )
    log_file_name = f"{name}_{timestamp}.log"
    log_file = os.path.join(log_dir, log_file_name)

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # Avoid adding handlers multiple times
    if not logger.handlers:
        formatter = logging.Formatter(log_format)

        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # Clean up old logs if exceeding max_logs
    old_logs = sorted(glob(os.path.join(log_dir, f"{name}_*.log")))
    if len(old_logs) > max_logs:
        for old_log in old_logs[:-max_logs]:
            os.remove(old_log)

    return logger
