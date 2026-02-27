"""
Module: main
Purpose: Call pipeline

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-17"

import yaml
from pathlib import Path
from datetime import datetime

from src.excel_to_sql import pipeline

try:
    pipeline.run_etl()
except Exception as e:
    print(f"ETL pipeline error: {e}. Check log files")
