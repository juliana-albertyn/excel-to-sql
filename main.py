"""
Module: main
Purpose: Call pipeline

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-17"

from src.excel_to_sql import pipeline

pipeline.run_etl()
