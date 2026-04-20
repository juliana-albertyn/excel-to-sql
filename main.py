"""
Entry point for the Fynbyte Excel-to-SQL pipeline.

Initialises and runs the full ETL process by invoking pipeline.run_etl().
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-17"

from src import pipeline

pipeline.run_etl()
