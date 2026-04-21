"""
Module: helpers
Purpose:

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-04-02"

from typing import Any, Optional
from datetime import datetime
import time
from logging import Logger


class Timer:
    """
    Simple context manager for timing code blocks.
    Logs duration using the provided logger when debug_trace is True.
    """

    def __init__(self, label: str, logger: Optional[Logger], debug_trace: bool):
        self.label = label
        self.logger = logger
        self.debug_trace = debug_trace

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        duration = (time.perf_counter() - self.start) * 1000
        if self.debug_trace and self.logger:
            self.logger.debug(f"[TIME] {self.label}: {duration:.2f} ms")


def str_to_bool(value: str | bool) -> bool:
    """
    Convert common truthy strings to a boolean.

    Accepts 'true', 'yes', '1' (case-insensitive). Falls back to Python
    truthiness for non-string values.
    """
    if isinstance(value, str):
        return value.strip().lower() in ("true", "yes", "1")
    return bool(value)  # fallback for non-strings


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


def is_valid_yyyy_mm_dd(value: str) -> bool:
    """
    Return True if value is in strict YYYY-MM-DD format.
    Returns False on invalid format or correct format, invalid date
    Rejects non-strings
    """
    if not value or not isinstance(value, str):
        return False

    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def is_valid_int(value: str) -> bool:
    return value.isdigit() or (value.startswith("-") and value[1:].isdigit())


def is_valid_float(value: str) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False
