"""
Module: column_definitions
Purpose:

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-04-02"

from dataclasses import dataclass
from typing import Any, Literal


@dataclass
class BaseColumn:
    column_name: str
    source_column: str
    data_type: str
    null_allowed: bool

    def __post_init__(self):
        if not self.source_column:
            raise ValueError("column_name is required")

        if not self.source_column:
            raise ValueError("source_column is required")

        if not self.data_type:
            raise ValueError("data_type is required")

        if self.null_allowed is None:
            raise ValueError("null_allowed must be True or False")


@dataclass
class ColumnDefinition(BaseColumn):
    primary_key: bool = False

    max_length: str | None = None
    precision: str | None = None
    scale: str | None = None

    min_value: str | None = None
    max_value: str | None = None

    min_date: str | None = None
    max_date: str | None = None

    str_case: Literal["lower", "upper", "title", "asis"] = "asis"

    value_mapping: dict[str, Any] | None = None

    validation: dict[str, Any] | None = None

    foreign_key: str | None = None
