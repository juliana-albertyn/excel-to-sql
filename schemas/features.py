"""
Module: features
Purpose: Rules for features in YAML schema

This module is part of the Fynbyte toolkit.
"""

from __future__ import annotations

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-04-13"

from typing import TYPE_CHECKING

import utils.helpers as helpers

if TYPE_CHECKING:
    from schemas.column_definitions import ColumnDefinition
    from schemas.table_schema import SchemaValidationResult


def validate_value_mapping(
    col: ColumnDefinition, result: SchemaValidationResult, table_name: str
):
    value_mapping = getattr(col, "value_mapping", None)
    if value_mapping is None:
        return

    for normalised, raw_list in value_mapping.items():
        if raw_list is None:
            result.add_issue(
                table_name, col.column_name, f"No value mappings for '{normalised}'"
            )


def validate_format_rules(
    col: ColumnDefinition, result: SchemaValidationResult, table_name: str
):
    validation = getattr(col, "validation", None)
    if not validation:
        return

    fmt = validation.get("format")
    if fmt != "E.164":
        return

    allow_local = helpers.str_to_bool(validation.get("allow_local"))
    if allow_local:
        prefix = validation.get("dialling_prefix")
        if prefix is None:
            result.add_issue(
                table_name,
                col.column_name,
                "Dialling prefix must be given if 'allow_local' is true",
            )
        elif not prefix.startswith("+"):
            result.add_issue(
                table_name,
                col.column_name,
                "Country dialling prefix must start with '+'",
            )


def validate_derived_column(
    col: ColumnDefinition,
    result: SchemaValidationResult,
    table_name: str,
    column_names: list[str],
):
    validation = getattr(col, "validation", None)
    if not validation:
        return

    derived = validation.get("derived_from")
    if derived is None:
        return

    depends_on = derived.get("depends_on")
    formula = derived.get("formula")

    if not formula or not depends_on:
        result.add_issue(
            table_name,
            col.column_name,
            "Formula and 'depends_on' must be given for derived columns",
        )
        return

    for dep in depends_on:
        if dep not in column_names:
            result.add_issue(
                table_name,
                col.column_name,
                f"Column '{dep}' used by formula '{formula}' is missing",
            )


def validate_foreign_key_format(
    col: ColumnDefinition, result: SchemaValidationResult, table_name: str
) -> None:
    fk = col.foreign_key
    if not fk:
        return

    if fk.count(".") != 1:
        result.add_issue(
            table_name,
            col.column_name,
            f"Foreign key '{fk}' must be in the format table.column",
        )
        return

    table, column = fk.split(".", 1)

    if not table or not column:
        result.add_issue(
            table_name,
            col.column_name,
            f"Foreign key '{fk}' must specify both table and column",
        )


FEATURE_DISPATCH = {
    "value_mapping": validate_value_mapping,
    "validation_format": validate_format_rules,
    "derived_from": validate_derived_column,
    "foreign_key": validate_foreign_key_format,
}
