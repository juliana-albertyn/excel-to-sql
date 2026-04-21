"""
Module: table_schema
Purpose:

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-04-02"

import yaml
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime

import utils.helpers as helpers

from schemas.column_definitions import ColumnDefinition
from schemas.data_types import is_integer_type, DATA_TYPE_RULES
from schemas.features import FEATURE_DISPATCH


@dataclass
class SchemaIssue:
    """Data Class to describe a schema issue"""

    table_name: str = ""
    col_name: str = ""
    msg: str = ""

    def __str__(self) -> str:
        return f"{self.table_name}-{self.col_name}: {self.msg}"


class SchemaValidationResult:
    """Class to store the schema validation results"""

    def __init__(self) -> None:
        """Initialise an instance of SchemaValidationResult"""
        self.issues: list[SchemaIssue] = []

    def __str__(self) -> str:
        result: str = ""
        for i in self.issues:
            result += "\n" + str(i)
        return result

    def add_issue(self, table_name: str, col_name: str, msg: str) -> None:
        """Add a SchemaIssue to the list of results"""
        issue = SchemaIssue(table_name=table_name, col_name=col_name, msg=msg)
        self.issues.append(issue)

    def has_issues(self) -> bool:
        """Check if there are issues"""
        return len(self.issues) > 0

    def issue_count(self) -> int:
        """Return the number of issues"""
        return len(self.issues)


@dataclass
class TableSchema:
    table_name: str = ""
    columns: list[ColumnDefinition] = field(default_factory=list)
    engine_type: str = ""
    has_primary_key: bool = False
    debug_trace: bool = False

    # Debug helpers
    def _trace(self, msg: str) -> None:
        """
        Emit a debug trace message when debug tracing is enabled.
        """
        if self.debug_trace:
            print(f"[TRACE] {msg}")

    def __repr__(self) -> str:
        return f"table_name: {self.table_name}\ncolumns: {self.columns}\nengine_type: {self.engine_type}"

    @classmethod
    def from_yaml(cls, path: Path) -> "TableSchema":
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        raw = helpers.lower_keys(raw)
        columns = []
        has_primary_key = False
        table_name = raw.get("table_name", "")
        column_config = raw.get("columns", [])
        if table_name != "" and len(column_config) != 0:
            # this is the minimum we need to continue
            # get all the information that is available, validation will do the checking
            for col in column_config:
                column_name = col.get("column_name", "")
                source_column = col.get("source_column", "")
                data_type = col.get("data_type", "")
                null_allowed = col.get("null_allowed", True)
                col_definition = ColumnDefinition(
                    column_name=column_name,
                    source_column=source_column,
                    data_type=data_type,
                    null_allowed=null_allowed,
                )
                if col.get("primary_key") is not None:
                    col_definition.primary_key = helpers.str_to_bool(
                        col.get("primary_key")
                    )
                    has_primary_key |= col_definition.primary_key
                col_definition.max_length = col.get("max_length")
                col_definition.precision = col.get("precision")
                col_definition.scale = col.get("scale")
                col_definition.min_value = col.get("min_value")
                col_definition.max_value = col.get("max_value")
                col_definition.min_date = col.get("min_date")
                col_definition.max_date = col.get("max_date")
                col_definition.foreign_key = col.get("foreign_key")
                col_definition.str_case = col.get("str_case", "asis")

                columns.append(col_definition)
        return cls(
            table_name=table_name, has_primary_key=has_primary_key, columns=columns
        )

    def validate(self, engine_type: str = "") -> SchemaValidationResult:
        result = SchemaValidationResult()

        # Basic table-level checks
        if not self.table_name:
            result.add_issue("", "", "Table name not given")
        if not engine_type:
            result.add_issue(self.table_name, "", "Engine type not given")
        if not self.has_primary_key:
            result.add_issue(
                self.table_name, "", f"Primary key for '{self.table_name}' not given"
            )
        if not self.columns:
            result.add_issue(self.table_name, "", "Column definitions not given")
            return result

        for col in self.columns:
            dtype = (col.data_type or "").split("(", 1)[0].lower()
            rules = DATA_TYPE_RULES.get(dtype, {})

            # --- Column name / source column ---
            if not col.column_name:
                result.add_issue(self.table_name, "", "Column name not given")
            if not col.source_column:
                result.add_issue(self.table_name, "", "source_column not given")

            # --- Data type missing ---
            if not col.data_type:
                result.add_issue(
                    self.table_name, col.column_name, "data_type not given"
                )
                continue

            # --- Length validation ---
            if rules.get("requires_length"):
                if (
                    not col.max_length
                    or not col.max_length.isdigit()
                    or int(col.max_length) <= 0
                ):
                    result.add_issue(
                        self.table_name,
                        col.column_name,
                        f"Data type '{dtype}' requires a positive max_length",
                    )

            # --- Date validation ---
            if rules.get("requires_min_max_date"):
                if not col.min_date or not col.max_date:
                    result.add_issue(
                        self.table_name,
                        col.column_name,
                        f"Date/time type '{dtype}' requires min_date and max_date",
                    )
                else:
                    min_ok = helpers.is_valid_yyyy_mm_dd(col.min_date)
                    max_ok = helpers.is_valid_yyyy_mm_dd(col.max_date)

                    if not min_ok:
                        result.add_issue(
                            self.table_name,
                            col.column_name,
                            f"min_date '{col.min_date}' must be in YYYY-MM-DD format",
                        )
                    if not max_ok:
                        result.add_issue(
                            self.table_name,
                            col.column_name,
                            f"max_date '{col.max_date}' must be in YYYY-MM-DD format",
                        )

                    if min_ok and max_ok:
                        dmin = datetime.strptime(col.min_date, "%Y-%m-%d")
                        dmax = datetime.strptime(col.max_date, "%Y-%m-%d")
                        if dmin > dmax:
                            result.add_issue(
                                self.table_name,
                                col.column_name,
                                f"max_date '{col.max_date}' must be greater than min_date '{col.min_date}'",
                            )

            # --- Numeric validation (optional min/max) ---
            if rules.get("validate_numeric"):
                min_valid = True
                max_valid = True

                # min_value
                if col.min_value is not None:
                    if is_integer_type(engine_type, dtype):
                        if not helpers.is_valid_int(col.min_value):
                            min_valid = False
                            result.add_issue(
                                self.table_name,
                                col.column_name,
                                f"min_value '{col.min_value}' must be an integer",
                            )
                    else:
                        if not helpers.is_valid_float(col.min_value):
                            min_valid = False
                            result.add_issue(
                                self.table_name,
                                col.column_name,
                                f"min_value '{col.min_value}' must be numeric",
                            )

                # max_value
                if col.max_value is not None:
                    if is_integer_type(engine_type, dtype):
                        if not helpers.is_valid_int(col.max_value):
                            max_valid = False
                            result.add_issue(
                                self.table_name,
                                col.column_name,
                                f"max_value '{col.max_value}' must be an integer",
                            )
                    else:
                        if not helpers.is_valid_float(col.max_value):
                            max_valid = False
                            result.add_issue(
                                self.table_name,
                                col.column_name,
                                f"max_value '{col.max_value}' must be numeric",
                            )

                # ordering (only if both provided and valid)
                if (
                    col.min_value is not None
                    and col.max_value is not None
                    and min_valid
                    and max_valid
                ):
                    if float(col.min_value) > float(col.max_value):
                        result.add_issue(
                            self.table_name,
                            col.column_name,
                            f"max_value '{col.max_value}' must be greater than min_value '{col.min_value}'",
                        )

            # --- Feature-driven rules ---
            if col.value_mapping is not None:
                FEATURE_DISPATCH["value_mapping"](col, result, self.table_name)

            if col.validation is not None:
                fmt = col.validation.get("format")

                if fmt:
                    FEATURE_DISPATCH["validation_format"](col, result, self.table_name)

                if col.validation.get("derived_from"):
                    column_names = {c.column_name for c in self.columns}
                    FEATURE_DISPATCH["derived_from"](
                        col, result, self.table_name, column_names
                    )
            if col.foreign_key:
                FEATURE_DISPATCH["foreign_key"](col, result, self.table_name)

        return result
