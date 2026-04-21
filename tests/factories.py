# tests/factories.py

from __future__ import annotations
from typing import TYPE_CHECKING, Any, Callable

import pytest

if TYPE_CHECKING:
    from schemas.column_definitions import ColumnDefinition
    from schemas.table_schema import TableSchema


@pytest.fixture
def make_column() -> Callable[..., ColumnDefinition]:
    """
    Factory for ColumnDefinition.
    Allows overriding any field while providing safe defaults.
    """
    from schemas.column_definitions import ColumnDefinition

    def _factory(**overrides: Any) -> ColumnDefinition:
        defaults = dict(
            column_name="col",
            source_column="col",
            data_type="int",
            primary_key=False,
            max_length=None,
            precision=None,
            scale=None,
            min_value=None,
            max_value=None,
            min_date=None,
            max_date=None,
            value_mapping=None,
            validation=None,
            foreign_key=None,
        )
        defaults.update(overrides)
        return ColumnDefinition(**defaults)

    return _factory


@pytest.fixture
def make_schema(make_column) -> Callable[..., TableSchema]:
    """
    Factory for TableSchema.
    Allows overriding table_name, engine_type, columns, and flags.
    """
    from schemas.table_schema import TableSchema

    def _factory(**overrides: Any) -> TableSchema:
        defaults = dict(
            table_name="test_table",
            engine_type="mssql",
            has_primary_key=True,
            columns=[make_column(column_name="id", source_column="id", data_type="int", primary_key=True)],
        )
        defaults.update(overrides)
        return TableSchema(**defaults)

    return _factory