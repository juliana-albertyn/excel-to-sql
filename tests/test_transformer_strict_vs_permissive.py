import pandas as pd
import pytest

from src.transformer import Transformer
from schemas.column_definitions import ColumnDefinition
from schemas.table_schema import TableSchema


@pytest.fixture
def sample_df():
    # row 1 has missing dependency → triggers invalid-row behaviour
    return pd.DataFrame(
        {
            "a": [1, None, 4],
            "b": [10, 20, None],
            "result": [None, None, None],
            "a_cleaned": [1, None, 4],
            "b_cleaned": [10, 20, None],
            "result_cleaned": [None, None, None],
        }
    )


@pytest.fixture
def table_schema():
    schema = TableSchema()
    schema.table_name = "Test"
    schema.columns = [
        ColumnDefinition(
            column_name="a", source_column="a", null_allowed=True, data_type="int"
        ),
        ColumnDefinition(
            column_name="b", source_column="b", null_allowed=True, data_type="int"
        ),
        ColumnDefinition(
            column_name="result",
            data_type="int",
            source_column="result",
            null_allowed=True,
            validation={
                "derived_from": {
                    "depends_on": ["a", "b"],
                    "formula": "a + b",
                }
            },
        ),
    ]
    return schema


def test_transform_data_strict_mode(
    sample_df, project_config_strict, etl_context, table_schema, caplog
):
    transformer = Transformer(
        df=sample_df.copy(),
        project_config=project_config_strict,
        table_schema=table_schema,
        etl_context=etl_context,
    )

    df = transformer.transform_data()

    # Valid row
    assert df.loc[0, "result_cleaned"] == 11

    # Invalid rows → NA
    assert pd.isna(df.loc[1, "result_cleaned"])
    assert pd.isna(df.loc[2, "result_cleaned"])

    # Strict mode should still log invalid rows
    assert "Cannot compute 'result_cleaned' for rows:" in caplog.text


def test_transform_data_permissive_mode(
    sample_df, project_config_permissive, etl_context, table_schema, caplog
):
    transformer = Transformer(
        df=sample_df.copy(),
        project_config=project_config_permissive,
        table_schema=table_schema,
        etl_context=etl_context,
    )

    df = transformer.transform_data()

    # Valid row
    assert df.loc[0, "result_cleaned"] == 11

    # Invalid rows → NA
    assert pd.isna(df.loc[1, "result_cleaned"])
    assert pd.isna(df.loc[2, "result_cleaned"])

    # Permissive mode also logs invalid rows (same behaviour)
    assert "Cannot compute 'result_cleaned' for rows:" in caplog.text

    # Crucially: permissive mode must NOT raise exceptions
    # (pytest would fail the test automatically if an exception was raised)
