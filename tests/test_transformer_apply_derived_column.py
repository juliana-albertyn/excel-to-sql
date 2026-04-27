import pandas as pd
import pytest

from src.transformer import Transformer
from schemas.column_definitions import ColumnDefinition
from schemas.table_schema import TableSchema


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "a_cleaned": [1, 2, None, 4],
            "b_cleaned": [10, None, 30, 40],
            "result_cleaned": [None, None, None, None],
        }
    )


@pytest.fixture
def transformer(sample_df, project_config, etl_context):
    schema = TableSchema()
    schema.table_name = "Test"
    schema.columns = [
        ColumnDefinition(
            column_name="a", data_type="int", source_column="a", null_allowed=True
        ),
        ColumnDefinition(
            column_name="b", data_type="int", source_column="a", null_allowed=True
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
    project_config.source.header_rows = 1
    return Transformer(
        df=sample_df,
        project_config=project_config,
        table_schema=schema,
        etl_context=etl_context,
    )


def test_apply_derived_column_basic(transformer):
    df = transformer.df
    col = transformer.table_schema.columns[2]

    transformer._apply_derived_column(
        col_config=col,
        target_col="result_cleaned",
    )

    assert df.loc[0, "result_cleaned"] == 11
    assert df.loc[3, "result_cleaned"] == 44
    assert pd.isna(df.loc[1, "result_cleaned"])
    assert pd.isna(df.loc[2, "result_cleaned"])


def test_apply_derived_column_logs_invalid_rows(transformer, caplog):
    col = transformer.table_schema.columns[2]
    target_col = "result_cleaned"
    transformer._apply_derived_column(
        col_config=col,
        target_col=target_col,
    )

    assert f"Cannot compute '{target_col}' for rows: [2, 3]" in caplog.text
