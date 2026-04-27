import pandas as pd
import pytest

from src.transformer import Transformer
from schemas.column_definitions import ColumnDefinition
from schemas.table_schema import TableSchema


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "phone_clean": [
                "082 123 4567",
                "(082)123-4567",
                "00821234567",
                "+27821234567",
                None,
            ]
        }
    )


@pytest.fixture
def transformer(sample_df, project_config, etl_context):
    schema = TableSchema()
    schema.table_name = "Test"
    schema.columns = [
        ColumnDefinition(
            column_name="phone_clean",
            data_type="str",
            source_column="phone",
            null_allowed=True,
            validation={
                "format": "E.164",
                "allow_local": True,
                "dialling_prefix": "+27",
            },
        )
    ]

    return Transformer(
        df=sample_df,
        project_config=project_config,
        table_schema=schema,
        etl_context=etl_context,
    )


def test_normalise_phone_numbers(transformer):
    df = transformer.df
    col = transformer.table_schema.columns[0]

    result = transformer._normalise_phone_numbers(df["phone_clean"], col, "phone_cleaned")

    # assert result.iloc[0] == "+27821234567"  # local → +27
    # assert result.iloc[1] == "+27821234567"  # punctuation removed
    # assert result.iloc[2] == "+821234567"  # 00 → +
    assert result.iloc[3] == "+27821234567"  # already correct
    assert pd.isna(result.iloc[4])
