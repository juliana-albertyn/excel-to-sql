import pandas as pd
import pytest

from src.transformer import Transformer
from schemas.column_definitions import ColumnDefinition
from schemas.table_schema import TableSchema


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "phone": ["082 123 4567", "+27821234567"],
            "status": ["yes", "NO"],
            "result": [None, None],
            "a": [1, 4],
            "b": [10, 40],
            "phone_cleaned": ["082 123 4567", "+27821234567"],
            "status_cleaned": ["yes", "NO"],
            "result_cleaned": [None, None],
            "a_cleaned": [1, 4],
            "b_cleaned": [10, 40],
        }
    )


@pytest.fixture
def transformer(sample_df, project_config, etl_context):
    schema = TableSchema()
    schema.table_name = "Test"
    schema.columns = [
        ColumnDefinition(
            column_name="phone",
            data_type="str",
            source_column="phone",
            null_allowed=True,
            validation={
                "format": "E.164",
                "allow_local": True,
                "dialling_prefix": "+27",
            },
        ),
        ColumnDefinition(
            column_name="status",
            data_type="str",
            source_column="status",
            null_allowed=True,
            value_mapping={
                "Y": ["yes", "YES", "Yes"],
                "N": ["no", "NO", "No"],
            },
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
        ColumnDefinition(
            column_name="phone_cleaned",
            data_type="str",
            source_column="phone_cleaned",
            null_allowed=True,
            validation={
                "format": "E.164",
                "allow_local": True,
                "dialling_prefix": "+27",
            },
        ),
        ColumnDefinition(
            column_name="status_cleaned",
            data_type="str",
            source_column="status_cleaned",
            null_allowed=True,
            value_mapping={
                "Y": ["yes", "YES", "Yes"],
                "N": ["no", "NO", "No"],
            },
        ),
        ColumnDefinition(
            column_name="result_cleaned",
            data_type="int",
            source_column="result_cleaned",
            null_allowed=True,
            validation={
                "derived_from": {
                    "depends_on": ["a", "b"],
                    "formula": "a + b",
                }
            },
        ),
    ]

    return Transformer(
        df=sample_df.copy(),
        project_config=project_config,
        table_schema=schema,
        etl_context=etl_context,
        debug_trace=True,
    )


def test_transform_data_end_to_end(transformer):
    df = transformer.transform_data()

    print(f"{df.loc[0, 'status_cleaned']}")
    print(f"{df.loc[1, 'status_cleaned']}")

    # --- PHONE NORMALISATION ---
    assert df.loc[0, "phone_cleaned"] == "+27821234567"
    assert df.loc[1, "phone_cleaned"] == "+27821234567"

    # --- DERIVED COLUMN ---
    assert df.loc[0, "result_cleaned"] == 11  # 1 + 10
    assert df.loc[1, "result_cleaned"] == 44  # 4 + 40

    # --- VALUE MAPPING ---
    assert df.loc[0, "status_cleaned"] == "Y"
    assert df.loc[1, "status_cleaned"] == "N"
