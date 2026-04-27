import pandas as pd
import pytest

from src.cleaner import Cleaner
from src.errors import CleanerError
from schemas.table_schema import TableSchema
from schemas.column_definitions import ColumnDefinition
from config.project_config import ProjectConfig
from src.context import ETLContext


# ---------------------------------------------------------------------
# helpers
#
# ---------------------------------------------------------------------
def table_schema_with_col(col_name, data_type):
    # table schema with specific column
    schema = TableSchema()
    schema.table_name = "TestTable"
    schema.columns = [
        ColumnDefinition(
            column_name=col_name,
            source_column=col_name,
            data_type=data_type,
            null_allowed=True,
        ),
    ]
    return schema


# ----------------------------------------------------------------------
# 2. Date parsing strict mode
# ----------------------------------------------------------------------


def test_col_to_date_strict_raises(
    project_config_strict: ProjectConfig, etl_context: ETLContext
):
    df = pd.DataFrame({"d": ["2024-01-01", "not-a-date"]})
    schema = table_schema_with_col("d", "date")

    cleaner = Cleaner(df, project_config_strict, schema, etl_context)

    with pytest.raises(CleanerError) as exc:
        cleaner._col_to_date(df["d"], schema.columns[0])

    assert "Invalid date" in str(exc.value)
    assert "row" in str(exc.value)


# ----------------------------------------------------------------------
# 3. Date parsing permissive mode
# ----------------------------------------------------------------------


def test_col_to_date_permissive(
    project_config_permissive: ProjectConfig, etl_context: ETLContext
):
    df = pd.DataFrame({"d": ["2024-01-01", "not-a-date"]})
    schema = table_schema_with_col("d", "date")

    cleaner = Cleaner(df, project_config_permissive, schema, etl_context)
    out = cleaner._col_to_date(df["d"], schema.columns[0])

    assert pd.isna(out.iloc[1])  # invalid coerced to NaT
    assert pd.to_datetime("2024-01-01") == out.iloc[0]


# ----------------------------------------------------------------------
# 4. Integer strict mode
# ----------------------------------------------------------------------


def test_col_to_int_strict_raises(
    project_config_strict: ProjectConfig, etl_context: ETLContext
):
    df = pd.DataFrame({"n": ["10", "x"]})
    schema = table_schema_with_col("n", "int")

    cleaner = Cleaner(df, project_config_strict, schema, etl_context)

    with pytest.raises(CleanerError):
        cleaner._col_to_int(df["n"], schema.columns[0])


# ----------------------------------------------------------------------
# 5. Integer permissive mode
# ----------------------------------------------------------------------


def test_col_to_int_permissive(
    project_config_permissive: ProjectConfig, etl_context: ETLContext
):
    df = pd.DataFrame({"n": ["10", "x"]})
    schema = table_schema_with_col("n", "int")

    cleaner = Cleaner(df, project_config_permissive, schema, etl_context)
    out = cleaner._col_to_int(df["n"], schema.columns[0])

    assert out.iloc[0] == 10
    assert pd.isna(out.iloc[1])


# ----------------------------------------------------------------------
# 6. Money strict mode
# ----------------------------------------------------------------------


def test_col_to_money_strict_raises(
    project_config_strict: ProjectConfig, etl_context: ETLContext
):
    df = pd.DataFrame({"m": ["R100", "Rxx"]})
    schema = table_schema_with_col("m", "money")

    cleaner = Cleaner(df, project_config_strict, schema, etl_context)

    with pytest.raises(CleanerError):
        cleaner._col_to_money(df["m"], schema.columns[0])


# ----------------------------------------------------------------------
# 7. Binary strict mode
# ----------------------------------------------------------------------


def test_col_to_binary_strict_raises(
    project_config_strict: ProjectConfig, etl_context: ETLContext
):
    df = pd.DataFrame({"b": ["ZZZZ"]})
    schema = table_schema_with_col("b", "binary")

    cleaner = Cleaner(df, project_config_strict, schema, etl_context)

    with pytest.raises(CleanerError):
        cleaner._col_to_binary(df["b"], schema.columns[0])


# ----------------------------------------------------------------------
# 8. Image strict mode
# ----------------------------------------------------------------------


def test_col_to_image_valid_jpeg(
    project_config_strict: ProjectConfig, etl_context: ETLContext
):
    jpeg_hex = "FFD8FFAA11"  # valid JPEG magic bytes
    df = pd.DataFrame({"img": [jpeg_hex]})

    schema = table_schema_with_col("img", "image")

    cleaner = Cleaner(df, project_config_strict, schema, etl_context)
    out = cleaner._col_to_image(df["img"], schema.columns[0])

    assert isinstance(out.iloc[0], bytes)
    assert out.iloc[0].startswith(bytes.fromhex("FFD8FF"))


# ----------------------------------------------------------------------
# 9. End-to-end clean_data
# ----------------------------------------------------------------------


def test_clean_data_end_to_end(
    project_config_permissive: ProjectConfig, etl_context: ETLContext
):
    df = pd.DataFrame({"name": [" Alice ", "Bob"], "age": ["10", "20"]})

    schema = TableSchema(
        table_name="People",
        columns=[
            ColumnDefinition(
                column_name="name",
                source_column="name",
                data_type="varchar",
                null_allowed=True,
            ),
            ColumnDefinition(
                column_name="age",
                source_column="age",
                data_type="int",
                null_allowed=True,
            ),
        ],
    )

    cleaner = Cleaner(df, project_config_permissive, schema, etl_context)
    out = cleaner.clean_data()

    assert "name_cleaned" in out.columns
    assert "age_cleaned" in out.columns
    assert out["name_cleaned"].iloc[0] == "alice" or out["name_cleaned"].iloc[0] == "Alice"
    assert out["age_cleaned"].iloc[0] == 10
