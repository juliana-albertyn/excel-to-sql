import pandas as pd
from transformer import Transformer
from schemas.column_definitions import ColumnDefinition
from schemas.table_schema import TableSchema


def test_apply_value_mapping(project_config, etl_context):
    """
    Verify that _apply_value_mapping:
    - normalises case and whitespace
    - maps raw values to canonical values
    - sets unmapped values to NA
    - records the cleaned column in normalised_cols
    """

    # --- Arrange -------------------------------------------------------------

    # simple mapping: "yes" and "y" → "YES"; "no" and "n" → "NO"
    col = ColumnDefinition(
        column_name="Active",
        source_column="Active",
        data_type="str",
        null_allowed=True,
        value_mapping={
            "YES": ["yes", " y ", "Y"],
            "NO": ["no", " n", "N "],
        },
    )

    schema = TableSchema()
    schema.table_name = "Test"
    schema.columns = [col]

    cleaned_col = f"Active{etl_context.cleaned_suffix}"

    df = pd.DataFrame(
        {
            cleaned_col: [
                " yes ",
                "Y",
                " n ",
                "NO",
                "maybe",  # unmapped
                None,  # NA stays NA
            ]
        }
    )

    transformer = Transformer(
        df=df,
        project_config=project_config,
        table_schema=schema,
        etl_context=etl_context,
        debug_trace=True,
    )

    # --- Act ----------------------------------------------------------------
    result = transformer._apply_value_mapping(df[cleaned_col], col, cleaned_col)

    # --- Assert --------------------------------------------------------------

    expected = pd.Series(
        ["YES", "YES", "NO", "NO", None, None], name=cleaned_col, dtype="object"
    )

    result = result.astype("string")
    expected = expected.astype("string")
    pd.testing.assert_series_equal(result, expected)

    # ensure the column was marked as normalised
    assert cleaned_col in transformer.normalised_cols
