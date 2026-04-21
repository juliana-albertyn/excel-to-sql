import pandas as pd
import pytest

import src.errors as errors


# --------------------------------------------------------------------
# FIXTURE: mock pandas.ExcelFile and pandas.read_excel
# --------------------------------------------------------------------
@pytest.fixture
def mock_excel(monkeypatch):
    """Mock ExcelFile and read_excel so no real file is needed."""

    class FakeExcelFile:
        sheet_names = ["Sheet1"]

    def fake_excel_file(path):
        return FakeExcelFile()

    def fake_read_excel(*args, **kwargs):
        # Simulate a simple Excel sheet with two columns
        return pd.DataFrame(
            {
                "full_name": ["Alice", "Bob"],
                "email_address": ["a@example.com", "b@example.com"],
                "mobile_number": ["1234567", "2345678"],
            }
        )

    monkeypatch.setattr("extractor.pd.ExcelFile", fake_excel_file)
    monkeypatch.setattr("extractor.pd.read_excel", fake_read_excel)


# --------------------------------------------------------------------
# TEST: successful load + rename
# --------------------------------------------------------------------
def test_load_excel_success(mock_excel, extractor, table_schema):

    df = extractor.from_excel(
        table_name="People", sheet_name="Sheet1", table_schema=table_schema
    )

    expected = ["Name", "Email"]
    actual = list(df.columns)
    assert set(expected).issubset(actual)  # strict_validation = False
    assert df.iloc[0]["Name"] == "Alice"
    assert df.iloc[0]["Email"] == "a@example.com"


# --------------------------------------------------------------------
# TEST: missing sheet triggers ExtractorError
# --------------------------------------------------------------------
def test_missing_sheet_raises(mock_excel, monkeypatch, extractor, table_schema):
    # Override sheet list to simulate missing sheet
    class FakeExcelFile:
        sheet_names = ["OtherSheet"]

    monkeypatch.setattr("extractor.pd.ExcelFile", lambda path: FakeExcelFile())

    with pytest.raises(errors.ExtractorError) as exc:
        extractor.from_excel(
            table_name="People",
            sheet_name="Sheet1",
            table_schema=table_schema,
        )

    assert "Worksheet 'Sheet1' not found" in str(exc.value)


# --------------------------------------------------------------------
# TEST: unexpected columns triggers ExtractorError when strict_validation=True
# --------------------------------------------------------------------
def test_unexpected_columns_strict(mock_excel, extractor, project_config, table_schema):

    project_config.runtime.strict_validation = True

    with pytest.raises(errors.ExtractorError) as exc:
        extractor.from_excel(
            table_name="People",
            sheet_name="Sheet1",
            table_schema=table_schema,
        )

    assert "Unexpected column names" in str(exc.value)
