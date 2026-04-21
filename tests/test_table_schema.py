import pytest

from schemas.table_schema import TableSchema
from schemas.column_definitions import ColumnDefinition


@pytest.fixture
def base_col():
    """A minimal valid column definition."""
    return ColumnDefinition(
        column_name="id",
        source_column="id",
        data_type="int",
        primary_key=True,
        null_allowed=False,
    )


@pytest.fixture
def base_schema(base_col):
    """A minimal valid table schema."""
    return TableSchema(
        table_name="users",
        columns=[base_col],
        engine_type="mssql",
        has_primary_key=True,
        debug_trace=True,
    )


# ------------------------------------------------------------
# TABLE-LEVEL VALIDATION
# ------------------------------------------------------------


def test_missing_table_name(base_schema):
    base_schema.table_name = ""
    result = base_schema.validate("mssql")
    assert result.has_issues()
    assert "Table name not given" in {i.msg for i in result.issues}


def test_missing_engine_type(base_schema):
    result = base_schema.validate("")
    assert result.has_issues()
    assert "Engine type not given" in {i.msg for i in result.issues}


def test_missing_primary_key(base_schema):
    base_schema.has_primary_key = False
    result = base_schema.validate("mssql")
    assert result.has_issues()
    assert "Primary key for 'users' not given" in {i.msg for i in result.issues}


def test_missing_columns():
    schema = TableSchema(table_name="x", columns=[], engine_type="mssql")
    result = schema.validate("mssql")
    assert result.has_issues()
    assert "Column definitions not given" in {i.msg for i in result.issues}


# ------------------------------------------------------------
# COLUMN-LEVEL VALIDATION
# ------------------------------------------------------------


def test_missing_column_name(base_schema):
    base_schema.columns[0].column_name = ""
    result = base_schema.validate("mssql")
    assert "Column name not given" in {i.msg for i in result.issues}


def test_missing_source_column(base_schema):
    base_schema.columns[0].source_column = ""
    result = base_schema.validate("mssql")
    assert "source_column not given" in {i.msg for i in result.issues}


def test_missing_data_type(base_schema):
    base_schema.columns[0].data_type = ""
    result = base_schema.validate("mssql")
    assert "data_type not given" in {i.msg for i in result.issues}


# ------------------------------------------------------------
# LENGTH RULES
# ------------------------------------------------------------


def test_length_required_missing(base_schema):
    col = ColumnDefinition(
        column_name="name",
        source_column="name",
        data_type="varchar",
        max_length=None,
        null_allowed=True,
    )
    base_schema.columns = [col]
    result = base_schema.validate("mssql")
    msgs = {i.msg for i in result.issues}
    assert "requires a positive max_length" in next(iter(msgs))


def test_length_required_valid(base_schema):
    col = ColumnDefinition(
        column_name="name",
        source_column="name",
        data_type="varchar",
        max_length="50",
        null_allowed=True,
    )
    base_schema.columns = [col]
    result = base_schema.validate("mssql")
    assert not result.has_issues()


# ------------------------------------------------------------
# DATE RULES
# ------------------------------------------------------------


def test_date_requires_min_max(base_schema):
    col = ColumnDefinition(
        column_name="dob",
        source_column="dob",
        data_type="date",
        min_date=None,
        max_date=None,
        null_allowed=True,
    )
    base_schema.columns = [col]
    result = base_schema.validate("mssql")
    msgs = {i.msg for i in result.issues}
    assert "requires min_date and max_date" in next(iter(msgs))


def test_date_invalid_format(base_schema):
    col = ColumnDefinition(
        column_name="dob",
        source_column="dob",
        data_type="date",
        min_date="2020/01/01",
        max_date="2020-12-31",
        null_allowed=True,
    )
    base_schema.columns = [col]
    result = base_schema.validate("mssql")
    msgs = {i.msg for i in result.issues}
    assert "must be in YYYY-MM-DD format" in next(iter(msgs))


def test_date_min_greater_than_max(base_schema):
    col = ColumnDefinition(
        column_name="dob",
        source_column="dob",
        data_type="date",
        min_date="2021-01-01",
        max_date="2020-01-01",
        null_allowed=True,
    )
    base_schema.columns = [col]
    result = base_schema.validate("mssql")
    msgs = {i.msg for i in result.issues}
    assert "must be greater than min_date" in next(iter(msgs))


# ------------------------------------------------------------
# NUMERIC RULES
# ------------------------------------------------------------


def test_numeric_min_greater_than_max(base_schema):
    col = ColumnDefinition(
        column_name="age",
        source_column="age",
        data_type="int",
        min_value="10",
        max_value="5",
        null_allowed=True,
    )

    base_schema.columns = [col]
    result = base_schema.validate("mssql")
    msgs = {i.msg for i in result.issues}
    assert "must be greater than min_value" in next(iter(msgs))


# ------------------------------------------------------------
# FOREIGN KEY FORMAT RULES
# ------------------------------------------------------------


def test_foreign_key_invalid_format(base_schema):
    col = ColumnDefinition(
        column_name="dept_id",
        source_column="dept_id",
        data_type="int",
        foreign_key="departments",  # missing .column
        null_allowed=True,
    )
    base_schema.columns = [col]
    result = base_schema.validate("mssql")
    msgs = {i.msg for i in result.issues}
    assert "must be in the format table.column" in next(iter(msgs))


def test_foreign_key_valid_format(base_schema):
    col = ColumnDefinition(
        column_name="dept_id",
        source_column="dept_id",
        data_type="int",
        foreign_key="departments.id",
        null_allowed=True,
    )
    base_schema.columns = [col]
    result = base_schema.validate("mssql")
    assert not result.has_issues()


# ------------------------------------------------------------
# FEATURE RULES (value_mapping, validation.format, derived_from)
# ------------------------------------------------------------


def test_value_mapping_missing_list(base_schema):
    col = ColumnDefinition(
        column_name="status",
        source_column="status",
        data_type="varchar",
        max_length="10",
        value_mapping={"active": None},
        null_allowed=True,
    )
    base_schema.columns = [col]
    result = base_schema.validate("mssql")
    msgs = {i.msg for i in result.issues}
    assert "No value mappings for" in next(iter(msgs))


def test_validation_format_e164_missing_prefix(base_schema):
    col = ColumnDefinition(
        column_name="phone",
        source_column="phone",
        data_type="varchar",
        max_length="20",
        validation={"format": "E.164", "allow_local": "true"},
        null_allowed=True,
    )
    base_schema.columns = [col]
    result = base_schema.validate("mssql")
    msgs = {i.msg for i in result.issues}
    assert "Dialling prefix must be given" in next(iter(msgs))


def test_derived_column_missing_depends_on(base_schema):
    col = ColumnDefinition(
        column_name="full_name",
        source_column="full_name",
        data_type="varchar",
        max_length="50",
        validation={"derived_from": {"formula": "first + last"}},
        null_allowed=True,
    )
    base_schema.columns = [col]
    result = base_schema.validate("mssql")
    msgs = {i.msg for i in result.issues}
    assert "Formula and 'depends_on' must be given" in next(iter(msgs))
