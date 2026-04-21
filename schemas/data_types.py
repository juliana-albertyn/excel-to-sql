"""
Module: data_types
Purpose:

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-04-02"


VALID_DATA_TYPES = {
    "mssql": [
        "char",
        "varchar",
        "text",
        "nchar",
        "nvarchar",
        "ntext",
        "tinyint",
        "smallint",
        "int",
        "bigint",
        "bit",
        "decimal",
        "numeric",
        "money",
        "smallmoney",
        "float",
        "real",
        "date",
        "time",
        "datetime",
        "datetime2",
        "smalldatetime",
        "binary",
        "varbinary",
    ],
    "postgres": [
        "smallint",
        "integer",
        "bigint",
        "decimal",
        "numeric",
        "real",
        "double precision",
        "serial",
        "bigserial",
        "boolean",
        "char",
        "varchar",
        "text",
        "date",
        "time",
        "timestamp",
        "timestamptz",
        "bytea",
        "json",
        "jsonb",
    ],
    "sqlite": [
        "integer",
        "real",
        "text",
        "blob",
        "numeric",
    ],
}

LENGTH_REQUIRED = {
    "mssql": {"char", "varchar", "nchar", "nvarchar"},
    "postgres": {"char", "varchar"},
    "sqlite": set(),  # SQLite ignores length entirely
}

DATE_TIME_TYPES = {
    "mssql": {
        "date",
        "time",
        "datetime",
        "datetime2",
        "smalldatetime",
    },
    "postgres": {
        "date",
        "time",
        "timestamp",
        "timestamptz",
    },
    "sqlite": {
        "date",
        "time",
        "datetime",
    },
}

INTEGER_TYPES = {
    "mssql": {"tinyint", "smallint", "int", "bigint"},
    "postgres": {"smallint", "integer", "bigint"},
    "sqlite": {"integer"},
}

FLOAT_TYPES = {
    "mssql": {"float", "real"},
    "postgres": {"real", "double precision"},
    "sqlite": {"real"},
}

DECIMAL_TYPES = {
    "mssql": {"decimal", "numeric"},
    "postgres": {"decimal", "numeric"},
    "sqlite": {"numeric"},
}

MONEY_TYPES = {
    "mssql": {"money", "smallmoney"},
    "postgres": set(),  # Postgres has no money type by default
    "sqlite": set(),
}

# Declarative rules for each base data type
DATA_TYPE_RULES = {
    "varchar": {
        "requires_length": True,
        "requires_min_max_date": False,
        "validate_numeric": False,
    },
    "char": {
        "requires_length": True,
        "requires_min_max_date": False,
        "validate_numeric": False,
    },
    "decimal": {
        "requires_length": False,
        "requires_min_max_date": False,
        "validate_numeric": True,
    },
    "numeric": {
        "requires_length": False,
        "requires_min_max_date": False,
        "validate_numeric": True,
    },
    "int": {
        "requires_length": False,
        "requires_min_max_date": False,
        "validate_numeric": True,
    },
    "tinyint": {
        "requires_length": False,
        "requires_min_max_date": False,
        "validate_numeric": True,
    },
    "smallint": {
        "requires_length": False,
        "requires_min_max_date": False,
        "validate_numeric": True,
    },
    "bigint": {
        "requires_length": False,
        "requires_min_max_date": False,
        "validate_numeric": True,
    },
    "integer": {
        "requires_length": False,
        "requires_min_max_date": False,
        "validate_numeric": True,
    },
    "float": {
        "requires_length": False,
        "requires_min_max_date": False,
        "validate_numeric": True,
    },
    "real": {
        "requires_length": False,
        "requires_min_max_date": False,
        "validate_numeric": True,
    },
    "money": {
        "requires_length": False,
        "requires_min_max_date": False,
        "validate_numeric": True,
    },
    "smallmoney": {
        "requires_length": False,
        "requires_min_max_date": False,
        "validate_numeric": True,
    },
    "date": {
        "requires_length": False,
        "requires_min_max_date": True,
        "validate_numeric": False,
    },
    "time": {
        "requires_length": False,
        "requires_min_max_date": True,
        "validate_numeric": False,
    },
    "datetime": {
        "requires_length": False,
        "requires_min_max_date": True,
        "validate_numeric": False,
    },
    "datetime2": {
        "requires_length": False,
        "requires_min_max_date": True,
        "validate_numeric": False,
    },
    "smalldatetime": {
        "requires_length": False,
        "requires_min_max_date": True,
        "validate_numeric": False,
    },
}


def types_requiring_length(engine_type: str) -> set[str]:
    return LENGTH_REQUIRED.get(engine_type.lower(), set())


def is_date_or_time_type(engine_type: str, data_type: str) -> bool:
    """
    Return True if the given data_type is a date/time type for the engine.
    """
    if not data_type:
        return False

    base = data_type.split("(", 1)[0].lower()
    return base in DATE_TIME_TYPES.get(engine_type.lower(), set())


def is_integer_type(engine, dtype):
    return dtype in INTEGER_TYPES.get(engine, set())


def is_float_type(engine, dtype):
    return dtype in FLOAT_TYPES.get(engine, set())


def is_decimal_type(engine, dtype):
    return dtype in DECIMAL_TYPES.get(engine, set())


def is_money_type(engine, dtype):
    return dtype in MONEY_TYPES.get(engine, set())
