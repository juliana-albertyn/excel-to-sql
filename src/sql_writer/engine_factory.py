# A single place for engine creation (`engine_factory.py`)
# Handles:
# - connection strings
# - driver selection
# - environment variables
# - future expansion (e.g., Azure SQL, RDS, etc.)
# - `get_engine(db_config)`
# - Handles SQL Server, SQLite, Postgres
# - Returns a SQLAlchemy Engine

from typing import Any
from sqlalchemy import create_engine, Engine

import src.logging_setup as logging_setup
from config.database_config import (
    DatabaseConfig,
    SQLiteConfig,
    MSSQLConfig,
    PostgresConfig,
)


def _build_sqlite_engine(config: SQLiteConfig) -> Engine:
    # engine = create_engine("sqlite:///D:/upwork_projects/excel_to_sql_pipeline/data/processed/.sqlite", echo=True)
    engine = create_engine(f"sqlite:///{config.path}", echo=True)
    return engine


def _build_mssql_engine(config: MSSQLConfig) -> Engine:
    pass


def _build_postgres_engine(config: PostgresConfig) -> Engine:
    pass


def get_engine(config: DatabaseConfig):
    if isinstance(config, SQLiteConfig):
        return _build_sqlite_engine(config)
    if isinstance(config, MSSQLConfig):
        return _build_mssql_engine(config)
    if isinstance(config, PostgresConfig):
        return _build_postgres_engine(config)
