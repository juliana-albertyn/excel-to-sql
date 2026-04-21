"""
Module: database_config
Purpose: Short description

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-04-02"


from pathlib import Path
from typing import Literal
from dataclasses import dataclass
from abc import ABC


@dataclass
class DatabaseConfig(ABC): 
    """Base class for all database configuration types."""
    pass


@dataclass
class SQLiteConfig(DatabaseConfig):
    path: Path
    if_exists: Literal["append", "replace", "fail"]
    batch_size: int


@dataclass
class AuthenticationConfig:
    auth_type: Literal["windows", "sql"]
    username: str | None
    password: str | None


@dataclass
class MSSQLConfig(DatabaseConfig):
    driver: str
    server: str
    database: str
    authentication: AuthenticationConfig
    schema: str
    if_exists: Literal["append", "replace", "fail"]
    batch_size: int
    fast_executemany: bool


@dataclass
class PostgresConfig(DatabaseConfig):
    host: str
    port: int
    database: str
    username: str
    password: str
    schema: str = "public"
    if_exists: Literal["append", "replace", "fail"] = "append"
    batch_size: int = 1000
