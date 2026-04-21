"""
Module: pipeline_config
Purpose:

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-04-02"

import yaml
from dataclasses import dataclass
from typing import Dict, Literal
from pathlib import Path

from .database_config import (
    DatabaseConfig,
    SQLiteConfig,
    MSSQLConfig,
    PostgresConfig,
    AuthenticationConfig,
)
import src.errors as errors
from src.errors import ErrorContext
import utils.helpers as helpers


@dataclass
class PipelineConfig:
    active_target: Literal["sqlite", "mssql", "postgres"]
    targets: Dict[str, DatabaseConfig]

    @classmethod
    def from_yaml(cls, path: Path) -> "PipelineConfig":
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        raw = helpers.lower_keys(raw)

        active = raw.get("active_target")
        if active is None:
            raise errors.ConfigError("Active target type not given", ErrorContext())
        raw_targets = raw.get("targets")
        if raw_targets is None:
            raise errors.ConfigError("Target types not given", ErrorContext())

        targets: Dict[str, DatabaseConfig] = {}

        for name, cfg in raw_targets.items():
            if name == "sqlite":
                targets[name] = SQLiteConfig(
                    path=cfg["path"],
                    if_exists=cfg.get("if_exists", "append"),
                    batch_size=cfg.get("batch_size", 1000),
                )

            elif name == "mssql":
                auth = cfg["authentication"]
                targets[name] = MSSQLConfig(
                    driver=cfg["driver"],
                    server=cfg["server"],
                    database=cfg["database"],
                    authentication=AuthenticationConfig(
                        auth_type=auth["auth_type"],
                        username=auth.get("username"),
                        password=auth.get("password"),
                    ),
                    schema=cfg.get("schema", "dbo"),
                    if_exists=cfg.get("if_exists", "append"),
                    batch_size=cfg.get("batch_size", 1000),
                    fast_executemany=cfg.get("fast_executemany", True),
                )

            elif name == "postgres":
                targets[name] = PostgresConfig(
                    host=cfg["host"],
                    port=cfg["port"],
                    database=cfg["database"],
                    username=cfg["username"],
                    password=cfg["password"],
                    schema=cfg.get("schema", "public"),
                    if_exists=cfg.get("if_exists", "append"),
                    batch_size=cfg.get("batch_size", 1000),
                )

            else:
                raise errors.ConfigError(f"Unknown target type: {name}", ErrorContext())

        return cls(active_target=active, targets=targets)

    def get_active_target(self) -> DatabaseConfig:
        if self.active_target not in self.targets:
            raise errors.ConfigError(
                f"Active target '{self.active_target}' not found in targets.",
                ErrorContext(),
            )
        return self.targets[self.active_target]