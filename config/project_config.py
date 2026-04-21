"""
Module: project_config
Purpose:

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-04-02"

import yaml
from dataclasses import dataclass, field
from typing import Literal
from pathlib import Path

import src.errors as errors
from src.errors import ErrorContext
import utils.helpers as helpers


@dataclass
class RuntimeConfig:
    pipeline_config_file: str
    project_name: str = "excel_to_sql"
    currency_symbol: str = ""
    strict_validation: bool = True  # strict | permissive
    day_first_format: bool = True


@dataclass
class SourceFileConfig:
    file_name: str
    input_type: Literal["excel"] = "excel"  # only one option for now
    header_rows: int | None = None

    def __post_init__(self):
        if self.header_rows is not None and self.header_rows < 0:
            raise errors.ConfigError(
                "Header row must be zero (no header) or a positive number",
                ErrorContext(),
            )


@dataclass
class CleaningRules:
    trim_whitespace: bool = True
    standardise_nulls: bool = True
    remove_blank_rows: bool = True


@dataclass
class OutputSettings:
    write_sql: bool = True
    exports: list[Literal["excel", "csv"]] = field(default_factory=list)


@dataclass
class SheetToTableMapping:
    sheet_name: str
    schema_file: str
    target_table: str


@dataclass
class MappingConfig:
    mappings: dict[str, SheetToTableMapping]

    def __iter__(self):
        return iter(self.mappings.items())

    def __getitem__(self, key: str) -> SheetToTableMapping:
        return self.mappings[key]


@dataclass
class ProjectConfig:
    runtime: RuntimeConfig | None = None
    source: SourceFileConfig | None = None
    mappings: MappingConfig | None = None
    cleaning: CleaningRules | None = None
    output: OutputSettings | None = None

    @classmethod
    def from_yaml(cls, path: Path) -> "ProjectConfig":
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        raw = helpers.lower_keys(raw)
        runtime_config = None
        source_config = None
        cleaning_rules = None
        output_settings = None
        mapping_config = None

        runtime = raw.get("runtime")
        if runtime is None:
            raise errors.ConfigError("Runtime configuration not given", ErrorContext())
        else:
            runtime_config = RuntimeConfig(
                project_name=runtime.get("project_name"),
                pipeline_config_file=runtime.get("pipeline_config"),
                strict_validation=runtime.get("strict_validation"),
                currency_symbol=runtime.get("currency_symbol"),
                day_first_format=runtime.get("day_first_format"),
            )
        source = raw.get("source")
        if source is None:
            raise errors.ConfigError("Source configuration not given", ErrorContext())
        else:
            input_type = source.get("input_type")
            file_name = source.get("file_name")
            header_rows = source.get("header_rows")
            if input_type is None:
                raise errors.ConfigError("Source input type not given", ErrorContext())
            if file_name is None:
                raise errors.ConfigError("Source file name not given", ErrorContext())
            source_config = SourceFileConfig(
                input_type=input_type,
                file_name=file_name,
                header_rows=header_rows,
            )
        mappings = raw.get("mappings")
        if mappings is None:
            raise errors.ConfigError("Mapping configuration not given", ErrorContext())
        else:
            map_dict = dict()
            for table_name, config in mappings.items():
                map_dict[table_name] = SheetToTableMapping(
                    sheet_name=config["sheet_name"],
                    schema_file=config["schema_file"],
                    target_table=config["target_table"],
                )
            mapping_config = MappingConfig(map_dict)

        cleaning = raw.get("cleaning")
        if cleaning is not None:
            cleaning_rules = CleaningRules(
                trim_whitespace=cleaning.get("trim_whitespace"),
                standardise_nulls=cleaning.get("standardise_nulls"),
                remove_blank_rows=cleaning.get("remove_blank_rows"),
            )
        output = raw.get("output")
        if output is not None:
            output_settings = OutputSettings(
                write_sql=output.get("write_sql"), exports=output.get("exports")
            )
        return cls(
            runtime=runtime_config,
            source=source_config,
            cleaning=cleaning_rules,
            output=output_settings,
            mappings=mapping_config,
        )
