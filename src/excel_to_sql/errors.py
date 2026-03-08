from enum import StrEnum
from typing import Any
from dataclasses import dataclass, field
from abc import ABC


class Stage(StrEnum):
    UNKNOWN = "Unknown"
    ORCHESTRATOR = "Orchestrator"
    EXTRACTOR = "Extractor"
    CLEANER = "Cleaner"
    TRANSFORMER = "Transformer"
    VALIDATOR = "Validator"
    LOADER = "Loader"


@dataclass
class ErrorContext:
    stage: Stage = Stage.UNKNOWN
    table_name: str = ""
    column_name: str = ""
    rows: list[int] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)
    original_exception: Exception | None = None

    def __str__(self) -> str:
        s = f"Stage: {self.stage} "
        if self.table_name != "":
            s += f"Table: {self.table_name} "
        if self.column_name != "":
            s += f"Column: {self.column_name} "
        if len(self.rows) != 0:
            s += f"Rows: {self.rows}"
        return s.strip()

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "table": self.table_name,
            "column": self.column_name,
            "rows": self.rows,
            "details": self.details,
            "original_exception": self.original_exception,
        }

    def __repr__(self) -> str:
        s = f"Stage: {self.stage} "
        if self.table_name != "":
            s += f"Table: {self.table_name} "
        if self.column_name != "":
            s += f"Column: {self.column_name} "
        if len(self.rows) != 0:
            s += f"Rows: {self.rows} "
        if self.details != "":
            s += f"Details: {self.details}"
        if self.original_exception:
            s += f"Exception: {self.original_exception} "
        return s.strip()


class PipelineError(Exception):
    """Base exception type for the pipeline"""

    def __init__(self, msg: str, error_context: ErrorContext) -> None:
        self.msg = msg
        self.error_context = error_context

    def __str__(self) -> str:
        return f"{self.msg}. {self.error_context}"

    def __repr__(self) -> str:
        return f"{self.msg}. {self.error_context}"

    def to_dict(self) -> dict[str, Any]:
        error_dict = {"message": self.msg}
        context_dict = self.error_context.to_dict()
        return {**error_dict, **context_dict}


class ConfigError(PipelineError):
    STAGE = Stage.ORCHESTRATOR

    def __init__(self, msg, context):
        context.stage = self.STAGE
        super().__init__(msg, context)


class SchemaError(PipelineError):
    STAGE = Stage.ORCHESTRATOR

    def __init__(self, msg, context):
        context.stage = self.STAGE
        super().__init__(msg, context)


class PipelineStageError(PipelineError, ABC):
    """Abstract base class for stage-specific pipeline errors."""

    pass


class ExtractorError(PipelineStageError):
    STAGE = Stage.EXTRACTOR

    def __init__(self, msg, context):
        context.stage = self.STAGE
        super().__init__(msg, context)


class CleanerError(PipelineStageError):
    STAGE = Stage.CLEANER

    def __init__(self, msg, context):
        context.stage = self.STAGE
        super().__init__(msg, context)


class TransformerError(PipelineStageError):
    STAGE = Stage.TRANSFORMER

    def __init__(self, msg, context):
        context.stage = self.STAGE
        super().__init__(msg, context)


class ValidatorError(PipelineError):
    STAGE = Stage.VALIDATOR

    def __init__(self, msg, context):
        context.stage = self.STAGE
        super().__init__(msg, context)


class LoaderError(PipelineStageError):
    STAGE = Stage.LOADER

    def __init__(self, msg, context):
        context.stage = self.STAGE
        super().__init__(msg, context)
