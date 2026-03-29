"""
Error types and context structures for the Fynbyte Excel-to-SQL pipeline.

Defines stage identifiers, a shared ErrorContext for attaching metadata,
and a hierarchy of pipeline exceptions used across extraction, cleaning,
transformation, validation, and loading.
"""

from enum import StrEnum
from typing import Any
from dataclasses import dataclass, field
from abc import ABC


class Stage(StrEnum):
    """
    Enumeration of pipeline stages used for error classification.
    """
    UNKNOWN = "Unknown"
    ORCHESTRATOR = "Orchestrator"
    EXTRACTOR = "Extractor"
    CLEANER = "Cleaner"
    TRANSFORMER = "Transformer"
    VALIDATOR = "Validator"
    LOADER = "Loader"


@dataclass
class ErrorContext:
    """
    Structured metadata describing where and why an error occurred.

    Captures stage, table, column, affected rows, optional details, and the
    original exception. Used by all pipeline errors for consistent reporting.
    """    
    stage: Stage = Stage.UNKNOWN
    table_name: str = ""
    column_name: str = ""
    rows: list[int] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)
    original_exception: Exception | None = None

    def __str__(self) -> str:
        s = f"Stage:{self.stage}. "
        if self.table_name != "":
            s += f"Table:{self.table_name}. "
        if self.column_name != "":
            s += f"Column:{self.column_name}. "
        if len(self.rows) != 0:
            s += f"Rows:{self.rows}. "
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
        s = f"Stage:{self.stage}. "
        if self.table_name != "":
            s += f"Table:{self.table_name}. "
        if self.column_name != "":
            s += f"Column:{self.column_name}. "
        if len(self.rows) != 0:
            s += f"Rows:{self.rows}. "
        if self.details != "":
            s += f"Details:{self.details}. "
        if self.original_exception:
            s += f"Exception:{self.original_exception}."
        return s.strip()


class PipelineError(Exception):
    """
    Base exception type for all pipeline errors.

    Wraps a message and an ErrorContext, and provides helpers for readable
    string output and serialisation.
    """
    def __init__(self, msg: str, error_context: ErrorContext) -> None:
        self.msg = msg
        self.error_context = error_context

    def __str__(self) -> str:
        """
        Return a concise human-readable summary of the error context.
        """        
        return f"{self.msg}. {self.error_context}"

    def __repr__(self) -> str:
        """
        Return a detailed representation of the error context for debugging.
        """        
        return f"{self.msg}. {self.error_context}"

    def to_dict(self) -> dict[str, Any]:
        """
        Serialise the error context to a dictionary for logging or reporting.
        """        
        error_dict = {"message": self.msg}
        context_dict = self.error_context.to_dict()
        return {**error_dict, **context_dict}


class ConfigError(PipelineError):
    """
    Error raised for invalid or missing configuration detected before
    pipeline execution.
    """
    STAGE = Stage.ORCHESTRATOR

    def __init__(self, msg: str, error_context: ErrorContext):
        error_context.stage = self.STAGE
        super().__init__(msg, error_context)


class SchemaError(PipelineError):
    """
    Error raised when schema definitions or mappings are invalid.
    """    
    STAGE = Stage.ORCHESTRATOR

    def __init__(self, msg: str, error_context: ErrorContext):
        error_context.stage = self.STAGE
        super().__init__(msg, error_context)


class PipelineStageError(PipelineError, ABC):
    """
    Abstract base class for stage-specific pipeline errors.
    """
    pass

class ExtractorError(PipelineStageError):
    """
    Error raised during the extraction stage.
    """    
    STAGE = Stage.EXTRACTOR

    def __init__(self, msg: str, error_context: ErrorContext):
        error_context.stage = self.STAGE
        super().__init__(msg, error_context)


class CleanerError(PipelineStageError):
    """
    Error raised during the cleaning stage.
    """    
    STAGE = Stage.CLEANER

    def __init__(self, msg: str, error_context: ErrorContext):
        error_context.stage = self.STAGE
        super().__init__(msg, error_context)


class TransformerError(PipelineStageError):
    """
    Error raised during the transformation stage.
    """
    STAGE = Stage.TRANSFORMER

    def __init__(self, msg: str, error_context: ErrorContext):
        error_context.stage = self.STAGE
        super().__init__(msg, error_context)


class ValidatorError(PipelineError):
    """
    Error raised during the validation stage.
    """    
    STAGE = Stage.VALIDATOR

    def __init__(self, msg: str, error_context: ErrorContext):
        error_context.stage = self.STAGE
        super().__init__(msg, error_context)


class LoaderError(PipelineStageError):
    """
    Error raised during the loading stage.
    """    
    STAGE = Stage.LOADER

    def __init__(self, msg: str, error_context: ErrorContext):
        error_context.stage = self.STAGE
        super().__init__(msg, error_context)
