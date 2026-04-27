"""
Cleaning engine for the Fynbyte Excel-to-SQL pipeline.

Applies global and per-column cleaning rules before validation.
Handles whitespace trimming, null normalisation, type coercion,
date/time parsing, numeric conversion, and basic format cleanup.
Uses ETLContext settings for strictness, locale, and parser behaviour.

Does not infer meaning or repair data. Only standardises and prepares it
for validation and loading.

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-08"

import pandas as pd
from pandas import DataFrame
import base64
from binascii import Error as BinasciiError
from logging import DEBUG

import src.logging_setup as logging_setup
import src.errors as errors
from src.context import ETLContext

from config.project_config import ProjectConfig

from schemas.table_schema import TableSchema
from schemas.column_definitions import ColumnDefinition

import utils.helpers as helpers


class Cleaner:

    def __init__(
        self,
        df: DataFrame,
        project_config: ProjectConfig,
        table_schema: TableSchema,
        etl_context: ETLContext,
        debug_trace: bool = False,
    ):
        self.df = df
        self.project_config = project_config
        self.table_schema = table_schema
        self.etl_context = etl_context
        self.debug_trace = debug_trace
        # setup logger
        self.logger = logging_setup.get_logger(etl_context, __name__)
        if self.debug_trace:
            self.logger = logging_setup.set_logger_level(self.logger, DEBUG)

    # Debug helpers
    def _trace(self, msg: str):
        """
        Emit a debug trace message when debug tracing is enabled.
        """
        if self.debug_trace and self.logger:
            self.logger.debug(f"[TRACE] {msg}")

    # column level functions
    def _col_to_date(self, series: pd.Series, col: ColumnDefinition) -> pd.Series:
        """
        Parse a Series to datetime using ETLContext's datetime parser.

        Raises CleanerError if the parser is missing, or (in strict mode)
        on the first non-null value that fails to parse. Otherwise invalid
        values are coerced to NaT. Error messages include table/column and
        row_offset-adjusted row numbers.
        """
        if self.etl_context.datetime_parser is None:
            raise errors.CleanerError(
                "DateTime parser not initialised",
                errors.ErrorContext(
                    original_exception=None,
                    rows=[],
                    table_name=self.table_schema.table_name,
                    column_name=col.column_name,
                    details={"function": "_col_to_date"},
                ),
            )
        out = series.apply(self.etl_context.datetime_parser.parse_date)

        # exception will be raised in project_config.py
        assert self.project_config.runtime is not None

        if self.project_config.runtime.strict_validation:
            mask = out.isna() & series.notna()
            if mask.any():
                bad_index = mask.index[0]
                bad_value = series.loc[bad_index]
                row_number = self.project_config.spreadsheet_row_number(int(bad_index))
                raise errors.CleanerError(
                    f"Invalid date '{bad_value}' at row {row_number}",
                    errors.ErrorContext(
                        original_exception=None,
                        rows=[row_number],
                        table_name=self.table_schema.table_name,
                        column_name=col.column_name,
                        details={"value": bad_value, "function": "_col_to_date"},
                    ),
                )

        return pd.to_datetime(out, errors="coerce")

    def _col_to_time(self, series: pd.Series, col: ColumnDefinition) -> pd.Series:
        """
        Parse a Series to time using ETLContext's parser.

        Raises CleanerError if the parser is missing, or (in strict mode)
        on the first non-null value that fails to parse. Otherwise invalid
        values are coerced to NaT. Error messages include table/column and
        row_offset-adjusted row numbers.
        """
        if self.etl_context.datetime_parser is None:
            raise errors.CleanerError(
                "DateTime parser not initialised",
                errors.ErrorContext(
                    original_exception=None,
                    rows=[],
                    table_name=self.table_schema.table_name,
                    column_name=col.column_name,
                    details={"function": "_col_to_time"},
                ),
            )

        out = series.apply(self.etl_context.datetime_parser.parse_time)

        # exception will be raised in project_config.py
        assert self.project_config.runtime is not None

        if self.project_config.runtime.strict_validation:
            mask = out.isna() & series.notna()
            if mask.any():
                bad_index = mask.index[0]
                bad_value = series.loc[bad_index]
                row_number = self.project_config.spreadsheet_row_number(int(bad_index))

                raise errors.CleanerError(
                    f"Invalid time '{bad_value}' at row {row_number}",
                    errors.ErrorContext(
                        original_exception=None,
                        rows=[row_number],
                        table_name=self.table_schema.table_name,
                        column_name=col.column_name,
                        details={"value": bad_value, "function": "_col_to_time"},
                    ),
                )

        return pd.to_datetime(out, errors="coerce")

    def _col_to_datetime(self, series: pd.Series, col: ColumnDefinition) -> pd.Series:
        """
        Parse a Series to datetime using ETLContext's parser.

        Raises CleanerError if the parser is missing, or (in strict mode)
        on the first non-null value that fails to parse. Otherwise invalid
        values are coerced to NaT. Error messages include table/column and
        row_offset-adjusted row numbers.
        """
        if self.etl_context.datetime_parser is None:
            raise errors.CleanerError(
                "DateTime parser not initialised",
                errors.ErrorContext(
                    original_exception=None,
                    rows=[],
                    table_name=self.table_schema.table_name,
                    column_name=col.column_name,
                    details={"function": "_col_to_datetime"},
                ),
            )

        out = series.apply(self.etl_context.datetime_parser.parse_datetime)

        # exception will be raised in project_config.py
        assert self.project_config.runtime is not None

        if self.project_config.runtime.strict_validation:
            mask = out.isna() & series.notna()
            if mask.any():
                bad_index = mask.index[0]
                bad_value = series.loc[bad_index]
                row_number = self.project_config.spreadsheet_row_number(int(bad_index))

                raise errors.CleanerError(
                    f"Invalid datetime '{bad_value}' at row {row_number}",
                    errors.ErrorContext(
                        original_exception=None,
                        rows=[row_number],
                        table_name=self.table_schema.table_name,
                        column_name=col.column_name,
                        details={"value": bad_value, "function": "_col_to_datetime"},
                    ),
                )

        return pd.to_datetime(out, errors="coerce")

    def _col_to_money(self, series: pd.Series, col: ColumnDefinition) -> pd.Series:
        """
        Parse a Series to numeric after removing currency symbols.

        Strict mode raises on the first invalid value; otherwise invalid
        values are coerced to NaN. Error messages include table/column and
        row_offset-adjusted row numbers.
        """
        # remove currency symbol and whitespace
        cleaned = (
            series.astype("string")
            .str.removeprefix(self.etl_context.currency_symbol)
            .str.strip()
        )

        # exception will be raised in project_config.py
        assert self.project_config.runtime is not None

        if self.project_config.runtime.strict_validation:
            try:
                return pd.to_numeric(cleaned, errors="raise")
            except Exception as e:
                # detect failing rows
                temp = pd.to_numeric(cleaned, errors="coerce")
                mask = temp.isna()

                bad_index = series.index[mask][0]
                bad_value = series.loc[bad_index]
                row_number = self.project_config.spreadsheet_row_number(int(bad_index))

                raise errors.CleanerError(
                    f"Invalid money value '{bad_value}' at row {row_number}",
                    errors.ErrorContext(
                        original_exception=e,
                        rows=[row_number],
                        table_name=self.table_schema.table_name,
                        column_name=col.column_name,
                        details={"value": bad_value, "function": "_col_to_money"},
                    ),
                )

        # permissive mode
        out = pd.to_numeric(cleaned, errors="coerce")
        return out

    def _col_to_str(self, series: pd.Series, col: ColumnDefinition) -> pd.Series:
        """
        Clean a Series as string data.

        Trims whitespace, applies case rules, and converts empty or 'nan'
        strings to pd.NA. Returns pandas string dtype.
        """
        # convert to string and trim
        cleaned = series.astype("string").str.strip()

        # apply case rules
        str_case = col.str_case.lower()
        if str_case == "lower":
            cleaned = cleaned.str.lower()
        elif str_case == "upper":
            cleaned = cleaned.str.upper()
        elif str_case == "title":
            cleaned = cleaned.str.title()
        # "asis" means do nothing

        # mask empty strings and literal "nan"
        mask = cleaned.str.strip().isin(["", "nan"])
        cleaned = cleaned.mask(mask, pd.NA)

        # ensure pandas string dtype
        return cleaned.astype("string")

    def _col_to_bit(self, series: pd.Series) -> pd.Series:
        """
        Convert a Series to boolean values.

        Applies truthy string mapping and preserves existing booleans.
        Returns pandas nullable boolean dtype.
        """
        # apply helper
        cleaned = series.apply(helpers.str_to_bool)

        # Ensure pandas boolean dtype
        return cleaned.astype("boolean")

    def _col_to_int(self, series: pd.Series, col: ColumnDefinition) -> pd.Series:
        """
        Parse a Series to integer (nullable Int64).

        Strict mode raises on the first invalid value; otherwise invalid
        values are coerced to NaN. Error messages include table/column and
        row_offset-adjusted row numbers.
        """
        # exception will be raised in project_config.py
        assert self.project_config.runtime is not None
        assert self.project_config.source is not None

        if self.project_config.runtime.strict_validation:
            try:
                return pd.to_numeric(series, errors="raise").astype("Int64")
            except Exception as e:
                temp = pd.to_numeric(series, errors="coerce")
                mask = temp.isna()

                bad_index = series.index[mask][0]
                bad_value = series.loc[bad_index]
                row_number = self.project_config.spreadsheet_row_number(int(bad_index))

                raise errors.CleanerError(
                    f"Invalid integer '{bad_value}' at row {row_number}",
                    errors.ErrorContext(
                        original_exception=e,
                        rows=[row_number],
                        table_name=self.table_schema.table_name,
                        column_name=col.column_name,
                        details={"value": bad_value, "function": "_col_to_int"},
                    ),
                )

        # permissive mode

        return pd.to_numeric(series, errors="coerce").astype("Int64")

    def _col_to_numeric(self, series: pd.Series, col: ColumnDefinition) -> pd.Series:
        """
        Parse a Series to numeric values.

        Strict mode raises on the first invalid value; otherwise invalid
        values are coerced to NaN. Error messages include table/column and
        row_offset-adjusted row numbers.
        """
        # exception will be raised in project_config.py
        assert self.project_config.runtime is not None

        if self.project_config.runtime.strict_validation:
            try:
                return pd.to_numeric(series, errors="raise")
            except Exception as e:
                temp = pd.to_numeric(series, errors="coerce")
                mask = temp.isna()

                bad_index = series.index[mask][0]
                bad_value = series.loc[bad_index]
                row_number = self.project_config.spreadsheet_row_number(int(bad_index))

                raise errors.CleanerError(
                    f"Invalid numeric value '{bad_value}' at row {row_number}",
                    errors.ErrorContext(
                        original_exception=e,
                        rows=[row_number],
                        table_name=self.table_schema.table_name,
                        column_name=col.column_name,
                        details={"value": bad_value, "function": "_col_to_int"},
                    ),
                )

        return pd.to_numeric(series, errors="coerce")

    def _col_to_decimal_float_real(
        self, series: pd.Series, col: ColumnDefinition
    ) -> pd.Series:
        """
        Parse a Series to float/decimal values.

        Strict mode raises on the first invalid value; otherwise invalid
        values are coerced to NaN. Error messages include table/column and
        row_offset-adjusted row numbers.
        """
        # exception will be raised in project_config.py
        assert self.project_config.runtime is not None

        if self.project_config.runtime.strict_validation:
            try:
                return pd.to_numeric(series, errors="raise")
            except Exception as e:
                temp = pd.to_numeric(series, errors="coerce")
                mask = temp.isna()

                bad_index = series.index[mask][0]
                bad_value = series.loc[bad_index]
                row_number = self.project_config.spreadsheet_row_number(int(bad_index))

                raise errors.CleanerError(
                    f"Invalid decimal/float value '{bad_value}' at row {row_number}",
                    errors.ErrorContext(
                        original_exception=e,
                        rows=[row_number],
                        table_name=self.table_schema.table_name,
                        column_name=col.column_name,
                        details={
                            "value": bad_value,
                            "function": "_col_to_decimal_float_real",
                        },
                    ),
                )

        return pd.to_numeric(series, errors="coerce")

    def _col_to_binary(self, series: pd.Series, col: ColumnDefinition) -> pd.Series:
        """
        Parse a Series to binary (bytes) from hex strings.

        Strict mode raises on the first invalid value; otherwise invalid
        values are set to None. Error messages include table/column and
        row_offset-adjusted row numbers.
        """
        cleaned = series.astype("string").str.strip()

        def to_bytes(val: str):
            if val in ("", "nan"):
                return None
            try:
                return bytes.fromhex(val)
            except Exception:
                return None

        out = cleaned.apply(to_bytes)

        # exception will be raised in project_config.py
        assert self.project_config.runtime is not None

        if self.project_config.runtime.strict_validation and out.isna().any():
            bad_index = out.index[out.isna()][0]
            bad_value = series.loc[bad_index]
            row_number = self.project_config.spreadsheet_row_number(int(bad_index))
            raise errors.CleanerError(
                f"Invalid binary value '{bad_value}' at row {row_number}",
                errors.ErrorContext(
                    original_exception=None,
                    rows=[row_number],
                    table_name=self.table_schema.table_name,
                    column_name=col.column_name,
                    details={"value": bad_value, "function": "_col_to_binary"},
                ),
            )

        return out

    def _col_to_image(self, series: pd.Series, col: ColumnDefinition) -> pd.Series:
        """
        Parse and validate image data as bytes.

        Accepts hex, base64, or raw bytes and checks known image signatures.
        Strict mode raises on the first invalid value; otherwise invalid
        values are set to None. Error messages include table/column and
        row_offset-adjusted row numbers.
        """

        # Magic byte signatures
        MAGIC = {
            "jpeg": bytes.fromhex("FFD8FF"),
            "png": bytes.fromhex("89504E470D0A1A0A"),
            "gif": bytes.fromhex("47494638"),
            "bmp": bytes.fromhex("424D"),
        }

        def decode_and_validate(val):
            if val is None or pd.isna(val):
                return None

            # Already bytes?
            if isinstance(val, (bytes, bytearray)):
                data = bytes(val)
            else:
                s = str(val).strip()

                # Try hex
                try:
                    data = bytes.fromhex(s)
                except ValueError:
                    # Try base64
                    try:
                        data = base64.b64decode(s, validate=True)
                    except (BinasciiError, ValueError):
                        return None

            # Validate magic bytes
            for sig in MAGIC.values():
                if data.startswith(sig):
                    return data

            return None  # Not a recognised image format

        decoded = series.apply(decode_and_validate)

        # exception will be raised in project_config.py
        assert self.project_config.runtime is not None

        if self.project_config.runtime.strict_validation and decoded.isna().any():
            bad_index = decoded.index[decoded.isna()][0]
            bad_value = series.loc[bad_index]
            row_number = self.project_config.spreadsheet_row_number(int(bad_index))

            raise errors.CleanerError(
                f"Invalid image data '{bad_value}' at row {row_number}",
                errors.ErrorContext(
                    original_exception=None,
                    rows=[row_number],
                    table_name=self.table_schema.table_name,
                    column_name=col.column_name,
                    details={"value": bad_value, "function": "_col_to_image"},
                ),
            )

        return decoded

    def _trim_whitespace(self) -> pd.DataFrame:
        """
        Trim leading and trailing whitespace in string columns.
        """
        for col in self.df.select_dtypes(include=["object", "string"]):
            self.df[col] = self.df[col].str.strip()
        return self.df

    def _standardise_nulls(self) -> pd.DataFrame:
        """
        Replace common null-like strings with pd.NA in string columns.
        """
        return self.df.apply(
            lambda col: (
                col.replace(r"^(NA|N/A|NULL|None)$", pd.NA, regex=True)
                if col.dtype in ["object", "string"]
                else col
            )
        )

    def _remove_empty_rows(self) -> pd.DataFrame:
        """
        Remove rows where all values are missing.
        """
        return self.df.dropna(how="all")

    def _log_column_changes(self) -> None:
        """
        Log differences between original and cleaned columns.

        Compares values by type and logs row-level changes using
        row_offset-adjusted row numbers.
        """
        self.logger.info(
            f"Audit of cleaned values for *** {self.table_schema.table_name} ***:"
        )
        cleaned_suffix = self.etl_context.cleaned_suffix

        for col in self.df.columns:
            if col.endswith(cleaned_suffix):
                original_col = col.replace(cleaned_suffix, "")

                orig = self.df[original_col]
                clean = self.df[col]
                equal_mask = pd.Series(False, index=clean.index)

                # 1. Missing values equal
                both_missing = orig.isna() & clean.isna()

                # 2. String comparison (detects trimming)
                if pd.api.types.is_string_dtype(clean):
                    orig_str = orig.astype("string")
                    clean_str = clean.astype("string")
                    string_equal = orig_str.eq(clean_str)
                    equal_mask = both_missing | string_equal

                # 3. Numeric comparison
                elif pd.api.types.is_numeric_dtype(clean):
                    orig_num = pd.to_numeric(orig, errors="coerce")
                    clean_num = pd.to_numeric(clean, errors="coerce")
                    numeric_equal = orig_num.eq(clean_num)
                    equal_mask = both_missing | numeric_equal

                # 4. Dates comparison
                elif pd.api.types.is_datetime64_any_dtype(clean):
                    # compare as str
                    orig_str = orig.astype("string").fillna(value="")
                    clean_str = clean.astype("string").fillna(value="")
                    orig_str = orig_str.str.replace("00:00:00", "").str.strip()
                    clean_str = clean_str.str.replace("00:00:00", "").str.strip()

                    string_equal = orig_str.eq(clean_str)

                    equal_mask = both_missing | string_equal

                diff_mask = ~equal_mask

                if not diff_mask.any():
                    self.logger.info(f"No cleaned values for column {original_col}")
                else:
                    for idx in self.df.index[diff_mask]:
                        row_number = self.project_config.spreadsheet_row_number(
                            int(idx)
                        )
                        self.logger.info(
                            f"{original_col} | Row {row_number} | "
                            f"'{self.df.at[idx, original_col]}' -> '{self.df.at[idx, col]}'"
                        )

    def clean_data(self) -> pd.DataFrame:
        """
        Clean a DataFrame using configured rules and column mappings.

        Applies global cleaning steps, then per-column transformations
        based on data types. Logs changes and returns the cleaned DataFrame.
        """

        # get the date parser - lazy create
        self.etl_context.datetime_parser = self.etl_context.get_datetime_parser(
            self.logger
        )

        # do automatic convert of data types e.g. object -> str
        self.df = self.df.convert_dtypes()

        # log 'before' info
        self.logger.info(
            f"Cleaning data for *** {self.table_schema.table_name} ***. Shape: {self.df.shape}"
        )

        # Get context info
        cleaned_suffix = self.etl_context.cleaned_suffix

        # exception will be raised in project_config.py
        assert self.project_config.cleaning is not None

        # apply global cleaning rules
        if self.project_config.cleaning.trim_whitespace:
            self.df = self._trim_whitespace()
            self.logger.info(
                f"Trimmed whitespace for *** {self.table_schema.table_name} ***"
            )

        if self.project_config.cleaning.standardise_nulls:
            self.df = self._standardise_nulls()
            self.logger.info(
                f"Standardised nulls for *** {self.table_schema.table_name} ***"
            )

        if self.project_config.cleaning.remove_blank_rows:
            empty_rows, _ = self.df[self.df.isna().all(axis=1)].shape
            if empty_rows > 0:
                self.df = self._remove_empty_rows()
                self.logger.info(
                    f"Removed {empty_rows} empty rows for *** {self.table_schema.table_name} ***"
                )

        # clean all columns according to the column rules
        for col in self.table_schema.columns:
            cleaned_col = f"{col.column_name}{cleaned_suffix}"
            self.logger.info(f"Cleaning column: {col.column_name}->{cleaned_col}")
            # data type - remember schema has already been validated in pipeline
            dtype = col.data_type.split("(", 1)[0].strip()
            # data types and formats
            if dtype in ["char", "varchar", "text", "nchar", "nvarchar", "ntext"]:
                self.df[cleaned_col] = self._col_to_str(
                    series=self.df[col.column_name], col=col
                )

            elif dtype in ["tinyint", "smallint", "int", "bigint"]:
                self.df[cleaned_col] = self._col_to_int(
                    series=self.df[col.column_name], col=col
                )

            elif dtype == "numeric":
                self.df[cleaned_col] = self._col_to_numeric(
                    series=self.df[col.column_name], col=col
                )

            elif dtype in ["decimal", "float", "real"]:
                self.df[cleaned_col] = self._col_to_decimal_float_real(
                    series=self.df[col.column_name], col=col
                )

            elif dtype in ["binary", "varbinary"]:
                self.df[cleaned_col] = self._col_to_binary(
                    series=self.df[col.column_name], col=col
                )

            elif dtype == "image":
                self.df[cleaned_col] = self._col_to_image(
                    series=self.df[col.column_name], col=col
                )

            elif dtype == "bit":
                self.df[cleaned_col] = self._col_to_bit(series=self.df[col.column_name])

            elif dtype == "date":
                self.df[cleaned_col] = self._col_to_date(
                    series=self.df[col.column_name], col=col
                )

            elif dtype == "datetime":
                self.df[cleaned_col] = self._col_to_datetime(
                    series=self.df[col.column_name], col=col
                )

            elif dtype == "time":
                self.df[cleaned_col] = self._col_to_time(
                    series=self.df[col.column_name], col=col
                )

            elif dtype in ["smallmoney", "money"]:
                self.df[cleaned_col] = self._col_to_money(
                    series=self.df[col.column_name], col=col
                )

        # log only cleaned columns
        self._log_column_changes()

        return self.df
