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

from typing import Any
import pandas as pd
from pandas import DataFrame
from logging import Logger
import base64
from binascii import Error as BinasciiError

import src.logging_setup as logging_setup
import src.errors as errors
from src.context import ETLContext

from config.project_config import ProjectConfig
from config.pipeline_config import PipelineConfig



def col_to_date(
    series: pd.Series, table_name: str, col_name: str, etl_context: ETLContext
) -> pd.Series:
    """
    Parse a Series to datetime using ETLContext's datetime parser.

    Raises CleanerError if the parser is missing, or (in strict mode)
    on the first non-null value that fails to parse. Otherwise invalid
    values are coerced to NaT. Error messages include table/column and
    row_offset-adjusted row numbers.
    """
    if etl_context.datetime_parser is None:
        raise errors.CleanerError(
            "DateTime parser not initialised",
            errors.ErrorContext(
                original_exception=None,
                rows=[],
                table_name=table_name,
                column_name=col_name,
                details={"function": "col_to_date"},
            ),
        )
    out = series.apply(etl_context.datetime_parser.parse_date)

    if etl_context.strict_validation:
        mask = out.isna() & series.notna()
        if mask.any():
            bad_index = mask.index[0]
            bad_value = series.loc[bad_index]

            raise errors.CleanerError(
                f"Invalid date '{bad_value}' at row {bad_index + etl_context.row_offset}",
                errors.ErrorContext(
                    original_exception=None,
                    rows=[int(bad_index) + etl_context.row_offset],
                    table_name=table_name,
                    column_name=col_name,
                    details={"value": bad_value},
                ),
            )

    return pd.to_datetime(out, errors="coerce")


def col_to_time(
    series: pd.Series, table_name: str, col_name: str, etl_context: ETLContext
) -> pd.Series:
    """
    Parse a Series to time using ETLContext's parser.

    Raises CleanerError if the parser is missing, or (in strict mode)
    on the first non-null value that fails to parse. Otherwise invalid
    values are coerced to NaT. Error messages include table/column and
    row_offset-adjusted row numbers.
    """
    if etl_context.datetime_parser is None:
        raise errors.CleanerError(
            "DateTime parser not initialised",
            errors.ErrorContext(
                original_exception=None,
                rows=[],
                table_name=table_name,
                column_name=col_name,
                details={"function": "col_to_time"},
            ),
        )

    out = series.apply(etl_context.datetime_parser.parse_time)

    if etl_context.strict_validation:
        mask = out.isna() & series.notna()
        if mask.any():
            bad_index = mask.index[0]
            bad_value = series.loc[bad_index]

            raise errors.CleanerError(
                f"Invalid time '{bad_value}' at row {bad_index + etl_context.row_offset}",
                errors.ErrorContext(
                    original_exception=None,
                    rows=[int(bad_index) + etl_context.row_offset],
                    table_name=table_name,
                    column_name=col_name,
                    details={"value": bad_value},
                ),
            )

    return pd.to_datetime(out, errors="coerce")


def col_to_datetime(
    series: pd.Series,
    table_name: str,
    col_name: str,
    etl_context: ETLContext,
) -> pd.Series:
    """
    Parse a Series to datetime using ETLContext's parser.

    Raises CleanerError if the parser is missing, or (in strict mode)
    on the first non-null value that fails to parse. Otherwise invalid
    values are coerced to NaT. Error messages include table/column and
    row_offset-adjusted row numbers.
    """
    if etl_context.datetime_parser is None:
        raise errors.CleanerError(
            "DateTime parser not initialised",
            errors.ErrorContext(
                original_exception=None,
                rows=[],
                table_name=table_name,
                column_name=col_name,
                details={"function": "col_to_datetime"},
            ),
        )

    out = series.apply(etl_context.datetime_parser.parse_datetime)

    if etl_context.strict_validation:
        mask = out.isna() & series.notna()
        if mask.any():
            bad_index = mask.index[0]
            bad_value = series.loc[bad_index]

            raise errors.CleanerError(
                f"Invalid datetime '{bad_value}' at row {bad_index + etl_context.row_offset}",
                errors.ErrorContext(
                    original_exception=None,
                    rows=[int(bad_index) + etl_context.row_offset],
                    table_name=table_name,
                    column_name=col_name,
                    details={"value": bad_value},
                ),
            )

    return pd.to_datetime(out, errors="coerce")


def col_to_money(
    series: pd.Series, table_name: str, col_name: str, etl_context: ETLContext
) -> pd.Series:
    """
    Parse a Series to numeric after removing currency symbols.

    Strict mode raises on the first invalid value; otherwise invalid
    values are coerced to NaN. Error messages include table/column and
    row_offset-adjusted row numbers.
    """
    # remove currency symbol and whitespace
    cleaned = (
        series.astype("string")
        .str.removeprefix(etl_context.currency_symbol)
        .str.strip()
    )

    if etl_context.strict_validation:
        try:
            return pd.to_numeric(cleaned, errors="raise")
        except Exception as e:
            # detect failing rows
            temp = pd.to_numeric(cleaned, errors="coerce")
            mask = temp.isna()

            bad_index = series.index[mask][0]
            bad_value = series.loc[bad_index]

            raise errors.CleanerError(
                f"Invalid money value '{bad_value}' at row {bad_index + etl_context.row_offset}",
                errors.ErrorContext(
                    original_exception=e,
                    rows=[int(bad_index) + etl_context.row_offset],
                    table_name=table_name,
                    column_name=col_name,
                    details={"value": bad_value},
                ),
            )

    # permissive mode
    out = pd.to_numeric(cleaned, errors="coerce")
    return out


def col_to_str(series: pd.Series, str_case: str) -> pd.Series:
    """
    Clean a Series as string data.

    Trims whitespace, applies case rules, and converts empty or 'nan'
    strings to pd.NA. Returns pandas string dtype.
    """
    # convert to string and trim
    cleaned = series.astype("string").str.strip()

    # apply case rules
    str_case = str_case.lower()
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


def col_to_bit(series: pd.Series) -> pd.Series:
    """
    Convert a Series to boolean values.

    Applies truthy string mapping and preserves existing booleans.
    Returns pandas nullable boolean dtype.
    """
    # apply helper
    cleaned = series.apply(str_to_bool)

    # Ensure pandas boolean dtype
    return cleaned.astype("boolean")


def col_to_int(
    series: pd.Series,
    table_name: str,
    col_name: str,
    etl_context: ETLContext,
) -> pd.Series:
    """
    Parse a Series to integer (nullable Int64).

    Strict mode raises on the first invalid value; otherwise invalid
    values are coerced to NaN. Error messages include table/column and
    row_offset-adjusted row numbers.
    """
    if etl_context.strict_validation:
        try:
            return pd.to_numeric(series, errors="raise").astype("Int64")
        except Exception as e:
            temp = pd.to_numeric(series, errors="coerce")
            mask = temp.isna()

            bad_index = series.index[mask][0]
            bad_value = series.loc[bad_index]

            raise errors.CleanerError(
                f"Invalid integer '{bad_value}' at row {bad_index + etl_context.row_offset}",
                errors.ErrorContext(
                    original_exception=e,
                    rows=[int(bad_index) + etl_context.row_offset],
                    table_name=table_name,
                    column_name=col_name,
                    details={"value": bad_value},
                ),
            )

    # permissive mode

    return pd.to_numeric(series, errors="coerce").astype("Int64")


def col_to_numeric(
    series: pd.Series, table_name: str, col_name: str, etl_context: ETLContext
) -> pd.Series:
    """
    Parse a Series to numeric values.

    Strict mode raises on the first invalid value; otherwise invalid
    values are coerced to NaN. Error messages include table/column and
    row_offset-adjusted row numbers.
    """
    if etl_context.strict_validation:
        try:
            return pd.to_numeric(series, errors="raise")
        except Exception as e:
            temp = pd.to_numeric(series, errors="coerce")
            mask = temp.isna()

            bad_index = series.index[mask][0]
            bad_value = series.loc[bad_index]

            raise errors.CleanerError(
                f"Invalid numeric value '{bad_value}' at row {bad_index + etl_context.row_offset}",
                errors.ErrorContext(
                    original_exception=e,
                    rows=[int(bad_index) + etl_context.row_offset],
                    table_name=table_name,
                    column_name=col_name,
                    details={"value": bad_value},
                ),
            )

    return pd.to_numeric(series, errors="coerce")


def col_to_decimal_float_real(
    series: pd.Series, table_name: str, col_name: str, etl_context: ETLContext
) -> pd.Series:
    """
    Parse a Series to float/decimal values.

    Strict mode raises on the first invalid value; otherwise invalid
    values are coerced to NaN. Error messages include table/column and
    row_offset-adjusted row numbers.
    """
    if etl_context.strict_validation:
        try:
            return pd.to_numeric(series, errors="raise")
        except Exception as e:
            temp = pd.to_numeric(series, errors="coerce")
            mask = temp.isna()

            bad_index = series.index[mask][0]
            bad_value = series.loc[bad_index]

            raise errors.CleanerError(
                f"Invalid decimal/float value '{bad_value}' at row {bad_index + etl_context.row_offset}",
                errors.ErrorContext(
                    original_exception=e,
                    rows=[int(bad_index) + etl_context.row_offset],
                    table_name=table_name,
                    column_name=col_name,
                    details={"value": bad_value},
                ),
            )

    return pd.to_numeric(series, errors="coerce")


def col_to_binary(
    series: pd.Series, table_name: str, col_name: str, etl_context: ETLContext
) -> pd.Series:
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

    if etl_context.strict_validation and out.isna().any():
        bad_index = out.index[out.isna()][0]
        bad_value = series.loc[bad_index]
        raise errors.CleanerError(
            f"Invalid binary value '{bad_value}' at row {bad_index + etl_context.row_offset}",
            errors.ErrorContext(
                original_exception=None,
                rows=[int(bad_index) + etl_context.row_offset],
                table_name=table_name,
                column_name=col_name,
                details={"value": bad_value},
            ),
        )

    return out


def col_to_image(
    series: pd.Series, table_name: str, col_name: str, etl_context: ETLContext
) -> pd.Series:
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

    if etl_context.strict_validation and decoded.isna().any():
        bad_index = decoded.index[decoded.isna()][0]
        bad_value = series.loc[bad_index]

        raise errors.CleanerError(
            f"Invalid image data '{bad_value}' at row {bad_index + etl_context.row_offset}",
            errors.ErrorContext(
                original_exception=None,
                rows=[int(bad_index) + etl_context.row_offset],
                table_name=table_name,
                column_name=col_name,
                details={"value": bad_value},
            ),
        )

    return decoded


def trim_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trim leading and trailing whitespace in string columns.
    """
    for col in df.select_dtypes(include=["object", "string"]):
        df[col] = df[col].str.strip()
    return df


def standardise_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace common null-like strings with pd.NA in string columns.
    """
    return df.apply(
        lambda col: (
            col.replace(r"^(NA|N/A|NULL|None)$", pd.NA, regex=True)
            if col.dtype in ["object", "string"]
            else col
        )
    )


def remove_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows where all values are missing.
    """
    return df.dropna(how="all")


def log_column_changes(
    df: pd.DataFrame, table_name, etl_context: ETLContext, logger: Logger
) -> None:
    """
    Log differences between original and cleaned columns.

    Compares values by type and logs row-level changes using
    row_offset-adjusted row numbers.
    """
    logger.info(f"Audit of cleaned values for *** {table_name} ***:")
    cleaned_suffix = etl_context.cleaned_suffix
    # get the row offset from the context
    row_offset = etl_context.row_offset

    for col in df.columns:
        if col.endswith(cleaned_suffix):
            original_col = col.replace(cleaned_suffix, "")

            orig = df[original_col]
            clean = df[col]
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
                logger.info(f"No cleaned values for column {original_col}")
            else:
                for idx in df.index[diff_mask]:
                    logger.info(
                        f"{original_col} | Row {idx + row_offset} | "
                        f"'{df.at[idx, original_col]}' -> '{df.at[idx, col]}'"
                    )


def clean_data(
    df: DataFrame,
    cleaning_rules: dict[str, Any],
    table_name: str,
    column_config: dict[str, Any],
    etl_context: ETLContext,
) -> pd.DataFrame:
    """
    Clean a DataFrame using configured rules and column mappings.

    Applies global cleaning steps, then per-column transformations
    based on data types. Logs changes and returns the cleaned DataFrame.
    """
    # setup logger
    logger = logging_setup.get_logger(etl_context, __name__)

    # get the date parser - lazy create
    etl_context.datetime_parser = etl_context.get_datetime_parser(logger)

    # do automatic convert of data types e.g. object -> str
    df = df.convert_dtypes()

    # log 'before' info
    logger.info(f"Cleaning data for *** {table_name} ***. Shape: {df.shape}")

    # Get context info
    cleaned_suffix = etl_context.cleaned_suffix

    # apply global cleaning rules
    if cleaning_rules.get("trim_whitespace", False):
        df = trim_whitespace(df)
        logger.info(f"Trimmed whitespace for *** {table_name} ***")

    if cleaning_rules.get("standardise_nulls", False):
        df = standardise_nulls(df)
        logger.info(f"Standardised nulls for *** {table_name} ***")

    if cleaning_rules.get("remove_blank_rows", False):
        empty_rows, _ = df[df.isna().all(axis=1)].shape
        if empty_rows > 0:
            df = remove_empty_rows(df)
            logger.info(f"Removed {empty_rows} empty rows for *** {table_name} ***")

    # clean all columns according to the column rules
    for col_name, col_config in column_config.items():
        cleaned_col = f"{col_name}{cleaned_suffix}"
        logger.info(f"Cleaning column: {col_name}->{cleaned_col}")
        # data type - remember schema has already been validated in pipeline
        dtype = col_config["data_type"].split("(", 1)[0].strip()
        # data types and formats
        if dtype in ["char", "varchar", "text", "nchar", "nvarchar", "ntext"]:
            str_case = col_config.get("case", "asis")
            df[cleaned_col] = col_to_str(
                df[col_name],
                str_case=str_case,
            )
        elif dtype in ["tinyint", "smallint", "int", "bigint"]:
            df[cleaned_col] = col_to_int(
                df[col_name],
                table_name=table_name,
                col_name=col_name,
                etl_context=etl_context,
            )

        elif dtype == "numeric":
            df[cleaned_col] = col_to_numeric(
                df[col_name],
                table_name=table_name,
                col_name=col_name,
                etl_context=etl_context,
            )

        elif dtype in ["decimal", "float", "real"]:
            df[cleaned_col] = col_to_decimal_float_real(
                df[col_name],
                table_name=table_name,
                col_name=col_name,
                etl_context=etl_context,
            )

        elif dtype in ["binary", "varbinary"]:
            df[cleaned_col] = col_to_binary(
                df[col_name],
                table_name=table_name,
                col_name=col_name,
                etl_context=etl_context,
            )

        elif dtype == "image":
            df[cleaned_col] = col_to_image(
                df[col_name],
                table_name=table_name,
                col_name=col_name,
                etl_context=etl_context,
            )

        elif dtype == "bit":
            df[cleaned_col] = col_to_bit(
                df[col_name],
            )
        elif dtype == "date":
            df[cleaned_col] = col_to_date(
                df[col_name],
                table_name=table_name,
                col_name=col_name,
                etl_context=etl_context,
            )
        elif dtype == "datetime":
            df[cleaned_col] = col_to_datetime(
                df[col_name],
                table_name=table_name,
                col_name=col_name,
                etl_context=etl_context,
            )
        elif dtype == "time":
            df[cleaned_col] = col_to_time(
                df[col_name],
                table_name=table_name,
                col_name=col_name,
                etl_context=etl_context,
            )
        elif dtype in ["smallmoney", "money"]:
            df[cleaned_col] = col_to_money(
                df[col_name],
                table_name=table_name,
                col_name=col_name,
                etl_context=etl_context,
            )

    # log only cleaned columns
    log_column_changes(df, table_name, etl_context, logger)

    return df
