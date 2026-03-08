"""
Module: cleaner
Purpose: Clean data imported into pandas from excel.

Responsibilities:
Drop empty rows
Trim whitespace
Replace NaN
Normalize case
Strip special characters
Standardize formats
Normalize date formats
Convert strings to datetime
Remove obvious junk
Apply locale rules
Standardize timezone if needed

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-08"

from typing import Any, Optional
import pandas as pd
from pandas import DataFrame
from datetime import date, datetime, time
from logging import Logger
import base64
from binascii import Error as BinasciiError

import src.excel_to_sql.logging_setup as logging_setup
import src.excel_to_sql.errors as errors


def str_to_bool(value: str | bool) -> bool:
    """
    Convert common truthy strings to a boolean.
    Accepts 'true', 'yes', '1' (case-insensitive).
    Returns True for those, False otherwise.
    """
    if isinstance(value, str):
        return value.strip().lower() in ("true", "yes", "1")
    return bool(value)  # fallback for non-strings


def str_to_iso_date(value: str | date, day_first_format: bool) -> Optional[date]:
    "if date, return as-is, else if a str, convert to a date in iso format YYYY-MM-DD"
    if isinstance(value, date):
        return value
    elif isinstance(value, str):
        value = value.strip()
        format_list = [
            "%d-%b-%Y",
            "%d %b %Y",
            "%b %d, %Y",
            "%d-%B-%Y",
            "%d %B %Y",
            "%Y%m%d",
            "%Y/%m/%d",
            "%B %d, %Y",
        ]
        if day_first_format:
            format_list.extend(["%d%m%Y", "%d/%m/%Y"])
        else:
            format_list.extend(["%m%d%Y", "%m/%d/%Y"])

        for fmt in format_list:
            try:
                return datetime.strptime(value, fmt).date()
            except (ValueError, TypeError):
                continue
        return
    else:
        return


def str_to_iso_datetime(
    value: str | datetime, day_first_format: bool
) -> Optional[datetime]:
    """
    Convert a string into a datetime.datetime object using a list of common
    date+time formats. Returns None if parsing fails.
    """
    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        value = value.strip()

        # Base formats (date + time)
        format_list = [
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d %H:%M:%S",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "%Y%m%d %H:%M",
            "%Y%m%d %H:%M:%S",
            # 12-hour formats
            "%Y-%m-%d %I:%M %p",
            "%Y/%m/%d %I:%M %p",
            "%b %d, %Y %I:%M %p",
            "%B %d, %Y %I:%M %p",
        ]

        # Day-first variants
        if day_first_format:
            format_list.extend(
                [
                    "%d/%m/%Y %H:%M",
                    "%d/%m/%Y %H:%M:%S",
                    "%d-%m-%Y %H:%M",
                    "%d-%m-%Y %H:%M:%S",
                    "%d-%m-%Y %I:%M %p",
                    "%d/%m/%Y %I:%M %p",
                ]
            )
        else:
            format_list.extend(
                [
                    "%m/%d/%Y %H:%M",
                    "%m/%d/%Y %H:%M:%S",
                    "%m-%d-%Y %H:%M",
                    "%m-%d-%Y %H:%M:%S",
                    "%m-%d-%Y %I:%M %p",
                    "%m/%d/%Y %I:%M %p",
                ]
            )

        for fmt in format_list:
            try:
                return datetime.strptime(value, fmt)
            except (ValueError, TypeError):
                continue

    return None


def str_to_iso_time(value: str | datetime | date) -> Optional[time]:
    """
    Convert a string into a datetime.time object using common time formats.
    Returns None if parsing fails.
    """
    # If it's already a datetime or date, extract time if present
    if isinstance(value, datetime):
        return value.time()
    if isinstance(value, date):
        return None  # dates have no time component

    if isinstance(value, str):
        value = value.strip()

        format_list = [
            "%H:%M",
            "%H:%M:%S",
            "%I:%M %p",
            "%I:%M:%S %p",
        ]

        for fmt in format_list:
            try:
                return datetime.strptime(value, fmt).time()
            except (ValueError, TypeError):
                continue

    return None


def col_to_date(
    series: pd.Series, strict: bool, day_first: bool, table_name: str, col_name: str
) -> pd.Series:
    iso_fmt = "%Y-%m-%d"
    secondary_fmt = "%d-%m-%Y" if day_first else "%m-%d-%Y"

    if strict:
        try:
            return pd.to_datetime(series, errors="raise", format=iso_fmt)
        except Exception as e:
            temp = pd.to_datetime(series, errors="coerce", format=iso_fmt)
            mask = temp.isna()
            bad_index = series.index[mask][0]
            bad_value = series.loc[bad_index]
            raise errors.CleanerError(
                f"Invalid date '{bad_value}' at row {bad_index}",
                errors.ErrorContext(
                    original_exception=e,
                    rows=[int(bad_index)],
                    table_name=table_name,
                    column_name=col_name,
                    details={"value": bad_value, "format": iso_fmt},
                ),
            )

    # permissive mode
    out = pd.to_datetime(series, errors="coerce", format=iso_fmt)
    mask = out.isna()
    out.loc[mask] = pd.to_datetime(
        series.loc[mask], errors="coerce", format=secondary_fmt
    )
    mask = out.isna()
    out.loc[mask] = series.loc[mask].apply(str_to_iso_date, day_first_format=day_first)
    return out


def col_to_datetime(
    series: pd.Series, strict: bool, day_first: bool, table_name: str, col_name: str
) -> pd.Series:
    iso_fmt = "%Y-%m-%dT%H:%M:%S"
    secondary_fmt = "%d-%m-%YT%H:%M:%S" if day_first else "%m-%d-%YT%H:%M:%S"

    if strict:
        try:
            return pd.to_datetime(series, errors="raise", format=iso_fmt)
        except Exception as e:
            temp = pd.to_datetime(series, errors="coerce", format=iso_fmt)
            mask = temp.isna()
            bad_index = series.index[mask][0]
            bad_value = series.loc[bad_index]
            raise errors.CleanerError(
                f"Invalid datetime '{bad_value}' at row {bad_index}",
                errors.ErrorContext(
                    original_exception=e,
                    rows=[int(bad_index)],
                    table_name=table_name,
                    column_name=col_name,
                    details={"value": bad_value, "format": iso_fmt},
                ),
            )

    # permissive mode
    out = pd.to_datetime(series, errors="coerce", format=iso_fmt)
    mask = out.isna()
    out.loc[mask] = pd.to_datetime(
        series.loc[mask], errors="coerce", format=secondary_fmt
    )
    mask = out.isna()
    out.loc[mask] = series.loc[mask].apply(
        str_to_iso_datetime, day_first_format=day_first
    )
    return out


def col_to_time(
    series: pd.Series, strict: bool, table_name: str, col_name: str
) -> pd.Series:
    """
    Convert a pandas Series to Python datetime.time objects.
    Uses str_to_iso_time for parsing.
    """

    # Convert everything using helper
    out = series.map(str_to_iso_time)

    if strict:
        mask = out.isna() & series.notna()

        if mask.any():
            bad_index = series.index[mask][0]
            bad_value = series.loc[bad_index]

            raise errors.CleanerError(
                f"Invalid time '{bad_value}' at row {bad_index}",
                errors.ErrorContext(
                    original_exception=None,
                    rows=[int(bad_index)],
                    table_name=table_name,
                    column_name=col_name,
                    details={"value": bad_value},
                ),
            )

    return out.astype("object")


def col_to_money(
    series: pd.Series,
    strict: bool,
    currency_symbol: str,
    table_name: str,
    col_name: str,
) -> pd.Series:
    """
    Clean a money/smallmoney column:
    - remove currency symbols
    - strip whitespace
    - convert to numeric
    - strict/permissive behaviour
    """
    # Step 1: remove currency symbol and whitespace
    cleaned = (
        series.astype(str).str.replace(currency_symbol, "", regex=False).str.strip()
    )

    if strict:
        try:
            return pd.to_numeric(cleaned, errors="raise")
        except Exception as e:
            # detect failing rows
            temp = pd.to_numeric(cleaned, errors="coerce")
            mask = temp.isna()

            bad_index = series.index[mask][0]
            bad_value = series.loc[bad_index]

            raise errors.CleanerError(
                f"Invalid money value '{bad_value}' at row {bad_index}",
                errors.ErrorContext(
                    original_exception=e,
                    rows=[int(bad_index)],
                    table_name=table_name,
                    column_name=col_name,
                    details={"value": bad_value},
                ),
            )

    # permissive mode
    out = pd.to_numeric(cleaned, errors="coerce")
    return out


def col_to_str(
    series: pd.Series,
    str_case: str,
    table_name: str,
    col_name: str,
) -> pd.Series:
    """
    Clean a string/text column:
    - trim whitespace
    - apply case rules (lower/upper/title/asis)
    - convert empty strings and 'nan' to pd.NA
    - return pandas string dtype
    """
    # convert to string and trim
    cleaned = series.astype(str).str.strip()

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


def col_to_bit(
    series: pd.Series,
    table_name: str,
    col_name: str,
) -> pd.Series:
    """
    Clean a BIT column:
    - convert truthy strings to True
    - convert falsy strings to False
    - preserve existing booleans
    - return a boolean Series
    """
    # apply helper
    cleaned = series.apply(str_to_bool)

    # Ensure pandas boolean dtype
    return cleaned.astype("boolean")


def col_to_int(
    series: pd.Series,
    strict: bool,
    table_name: str,
    col_name: str,
) -> pd.Series:
    """
    Clean integer-like columns: tinyint, smallint, int, bigint.
    """
    if strict:
        try:
            return pd.to_numeric(series, errors="raise").astype("Int64")
        except Exception as e:
            temp = pd.to_numeric(series, errors="coerce")
            mask = temp.isna()

            bad_index = series.index[mask][0]
            bad_value = series.loc[bad_index]

            raise errors.CleanerError(
                f"Invalid integer '{bad_value}' at row {bad_index}",
                errors.ErrorContext(
                    original_exception=e,
                    rows=[int(bad_index)],
                    table_name=table_name,
                    column_name=col_name,
                    details={"value": bad_value},
                ),
            )

    # permissive mode
    out = pd.to_numeric(series, errors="coerce").astype("Int64")
    return out


def col_to_numeric(
    series: pd.Series,
    strict: bool,
    table_name: str,
    col_name: str,
) -> pd.Series:
    """
    Clean SQL NUMERIC columns.
    """
    if strict:
        try:
            return pd.to_numeric(series, errors="raise")
        except Exception as e:
            temp = pd.to_numeric(series, errors="coerce")
            mask = temp.isna()

            bad_index = series.index[mask][0]
            bad_value = series.loc[bad_index]

            raise errors.CleanerError(
                f"Invalid numeric value '{bad_value}' at row {bad_index}",
                errors.ErrorContext(
                    original_exception=e,
                    rows=[int(bad_index)],
                    table_name=table_name,
                    column_name=col_name,
                    details={"value": bad_value},
                ),
            )

    return pd.to_numeric(series, errors="coerce")


def col_to_decimal_float_real(
    series: pd.Series,
    strict: bool,
    table_name: str,
    col_name: str,
) -> pd.Series:
    """
    Clean SQL DECIMAL, FLOAT, REAL columns.
    """
    if strict:
        try:
            return pd.to_numeric(series, errors="raise")
        except Exception as e:
            temp = pd.to_numeric(series, errors="coerce")
            mask = temp.isna()

            bad_index = series.index[mask][0]
            bad_value = series.loc[bad_index]

            raise errors.CleanerError(
                f"Invalid decimal/float value '{bad_value}' at row {bad_index}",
                errors.ErrorContext(
                    original_exception=e,
                    rows=[int(bad_index)],
                    table_name=table_name,
                    column_name=col_name,
                    details={"value": bad_value},
                ),
            )

    return pd.to_numeric(series, errors="coerce")


def col_to_binary(
    series: pd.Series,
    strict: bool,
    table_name: str,
    col_name: str,
) -> pd.Series:
    """
    Clean BINARY / VARBINARY columns.
    Expect hex strings or byte-like values.
    """
    cleaned = series.astype(str).str.strip()

    def to_bytes(val: str):
        if val in ("", "nan"):
            return None
        try:
            return bytes.fromhex(val)
        except Exception:
            return None

    out = cleaned.apply(to_bytes)

    if strict and out.isna().any():
        bad_index = out.index[out.isna()][0]
        bad_value = series.loc[bad_index]
        raise errors.CleanerError(
            f"Invalid binary value '{bad_value}' at row {bad_index}",
            errors.ErrorContext(
                original_exception=None,
                rows=[int(bad_index)],
                table_name=table_name,
                column_name=col_name,
                details={"value": bad_value},
            ),
        )

    return out


def col_to_image(
    series: pd.Series,
    strict: bool,
    table_name: str,
    col_name: str,
) -> pd.Series:
    """
    Validate image data:
    - Accept hex strings, base64 strings, or raw bytes
    - Decode to bytes
    - Validate magic bytes for JPEG, PNG, GIF, BMP
    - Strict mode: raise on first invalid value
    - Permissive mode: invalid values become pd.NA
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

    if strict and decoded.isna().any():
        bad_index = decoded.index[decoded.isna()][0]
        bad_value = series.loc[bad_index]

        raise errors.CleanerError(
            f"Invalid image data '{bad_value}' at row {bad_index}",
            errors.ErrorContext(
                original_exception=None,
                rows=[int(bad_index)],
                table_name=table_name,
                column_name=col_name,
                details={"value": bad_value},
            ),
        )

    return decoded


def trim_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """get rid of whitespace in the data frame"""
    for col in df.select_dtypes(include="object"):
        df[col] = df[col].str.strip()
    return df


def standardise_nulls(df: pd.DataFrame) -> pd.DataFrame:
    "standardise nulls to pd.NA"
    return df.apply(
        lambda col: (
            col.replace(r"^(NA|N/A|NULL|None)$", pd.NA, regex=True)
            if col.dtype in ["object", "string"]
            else col
        )
    )


def remove_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove empty rows"""
    return df.dropna(how="all")


def log_column_changes(
    df: pd.DataFrame, table_name, context: dict[str, Any], logger: Logger
) -> None:
    """log only changed values per table"""
    logger.info(f"Audit of cleaned values for *** {table_name} ***:")
    cleaned_suffix = context.get("cleaned_suffix", "")
    # get the row offset from the context
    row_offset = context.get("row_offset", 1)

    for col in df.columns:
        if col.endswith("_cleaned"):
            original_col = col.replace(cleaned_suffix, "")

            left_num = pd.to_numeric(df[original_col], errors="coerce")
            right_num = pd.to_numeric(df[col], errors="coerce")

            numeric_diff = left_num.ne(right_num)

            string_diff = df[original_col].astype("string").ne(df[col].astype("string"))

            mask = numeric_diff & string_diff

            if not mask.any():
                logger.info(f"No cleaned values for column {original_col}")
            else:
                for idx in df.index[mask]:
                    logger.info(
                        f"{original_col} | Row {idx + row_offset} | "
                        f"{df.at[idx, original_col]} -> {df.at[idx, col]}"
                    )


def clean_data(
    df: DataFrame,
    cleaning_rules: dict[str, Any],
    table_name: str,
    column_config: dict[str, Any],
    context: dict[str, Any],
) -> pd.DataFrame:
    """Clean data using cleaning rules from yaml."""
    # Step 1: do automatic convert of data types e.g. object -> str
    df = df.convert_dtypes()

    # Step 2: log 'before' info
    logger = logging_setup.get_logger(context, __name__)
    logger.info(f"Cleaning data for *** {table_name} ***. Shape: {df.shape}")

    # Step 3: Get context info
    cleaned_suffix = context.get("cleaned_suffix", "")
    strict_validation = context.get("strict_validation", True)

    # Step 4: apply global cleaning rules
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

    # Step 5: get column cleaning ruls for currency and dates
    currency_symbol = cleaning_rules.get("currency_symbol", "")
    day_first_format = cleaning_rules.get("day_first_format", True)

    # Step 6: clean all columns according to the column rules
    for col_name, col_config in column_config.items():
        cleaned_col = f"{col_name}{cleaned_suffix}"
        logger.info(f"Cleaning column: {col_name}->{cleaned_col}")
        # data type - remember schema has already been validated in extractor
        dtype = col_config["data_type"].split("(", 1)[0].strip()
        # data types and formats
        if dtype in ["char", "varchar", "text", "nchar", "nvarchar", "ntext"]:
            str_case = col_config.get("case", "asis")
            df[cleaned_col] = col_to_str(
                df[col_name],
                str_case=str_case,
                table_name=table_name,
                col_name=col_name,
            )
        elif dtype in ["tinyint", "smallint", "int", "bigint"]:
            df[cleaned_col] = col_to_int(
                df[col_name],
                strict=strict_validation,
                table_name=table_name,
                col_name=col_name,
            )

        elif dtype == "numeric":
            df[cleaned_col] = col_to_numeric(
                df[col_name],
                strict=strict_validation,
                table_name=table_name,
                col_name=col_name,
            )

        elif dtype in ["decimal", "float", "real"]:
            df[cleaned_col] = col_to_decimal_float_real(
                df[col_name],
                strict=strict_validation,
                table_name=table_name,
                col_name=col_name,
            )

        elif dtype in ["binary", "varbinary"]:
            df[cleaned_col] = col_to_binary(
                df[col_name],
                strict=strict_validation,
                table_name=table_name,
                col_name=col_name,
            )

        elif dtype == "image":
            df[cleaned_col] = col_to_image(
                df[col_name],
                strict=strict_validation,
                table_name=table_name,
                col_name=col_name,
            )

        elif dtype == "bit":
            df[cleaned_col] = col_to_bit(
                df[col_name],
                table_name=table_name,
                col_name=col_name,
            )
        elif dtype == "date":
            df[cleaned_col] = col_to_date(
                df[col_name],
                strict=strict_validation,
                day_first=day_first_format,
                table_name=table_name,
                col_name=col_name,
            )
        elif dtype == "datetime":
            df[cleaned_col] = col_to_datetime(
                df[col_name],
                strict=strict_validation,
                day_first=day_first_format,
                table_name=table_name,
                col_name=col_name,
            )
        elif dtype == "time":
            df[cleaned_col] = col_to_time(
                df[col_name],
                strict=strict_validation,
                table_name=table_name,
                col_name=col_name,
            )
        elif dtype in ["smallmoney", "money"]:
            df[cleaned_col] = col_to_money(
                df[col_name],
                currency_symbol=currency_symbol,
                strict=strict_validation,
                table_name=table_name,
                col_name=col_name,
            )

    # Step 7: log only cleaned columns
    log_column_changes(df, table_name, context, logger)

    return df
