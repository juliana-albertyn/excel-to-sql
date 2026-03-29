"""
Transformation engine for the Fynbyte Excel-to-SQL pipeline.

Applies value mappings, normalisation rules, derived column formulas,
phone number formatting, and other table-specific transformations.
Keeps original data intact and writes results into cleaned columns before finalisation.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"

from typing import Any
import pandas as pd
from pandas import DataFrame
from pandas.api.types import is_string_dtype
from logging import Logger
import re

import src.excel_to_sql.logging_setup as logging_setup
import src.excel_to_sql.context as context


def log_column_changes(
    df: pd.DataFrame, table_name, etl_context: context.ETLContext, logger: Logger
) -> None:
    """
    Log differences between original and normalised values.

    Compares cleaned and normalised columns by type and logs only rows where
    values changed, using row_offset-adjusted row numbers.
    """
    logger.info(f"Audit of transformed values for *** {table_name} ***:")
    normalised_suffix = etl_context.normalised_suffix
    # get the row offset from the context
    row_offset = etl_context.row_offset

    for col in df.columns:
        if col.endswith(normalised_suffix):
            original_col = col.replace(normalised_suffix, "")

            orig = df[original_col]
            normalised = df[col]
            equal_mask = pd.Series(False, index=normalised.index)

            # 1. Missing values equal
            both_missing = orig.isna() & normalised.isna()

            # 2. String comparison (detects trimming)
            if pd.api.types.is_string_dtype(normalised):
                orig_str = orig.astype("string")
                normalised_str = normalised.astype("string")
                string_equal = orig_str.eq(normalised_str)
                equal_mask = both_missing | string_equal

            # 3. Numeric comparison
            elif pd.api.types.is_numeric_dtype(normalised):
                orig_num = pd.to_numeric(orig, errors="coerce")
                normalised_num = pd.to_numeric(normalised, errors="coerce")
                numeric_equal = orig_num.eq(normalised_num)
                equal_mask = both_missing | numeric_equal

            # 4. Dates comparison
            elif pd.api.types.is_datetime64_any_dtype(normalised):
                # Convert both to ISO8601 strings for comparison
                orig_dt = pd.to_datetime(orig, errors="coerce")
                normalised_dt = pd.to_datetime(normalised, errors="coerce")

                orig_str = orig_dt.dt.strftime("%Y-%m-%d")
                normalised_str = normalised_dt.dt.strftime("%Y-%m-%d")

                string_equal = orig_str.eq(normalised_str)
                equal_mask = both_missing | string_equal

            diff_mask = ~equal_mask

            if not diff_mask.any():
                logger.info(f"No transformed values for column {original_col}")
            else:
                for idx in df.index[diff_mask]:
                    logger.info(
                        f"{original_col} | Row {idx + row_offset} | "
                        f"'{df.at[idx, original_col]}' -> '{df.at[idx, col]}'"
                    )


def normalise_phone_numbers(
    series: pd.Series, allow_local: bool, dialling_code: str
) -> pd.Series:
    """
    Normalise phone numbers to E.164-compatible format.

    Removes punctuation, handles 00 → + conversion, and optionally prefixes
    local numbers with the configured dialling code.
    """    
    s = series.astype("string")

    # remove spaces, dashes, brackets
    s = s.str.replace(r"(?!^\+)[^\d]", "", regex=True)
    # convert 00 prefix to +
    s = s.str.replace(r"^00", "+", regex=True)

    # if allow local numbers, normalise with dialling code if needed
    if allow_local:
        needs_country = ~s.str.startswith("+") & s.notna()
        s = s.str.replace(r"^0", "", regex=True)
        s = s.where(~needs_country, dialling_code + s)
    return s


def apply_derived_column(
    df: pd.DataFrame,
    target_col: str,
    formula: str,
    depends_on: list[str],
    etl_context: context.ETLContext,
    logger: Logger,
) -> pd.DataFrame:
    """
    Compute a derived column using a vectorised pandas expression.

    Rewrites the formula to reference cleaned columns, evaluates it only
    where all dependencies are present, and sets invalid rows to NA.
    Logs rows where the formula could not be applied.
    """
    # log
    logger.info(f"Applying '{formula}' to {target_col}")

    row_offset = etl_context.row_offset
    original_col = target_col.replace(etl_context.normalised_suffix, "")

    # use correct colunms for formula
    cleaned_suffix = etl_context.cleaned_suffix
    depends_on_cleaned = [f"{col}{cleaned_suffix}" for col in depends_on]

    formula_cleaned = formula

    for idx, col in enumerate(depends_on):
        formula_cleaned = re.sub(
            rf"\b{col}\b", depends_on_cleaned[idx], formula_cleaned
        )

    # Identify rows where dependencies are complete
    mask_valid = df[depends_on_cleaned].notna().all(axis=1)

    # Compute only valid rows
    if mask_valid.any():
        df.loc[mask_valid, target_col] = df.loc[mask_valid].eval(formula_cleaned)

    # Set invalid rows to NA
    df.loc[~mask_valid, target_col] = pd.NA

    # Log invalid rows
    if (~mask_valid).any():
        invalid_indices = (df.index[~mask_valid] + row_offset).tolist()
        logger.warning(f"Cannot compute '{original_col}' for rows: {invalid_indices}")

    return df


def transform_data(
    df: DataFrame,
    table_name: str,
    columns_config: dict[str, Any],
    etl_context: context.ETLContext,
) -> DataFrame:
    """
    Apply transformation rules defined in the column configuration.

    Handles value mappings, phone number normalisation, and derived column
    formulas. Logs changed values, moves normalised results into cleaned
    columns, and drops temporary normalised columns.
    """
    # log start of transformation
    logger = logging_setup.get_logger(etl_context, __name__)
    logger.info(f"Transforming data for *** {table_name} ***")

    # we work only with cleaned columns - keep original data intact
    normalised_cols = []
    cleaned_suffix = etl_context.cleaned_suffix
    normalised_suffix = etl_context.normalised_suffix

    for col_name, config in columns_config.items():
        # log
        logger.info(f"Transforming column {col_name}")

        # set up
        cleaned_col = f"{col_name}{cleaned_suffix}"
        normalised_col = f"{col_name}{normalised_suffix}"

        # get the validation mapping
        value_map = config.get("value_mapping", None)

        # normalise columns
        if value_map is not None:
            # keep track of which columns were affected
            normalised_cols.append(normalised_col)

            # normalise values
            mapping = {}
            for normalised, raw_list in value_map.items():
                for raw_val in raw_list:
                    mapping[str(raw_val).strip().lower()] = normalised

            df[normalised_col] = (
                df[cleaned_col].astype("string").str.strip().str.lower().map(mapping)
            )

        # get the validation configuration
        validation_config = config.get("validation", None)
        if validation_config is not None:
            if (
                is_string_dtype(df[col_name])
                and validation_config.get("format", "") == "E.164"
            ):
                normalised_cols.append(normalised_col)
                dialling_prefix = validation_config.get("dialling_prefix", "")
                allow_local = validation_config.get("allow_local", False)
                df[normalised_col] = normalise_phone_numbers(
                    df[cleaned_col], allow_local, dialling_prefix
                )

            # check derived columns
            derived_config = validation_config.get("derived_from", None)
            if derived_config is None:
                continue
            depends_on = derived_config.get("depends_on", None)
            formula = derived_config.get("formula", None)
            # remember to work with cleaned cols
            if depends_on is not None and formula is not None:
                normalised_cols.append(normalised_col)
                df = apply_derived_column(
                    df=df,
                    target_col=normalised_col,
                    formula=formula,
                    depends_on=depends_on,
                    etl_context=etl_context,
                    logger=logger,
                )

    if len(normalised_cols) > 0:
        # just log the changed columns
        log_column_changes(df, table_name, etl_context, logger)

        # move the values from normalised columns into the cleaned columns and drop the normalised columns
        for col in normalised_cols:
            cleaned_col = col.replace(normalised_suffix, cleaned_suffix)
            if cleaned_col in df.columns:
                logger.warning(f"Overwriting {cleaned_col}")
            df[cleaned_col] = df[col]
            df.drop(columns=[col], inplace=True)

    # return the dataframe
    return df
