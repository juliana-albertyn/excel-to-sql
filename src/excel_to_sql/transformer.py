"""
Module: transformer
Purpose: Transform the data loaded into dataframe from excel

Responsibilities:
Compute derived columns (e.g., total_price = quantity * unit_price)
Split columns if needed
New derived columns needed by SQL
Pivoting / unpivoting
Exploding lists
Aggregating rows
Flattening nested data
Generate load timestamps
Add batch_id
Add ETL metadata columns
Add audit fields
Business logic like excluding certain rows e.g. cancelled orders

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"

from typing import Any
import pandas as pd
from pandas import DataFrame
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype, is_string_dtype
from logging import Logger


import src.excel_to_sql.logging_setup as logging_setup


def log_column_changes(
    df: pd.DataFrame, table_name: str, context: dict[str, Any], logger: Logger
) -> None:
    """log only changed values per table"""
    logger.info(f"Audit of transformed values for *** {table_name} ***:")
    # get the row offset from the context
    row_offset = context.get("row_offset", 1)

    for col in df.columns:
        if col.endswith("_normalised"):
            original_col = col.replace("_normalised", "")

            mask = df[original_col].astype("string").ne(df[col].astype("string"))

            if not mask.any():
                logger.info(f"No transformed values for {original_col}")
            else:
                for idx in df.index[mask]:
                    logger.info(
                        f"{original_col} | Row {idx + row_offset} | "
                        f"{df.at[idx, original_col]} -> {df.at[idx, col]}"
                    )


def normalise_phone_numbers(
    series: pd.Series, allow_local: bool, dialling_code: str
) -> pd.Series:
    s = series.astype("string")

    # remove spaces, dashes, brackets
    s = s.str.replace(r"[^\d+]", "", regex=True)

    # convert 00 prefix to +
    s = s.str.replace(r"^00", "+", regex=True)

    # if allow local numbers, normalise with dialling code if needed
    if allow_local:
        needs_country = ~s.str.startswith("+") & s.notna()
        s = s.where(~needs_country, dialling_code + s)
    return s


def apply_derived_column(
    df: pd.DataFrame,
    target_col: str,
    formula: str,
    depends_on: list[str],
    context: dict[str, Any],
    logger: Logger,
) -> pd.DataFrame:
    """
    Applies a vectorised derived column safely using pandas eval,
    only when all dependencies are present.
    """
    # log
    logger.info(f"Applying '{formula}' to {target_col}")

    row_offset = context.get("row_offset", 1)
    original_col = target_col.replace("_normalised", "")

    # use correct colunms for formula
    cleaned_suffix = context.get("cleaned_suffix", "")
    depends_on_cleaned = [f"{col}{cleaned_suffix}" for col in depends_on]

    formula_cleaned = formula
    for idx, col in enumerate(depends_on):
        formula_cleaned = formula_cleaned.replace(col, depends_on_cleaned[idx])

    # Identify rows where dependencies are complete
    mask_valid = df[depends_on_cleaned].notna().all(axis=1)

    # Compute only valid rows
    if mask_valid.any():
        df.loc[mask_valid, target_col] = df.loc[mask_valid].eval(formula_cleaned)

    # Set invalid rows to NA
    df.loc[~mask_valid, target_col] = pd.NA

    # 5️⃣ Log invalid rows
    if (~mask_valid).any():
        invalid_indices = (df.index[~mask_valid] + row_offset).tolist()
        logger.warning(f"Cannot compute '{original_col}' for rows: {invalid_indices}")

    return df


def transform_data(
    df: DataFrame,
    table_name: str,
    columns_config: dict[str, Any],
    context: dict[str, Any],
) -> DataFrame:
    """Use mapping configuration to transform the data"""

    # log start of transformation
    logger = logging_setup.get_logger(context, __name__)
    logger.info(f"Transforming data for *** {table_name} ***")

    # we work only with cleaned columns - keep original data intact
    normalised_cols = []
    cleaned_suffix = context.get("cleaned_suffix", "")

    for col_name, config in columns_config.items():
        # log
        logger.info(f"Transforming column {col_name}")

        # set up
        cleaned_col = f"{col_name}{cleaned_suffix}"
        normalised_col = col_name + "_normalised"

        # get the validation config
        value_map = None
        validation_config = config.get("validation", None)
        if validation_config is not None:
            value_map = validation_config.get("value_mapping", None)

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
            if is_numeric_dtype(df[cleaned_col]):
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
                        context=context,
                        logger=logger,
                    )

    if len(normalised_cols) > 0:
        # just log the changed columns
        log_column_changes(df, table_name, context, logger)

        # move the values from normalised columns into the cleaned columns and drop the normalised columns
        for col in normalised_cols:
            cleaned_col = col.replace("_normalised", cleaned_suffix)
            df[cleaned_col] = df[col]
            df.drop(columns=[col], inplace=True)

    # return the dataframe
    return df
