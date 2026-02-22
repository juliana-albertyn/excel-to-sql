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


def log_column_normalisation(df: pd.DataFrame, logger: Logger) -> None:
    """log only changed values per table"""
    logger.info(f"Audit of changed values for {df.table_name}:")
    for col in df.columns:
        if col.endswith("_normalised"):
            original_col = col.replace("_normalised", "")

            mask = df[original_col].astype("string").ne(df[col].astype("string"))

            for idx in df.index[mask]:
                logger.info(
                    f"{original_col} | Row {idx} | "
                    f"{df.at[idx, original_col]} -> {df.at[idx, col]}"
                )


def normalise_phone_numbers(series: pd.Series):
    s = series.astype("string")

    # remove spaces, dashes, brackets
    s = s.str.replace(r"[^\d+]", "", regex=True)

    # convert 00 prefix to +
    s = s.str.replace(r"^00", "+", regex=True)

    return s


import pandas as pd
import logging

logger = logging.getLogger(__name__)


def apply_derived_column(
    df: pd.DataFrame,
    target_col: str,
    formula: str,
    depends_on: list[str],
) -> pd.DataFrame:
    """
    Applies a vectorised derived column safely using pandas eval,
    only when all dependencies are present.
    """

    # Ensure dependencies exist
    missing_cols = [col for col in depends_on if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Cannot compute '{target_col}'. Missing columns: {missing_cols}"
        )

    # Identify rows where dependencies are complete
    mask_valid = df[depends_on].notna().all(axis=1)

    # Compute only valid rows
    if mask_valid.any():
        df.loc[mask_valid, target_col] = df.loc[mask_valid].eval(formula)

    # Set invalid rows to NA
    df.loc[~mask_valid, target_col] = pd.NA

    # 5️⃣ Log invalid rows
    if (~mask_valid).any():
        invalid_indices = df.index[~mask_valid].tolist()
        logger.warning(f"Cannot compute '{target_col}' for rows: {invalid_indices}")

    return df


def apply_mappings(
    df: DataFrame, column_config: dict[str, Any], context: dict[str, Any]
) -> DataFrame:
    """Use mapping configuration to transform the data"""

    # save attributes to reset later
    file_name = df.file_name
    table_name = df.table_name

    # log start of transformation
    logger = logging_setup.get_logger(context, __name__)
    logger.info("Transforming data for *** {table_name} ***")

    # we work only with cleaned columns - keep original data intact
    normalised_cols = []
    for col_name, config in column_config.items():
        # set up
        cleaned_col = col_name + "_cleaned"
        normalised_col = col_name + "_normalised"
        # check required columns are there

        # normalise coulmns
        value_map = config.get("value_mapping", "").lower()
        if len(value_map) != 0:
            # keep track of which columns were affected
            normalised_cols.extend(normalised_col)

            # normalise values
            mapping = {}
            for normalised, raw_list in config.items():
                for raw_val in raw_list:
                    mapping[str(raw_val).strip().lower()] = normalised

            df[normalised_col] = (
                df[cleaned_col].astype("string").str.strip().str.lower().map(mapping)
            )
        if is_string_dtype(df[col_name]) and config["format"] == "E.164":
            normalised_cols.extend(normalised_col)
            df[normalised_col] = (
                df[cleaned_col].astype("string").apply(normalise_phone_numbers)
            )

        # check derived columns
        if is_numeric_dtype(df[col_name]) and config.get("derived_from", "") != "":
            normalised_cols.extend(normalised_col)
            df = apply_derived_column(
                df=df,
                target_col=normalised_col,
                formula=config["formula"],
                depends_on=config["depends_on"],
            )

    if len(normalised_cols) > 0:
        # just log the changed columns
        log_column_normalisation(df, logger)

        # move the values from normalised columns into the cleaned columns and drop the normalised columns
        for col in normalised_cols:
            cleaned_col = col.replace("_normalised", "_cleaned")
            df[cleaned_col] = df[col]
            df.drop(columns=[col], inplace=True)

    # reset the attributes
    df.file_name = file_name
    df.table_name = table_name

    # return the dataframe
    return df
