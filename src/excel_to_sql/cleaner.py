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

from typing import Any
import pandas as pd
from pandas import DataFrame
import src.excel_to_sql.logging_setup as logging_setup


def clean_data(
    df: DataFrame, cleaning_rules: dict[str, Any], locale: str, context: dict[str, Any]
) -> DataFrame:
    """Clean data using cleaning rules from yaml."""
    logger = logging_setup.get_logger(context, __name__)
    logger.info(f"Pandas version {pd.__version__}")
    logger.info(f"Country code {locale}")
    logger.info("=== Before cleaning data ===")
    # logger.info(f"Shape: {df.shape}")
    # logger.info(f"Number of NaN/NaT:\n{df.isna().sum()}")

    logger.info("=== After cleaning data ===")
    # logger.info(f"Shape: {df.shape}")
    # logger.info(f"Number of NaN/NaT:\n{df.isna().sum()}")


# # Load the data
# df = pd.read_csv("messy_data.csv")
# df["email"] = df["email"].astype(str)
# df["mobile"] = df["mobile"].astype(str)

# # log before
# logger.info("=== BEFORE CLEANING ===")

# # Handle missing values
# df["amount"] = df["amount"].fillna(0)  # replace with default

# # Clean numeric columns by removing unwanted characters and spaces...
# df["amount_clean"] = (
#     df["amount"].astype(str).str.replace("R", "", regex=False).str.strip()
# )

# # ...and convert to numeric
# df["amount_clean"] = pd.to_numeric(df["amount_clean"], errors="coerce")

# # Clean date columns in two passes...
# if h.is_month_first_country(country_code):
#     df["date_clean"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=False)
# else:
#     df["date_clean"] = pd.to_datetime(
#         df["date"], errors="coerce", dayfirst=True
#     )  # ignore error message because we are handling different formats next

# # Use helper function looking for several formats
# mask = df["date_clean"].isna()
# df.loc[mask, "date_clean"] = df.loc[mask, "date"].apply(h.str_to_iso_date)

# # Check mobile number
# df["mobile_clean"] = df["mobile"].apply(h.validated_mobile_number)

# # Check email
# df["email_clean"] = df["email"].apply(h.validated_email)

# # log after
# logger.info("=== AFTER CLEANING ===")
# logger.info(f"Shape: {df.shape}")
# logger.info(
#     f'Number of NaN/NaT:\n{df[["amount_clean", "date_clean", "mobile_clean", "email_clean"]].isna().sum()}'
# )
