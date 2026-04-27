"""
Transformation engine for the Fynbyte Excel-to-SQL pipeline.

Applies value mappings, normalisation rules, derived column formulas,
phone number formatting, and other table-specific transformations.
Keeps original data intact and writes results into cleaned columns before finalisation.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"

import pandas as pd
from pandas import DataFrame
from pandas.api.types import is_string_dtype
import re
from logging import DEBUG

import src.logging_setup as logging_setup
from src.context import ETLContext

from config.project_config import ProjectConfig

from schemas.table_schema import TableSchema
from schemas.column_definitions import ColumnDefinition


class Transformer:
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

        self.normalised_cols: list[str] = []

    # Debug helpers
    def _trace(self, msg: str):
        """
        Emit a debug trace message when debug tracing is enabled.
        """
        if self.debug_trace and self.logger:
            self.logger.debug(f"[TRACE] {msg}")

    def _log_column_changes(self) -> None:
        """
        Log differences between original and cleaned values.

        Compares cleaned and cleaned columns by type and logs only rows where
        values changed, using row_offset-adjusted row numbers.
        """
        self.logger.info(
            f"Audit of transformed values for *** {self.table_schema.table_name} ***:"
        )
        # get the row offset from the context

        for col in self.normalised_cols:
            if col.endswith(self.etl_context.cleaned_suffix):
                original_col = col.replace(self.etl_context.cleaned_suffix, "")

                orig = self.df[original_col]
                cleaned = self.df[col]
                equal_mask = pd.Series(False, index=cleaned.index)

                # 1. Missing values equal
                both_missing = orig.isna() & cleaned.isna()

                # 2. String comparison (detects trimming)
                if pd.api.types.is_string_dtype(cleaned):
                    orig_str = orig.astype("string")
                    cleaned_str = cleaned.astype("string")
                    string_equal = orig_str.eq(cleaned_str)
                    equal_mask = both_missing | string_equal

                # 3. Numeric comparison
                elif pd.api.types.is_numeric_dtype(cleaned):
                    orig_num = pd.to_numeric(orig, errors="coerce")
                    cleaned_num = pd.to_numeric(cleaned, errors="coerce")
                    numeric_equal = orig_num.eq(cleaned_num)
                    equal_mask = both_missing | numeric_equal

                # 4. Dates comparison
                elif pd.api.types.is_datetime64_any_dtype(cleaned):
                    # Convert both to ISO8601 strings for comparison
                    orig_dt = pd.to_datetime(orig, errors="coerce")
                    cleaned_dt = pd.to_datetime(cleaned, errors="coerce")

                    orig_str = orig_dt.dt.strftime("%Y-%m-%d")
                    cleaned_str = cleaned_dt.dt.strftime("%Y-%m-%d")

                    string_equal = orig_str.eq(cleaned_str)
                    equal_mask = both_missing | string_equal

                diff_mask = ~equal_mask

                if not diff_mask.any():
                    self.logger.info(f"No transformed values for column {original_col}")
                else:
                    for idx in self.df.index[diff_mask]:
                        self.logger.info(
                            f"{original_col} | Row {self.project_config.spreadsheet_row_number(idx)} | "
                            f"'{self.df.at[idx, original_col]}' -> '{self.df.at[idx, col]}'"
                        )

    def _normalise_phone_numbers(
        self, series: pd.Series, col_config: ColumnDefinition, target_col: str
    ) -> pd.Series:
        """
        Normalise phone numbers to E.164-compatible format.

        Removes punctuation, handles 00 → + conversion, and optionally prefixes
        local numbers with the configured dialling code.
        """
        if col_config.validation is None:
            return series

        s = series
        if (
            is_string_dtype(self.df[col_config.column_name])
            and col_config.validation.get("format", "") == "E.164"
        ):
            self.normalised_cols.append(target_col)

            # remove spaces, dashes, brackets
            s = (
                s.astype(str)
                .str.replace(r"[^\d+]", "", regex=True)
                .str.replace(r"(?!^)\+", "", regex=True)
            )  # convert 00 prefix to +
            s = s.str.replace(r"^00", "+", regex=True)

            # if allow local numbers, normalise with dialling code if needed
            allow_local = col_config.validation.get("allow_local", False)
            if allow_local:
                dialling_prefix = col_config.validation.get("dialling_prefix")
                if dialling_prefix is not None:
                    needs_country = ~s.str.startswith("+") & s.notna()
                    s = s.str.replace(r"^0", "", regex=True)
                    s = s.where(~needs_country, dialling_prefix + s)
        return s

    def _apply_value_mapping(
        self, series: pd.Series, col_config: ColumnDefinition, target_col: str
    ) -> pd.Series:
        if col_config.value_mapping is None:
            return series

        # keep track of which columns were affected
        self.normalised_cols.append(target_col)

        # normalise values
        mapping = {}
        for normalised, raw_list in col_config.value_mapping.items():
            for raw_val in raw_list:
                mapping[str(raw_val).strip().lower()] = normalised
                self._trace(f"raw_val={str(raw_val)} normalised={str(normalised)}")

        return series.astype("string").str.strip().str.lower().map(mapping)

    def _apply_derived_column(
        self, col_config: ColumnDefinition, target_col: str
    ) -> None:
        """
        Compute a derived column using a vectorised pandas expression.

        Rewrites the formula to reference cleaned columns, evaluates it only
        where all dependencies are present, and sets invalid rows to NA.
        Logs rows where the formula could not be applied.
        """
        if col_config.validation is None:
            return

        # check derived columns
        derived_config = col_config.validation.get("derived_from", None)
        if derived_config is None:
            return

        depends_on = derived_config.get("depends_on", None)
        formula = derived_config.get("formula", None)
        # remember to work with cleaned cols
        if depends_on is None or formula is None:
            return

        self.normalised_cols.append(target_col)

        self.logger.info(f"Applying '{formula}' to {target_col}")

        # use correct colunms for formula
        # original_col = target_col.replace(self.etl_context.cleaned_suffix, "")
        depends_on_cleaned = [
            f"{col}{self.etl_context.cleaned_suffix}" for col in depends_on
        ]

        formula_cleaned = formula

        for idx, col in enumerate(depends_on):
            formula_cleaned = re.sub(
                rf"\b{col}\b", depends_on_cleaned[idx], formula_cleaned
            )

        # Identify rows where dependencies are complete
        mask_valid = self.df[depends_on_cleaned].notna().all(axis=1)

        # Compute only valid rows
        if mask_valid.any():
            self.df.loc[mask_valid, target_col] = self.df.loc[mask_valid].eval(
                formula_cleaned
            )

        # Set invalid rows to NA
        self.df.loc[~mask_valid, target_col] = pd.NA

        # Log invalid rows
        if (~mask_valid).any():
            invalid_indices = self.df.index[~mask_valid].tolist()
            print(f"invalid indices={invalid_indices}")
            row_numbers = [
                self.project_config.spreadsheet_row_number(r) for r in invalid_indices
            ]
            print(f"row numbers={row_numbers}")
            self.logger.warning(
                f"Cannot compute '{target_col}' for rows: {row_numbers}"
            )

    def transform_data(self) -> DataFrame:
        """
        Apply transformation rules defined in the column configuration.

        Handles value mappings, phone number normalisation, and derived column
        formulas.
        """
        # log start of transformation
        self.logger.info(
            f"Transforming data for *** {self.table_schema.table_name} ***"
        )

        # we work only with cleaned columns - keep original data intact
        cleaned_suffix = self.etl_context.cleaned_suffix

        for col in self.table_schema.columns:
            # ignore cleaned cols in this loop
            if col.column_name.endswith(cleaned_suffix):
                continue

            # log
            self.logger.info(f"Transforming column {col.column_name}")

            # set up
            cleaned_col = f"{col.column_name}{cleaned_suffix}"

            self.df[cleaned_col] = self._normalise_phone_numbers(
                self.df[cleaned_col], col, cleaned_col
            )

            self.df[cleaned_col] = self._apply_value_mapping(
                self.df[cleaned_col], col, cleaned_col
            )
            self._apply_derived_column(col, cleaned_col)

        if len(self.normalised_cols) > 0:
            # just log the changed columns
            self._log_column_changes()

        # return the dataframe
        return self.df
