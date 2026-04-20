"""
Module: test_transformer
Purpose: STesting transformer functions.

This module is part of the Fynbyte toolkit. 
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-03-29"


import pandas as pd
from src.transformer import normalise_phone_numbers


def test_normalise_phone_numbers_basic():
    s = pd.Series(["082 123 4567", "002712345678", None])

    result = normalise_phone_numbers(s, allow_local=True, dialling_code="+27")

    expected = pd.Series(["+27821234567", "+2712345678", None], dtype="string")

    assert result.equals(expected)
