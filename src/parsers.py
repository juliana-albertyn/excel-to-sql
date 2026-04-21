"""
Date, time, and datetime parsing utilities for the Fynbyte pipeline.

Provides a unified parser with caching, normalisation, multiple parsing
strategies, and optional debug tracing. Used by the cleaner to ensure
consistent and locale-aware conversions.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__date__ = "2026-02-19"

from datetime import datetime, date, timezone
from typing import Optional, Any, ContextManager
import pandas as pd

import dateparser
from logging import Logger
from utils.helpers import Timer


class DateTimeParser:
    """
    Unified parser for date, time, and datetime values.

    Normalises input, applies fast pandas parsing, falls back to dateparser,
    and finally tries manual formats. Includes caching, timezone handling,
    and optional debug tracing.
    """

    def __init__(
        self,
        day_first: bool = True,
        min_date: Optional[datetime] = None,
        locale: str = "en_ZA",
        fallback_locales: list[str] = ["af_ZA", "en_ZA"],
        logger: Optional[Logger] = None,
        debug_trace: bool = False,
    ):
        """
        Initialise a DateTimeParser.

        Configures locale, fallback locales, minimum date, day-first behaviour,
        debug tracing, and manual fallback formats. Creates an empty parse cache.
        """
        self.day_first = day_first
        self.min_date = min_date or datetime(1900, 1, 1)
        self.locale = locale
        self.fallback_locales = fallback_locales
        self.logger = logger
        self.debug_trace = debug_trace

        self.cache: dict[str, Optional[datetime]] = {}

        self.date_formats = [
            "%d-%b-%Y",
            "%d %b %Y",
            "%b %d %Y",
            "%d-%B-%Y",
            "%d %B %Y",
            "%B %d %Y",
            "%Y%m%d",
            "%Y/%m/%d",
        ]

        if self.day_first:
            self.date_formats.extend(["%d%m%Y", "%d/%m/%Y"])
        else:
            self.date_formats.extend(["%m%d%Y", "%m/%d/%Y"])

        self.time_formats = [
            "%H:%M",
            "%H:%M:%S",
            "%I:%M %p",
            "%I:%M:%S %p",
            "%I:%M%p",
            "%I:%M:%S%p",
        ]

    # Debug helpers
    def _trace(self, msg: str):
        """
        Emit a debug trace message when debug tracing is enabled.
        """
        if self.debug_trace and self.logger:
            self.logger.debug(f"[TRACE] {msg}")

    def _timed(self, label: str) -> ContextManager:
        return Timer(label, self.logger, self.debug_trace)

    # Normalisation helpers
    def _normalise(self, value: str) -> str:
        """
        Normalise a raw string before parsing.

        Strips whitespace, replaces common separators, collapses repeated spaces,
        and prepares the value for fast parsing.
        """
        raw = value
        value = value.strip()
        value = value.replace("T", " ")
        value = value.replace(",", " ")
        value = " ".join(value.split())

        self._trace(f"Normalised '{raw}' -> '{value}'")
        return value

    def _normalise_timezone(self, dt: datetime) -> datetime:
        """
        Convert timezone-aware datetimes to naive UTC for consistent comparison.
        """
        if dt.tzinfo is None:
            return dt
        return dt.astimezone(timezone.utc).replace(tzinfo=None)

    # sanity checks
    def _looks_like_time(self, value: str) -> bool:
        """
        Lightweight check to determine whether a string resembles a time value.
        """
        if value.strip() == "":
            return False

        # must contain at least one colon
        if ":" not in value:
            return False

        # normalise AM/PM
        v = value.strip()
        v_upper = v.upper()

        has_ampm = False
        if v_upper.endswith("AM") or v_upper.endswith("PM"):
            has_ampm = True
            v = v[:-2].strip()  # remove AM/PM

        # reject any remaining alphabetic characters
        if any(c.isalpha() for c in v):
            return False

        # split into parts
        parts = v.split(":")
        if len(parts) not in (2, 3):
            return False

        # safe integer conversion
        def safe_int(x):
            try:
                return int(x)
            except Exception:
                return None

        h = safe_int(parts[0])
        m = safe_int(parts[1])
        s = safe_int(parts[2]) if len(parts) == 3 else 0

        if h is None or m is None or s is None:
            return False

        # hour range
        if has_ampm:
            # 12-hour clock
            if h < 1 or h > 12:
                return False
        else:
            # 24-hour clock
            if h < 0 or h > 23:
                return False

        # minute + second range
        if m < 0 or m > 59:
            return False
        if s < 0 or s > 59:
            return False

        return True

    def _looks_like_date(self, value: str) -> bool:
        """
        Lightweight check to determine whether a string resembles a date value.
        """
        value = value.strip()
        if not value:
            return False

        if not any(c.isnumeric() for c in value):
            return False

        # must contain at least one separator
        if not any(sep in value for sep in ("/", "-", ",", " ")):
            return False

        # normalise
        v = value.replace(",", "-")
        v = value.replace(" ", "-")
        parts = v.replace("/", "-").split("-")

        # must be at least 3 parts
        if len(parts) < 3:
            return False

        # safe integer conversion
        def safe_int(x):
            try:
                return int(x)
            except Exception:
                return None

        nums = [safe_int(p) for p in parts]

        # ambiguous - can't do any more checking here
        if any(num is None for num in nums):
            return True

        # interpret based on day_first - remember month might be in alpha format, so will be None, just deal with it later
        if nums[0] is not None and nums[0] >= self.min_date.year:
            year, month, day, *_ = nums
        elif self.day_first:
            day, month, year, *_ = nums
        else:
            month, day, year, *_ = nums

        # quick range checks
        if month is not None and not (1 <= month <= 12):
            return False
        if day is not None and not (1 <= day <= 31):
            return False
        if year is not None and year < self.min_date.year:
            return False

        # final validation using datetime
        if year is not None and month is not None and day is not None:
            try:
                datetime(year, month, day)
            except ValueError:
                return False

        return True

    def _looks_like_datetime(self, value: str) -> bool:
        """
        Determine whether a string resembles a combined date-time value.
        """
        if not self._looks_like_date(value):
            return False

        if ":" in value:
            v = value.strip().upper()
            v = v.replace(" AM", "AM")
            v = v.replace(" PM", "PM")
            tm = v.rsplit(None, 1)[-1]
            if not self._looks_like_time(tm):
                return False

        return True

    # Datetime parsing
    def parse_datetime(self, value: Any) -> Optional[datetime]:
        """
        Parse a value into a datetime.

        Uses normalisation, cache lookup, pandas fast path, dateparser fallback,
        and manual date/time parsing. Returns None when all strategies fail.
        """
        if isinstance(value, datetime):
            return self._normalise_timezone(value)
        if isinstance(value, date):
            return datetime(value.year, value.month, value.day)
        if not isinstance(value, str):
            return None

        normalised = self._normalise(value)
        raw = normalised

        if not self._looks_like_datetime(
            normalised
        ):  # at the moment just checking for empty strings and no numbers
            self._trace(f"Not a datetime '{raw}'")
            self.cache[raw] = None
            return None

        # Cache
        if raw in self.cache:
            self._trace(f"Cache hit for '{raw}' -> {self.cache[raw]}")
            return self.cache[raw]
        self._trace(f"Cache miss for '{raw}'")

        # 1. Pandas fast path
        with self._timed(f"pandas datetime '{raw}'"):
            try:
                dt = pd.to_datetime(normalised, errors="raise", dayfirst=self.day_first)
                result = self._normalise_timezone(dt.to_pydatetime())
                self.cache[raw] = result
                self._trace(f"Pandas succeeded for '{raw}' -> {result}")
                return result
            except Exception:
                self._trace(f"Pandas failed for '{raw}'")

        # 2. dateparser fallback
        with self._timed(f"dateparser datetime '{raw}'"):
            dt = dateparser.parse(
                normalised,
                languages=[loc.split("_")[0] for loc in self.fallback_locales],
                settings={"RETURN_AS_TIMEZONE_AWARE": True},
            )
        if dt:
            result = self._normalise_timezone(dt)
            self.cache[raw] = result
            self._trace(f"dateparser succeeded for '{raw}' -> {result}")
            return result
        self._trace(f"dateparser failed for '{raw}'")

        # 3. Manual fallback
        self._trace(f"Trying manual fallback for '{raw}'")
        d = self.parse_date(normalised)
        t = self.parse_time(normalised)
        if d and t:
            result = datetime(d.year, d.month, d.day, t.hour, t.minute, t.second)
            self.cache[raw] = result
            self._trace(f"Manual fallback succeeded for '{raw}' -> {result}")
            return result

        # 4. Failure
        self._trace(f"All strategies failed for '{raw}'")
        self.cache[raw] = None
        return None

    # Date-only parsing
    def parse_date(self, value: Any) -> Optional[datetime]:
        """
        Parse a value into a date.

        Uses normalisation, cache lookup, pandas, dateparser, and manual formats.
        Returns None when all strategies fail.
        """
        if isinstance(value, datetime) or isinstance(value, date):
            return datetime(value.year, value.month, value.day)
        if not isinstance(value, str):
            return None

        normalised = self._normalise(value)
        raw = normalised

        if not self._looks_like_date(normalised):
            self._trace(f"Not a date '{raw}'")
            self.cache[raw] = None
            return None

        if raw in self.cache:
            self._trace(f"Cache hit (date) for '{raw}'")
            return self.cache[raw]
        self._trace(f"Cache miss (date) for '{raw}'")

        # 1. Pandas
        with self._timed(f"pandas date '{raw}'"):
            try:
                dt = pd.to_datetime(normalised, errors="raise", dayfirst=self.day_first)
                result = datetime(dt.year, dt.month, dt.day)
                self.cache[raw] = result
                self._trace(f"Pandas date succeeded for '{raw}' -> {result}")
                return result
            except Exception:
                self._trace(f"Pandas date failed for '{raw}'")

        # 2. dateparser
        with self._timed(f"dateparser date '{raw}'"):
            dt = dateparser.parse(
                normalised,
                languages=[loc.split("_")[0] for loc in self.fallback_locales],
                settings={"RETURN_AS_TIMEZONE_AWARE": True},
            )
        if dt:
            result = datetime(dt.year, dt.month, dt.day)
            self.cache[raw] = result
            self._trace(f"dateparser date succeeded for '{raw}' -> {result}")
            return result
        self._trace(f"dateparser date failed for '{raw}'")

        # 3. Manual formats
        self._trace(f"Trying manual date formats for '{raw}'")
        for fmt in self.date_formats:
            try:
                dt = datetime.strptime(normalised, fmt)
                result = datetime(dt.year, dt.month, dt.day)
                self.cache[raw] = result
                self._trace(f"Manual date succeeded for '{raw}' -> {result}")
                return result
            except Exception:
                continue

        self._trace(f"All date strategies failed for '{raw}'")
        self.cache[raw] = None
        return None

    # Time-only parsing
    def parse_time(self, value: Any) -> Optional[datetime]:
        """
        Parse a value into a time.

        Uses normalisation, cache lookup, pandas, dateparser, and manual formats.
        Returns None when all strategies fail.
        """
        if isinstance(value, datetime):
            return datetime(
                self.min_date.year,
                self.min_date.month,
                self.min_date.day,
                value.hour,
                value.minute,
                value.second,
            )
        if isinstance(value, date):
            return None
        if not isinstance(value, str):
            return None

        normalised = self._normalise(value)
        raw = normalised

        if not self._looks_like_time(normalised):
            self._trace(f"Not a time '{raw}'")
            self.cache[raw] = None
            return None

        if raw in self.cache:
            self._trace(f"Cache hit (time) for '{raw}'")
            return self.cache[raw]
        self._trace(f"Cache miss (time) for '{raw}'")

        # 1. Pandas
        with self._timed(f"pandas time '{raw}'"):
            try:
                dt = pd.to_datetime(normalised, errors="raise")
                result = datetime(
                    self.min_date.year,
                    self.min_date.month,
                    self.min_date.day,
                    dt.hour,
                    dt.minute,
                    dt.second,
                )
                self.cache[raw] = result
                self._trace(f"Pandas time succeeded for '{raw}' -> {result}")
                return result
            except Exception:
                self._trace(f"Pandas time failed for '{raw}'")

        # 2. dateparser
        with self._timed(f"dateparser time '{raw}'"):
            dt = dateparser.parse(
                normalised,
                languages=[loc.split("_")[0] for loc in self.fallback_locales],
            )
        if dt:
            result = datetime(
                self.min_date.year,
                self.min_date.month,
                self.min_date.day,
                dt.hour,
                dt.minute,
                dt.second,
            )
            self.cache[raw] = result
            self._trace(f"dateparser time succeeded for '{raw}' -> {result}")
            return result
        self._trace(f"dateparser time failed for '{raw}'")

        # 3. Manual formats
        self._trace(f"Trying manual time formats for '{raw}'")
        for fmt in self.time_formats:
            try:
                dt = datetime.strptime(normalised, fmt)
                result = datetime(
                    self.min_date.year,
                    self.min_date.month,
                    self.min_date.day,
                    dt.hour,
                    dt.minute,
                    dt.second,
                )
                self.cache[raw] = result
                self._trace(f"Manual time succeeded for '{raw}' -> {result}")
                return result
            except Exception:
                continue

        self._trace(f"All time strategies failed for '{raw}'")
        self.cache[raw] = None
        return None
