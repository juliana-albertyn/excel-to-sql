"""
Shared runtime context for the Fynbyte Excel-to-SQL pipeline.

Holds paths, environment settings, locale rules, date/time parsing
preferences, logging configuration, and run metadata. Passed through all
pipeline stages to ensure consistent behaviour across cleaning,
validation, and loading.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from logging import Logger, INFO

import src.parsers as parsers


@dataclass
class ETLContext:
    """
    Container for all ETL runtime settings.

    Stores paths, locale and parsing rules, logging configuration, run
    metadata, and strict/permissive behaviour flags. Provides lazy
    initialisation of the DateTimeParser so all components use the same
    parsing logic.
    """    
    # paths
    log_dir: Path
    data_dir: Path
    output_dir: Path
    config_dir: Path
    # development / production
    environment: str = "development"
    # new column names created during cleaning and transformation
    cleaned_suffix: str = "_cleaned"
    # for date and time parser
    min_date: datetime = datetime(1900, 1, 1, 0, 0, 0)
    locale = "en_ZA"
    fallback_locales: list["str"] = field(default_factory=list)
    day_first_format: bool = True
    datetime_parser: parsers.DateTimeParser | None = None
    # money
    currency_symbol: str = ""
    # the same run date is used for all log files
    run_date: date | None = None  # datetime.date object
    run_timestamp: str = ""
    log_level: int = INFO
    max_logs: int = 5  # keep last 5 log files per module
    

    def get_datetime_parser(self, logger: Logger | None) -> parsers.DateTimeParser:
        """
        Return the shared DateTimeParser instance.

        Creates the parser on first use using locale, fallback locales, minimum
        date, and day-first settings. Ensures consistent parsing across all
        cleaning and validation steps.
        """        
        if self.datetime_parser is None:
            self.datetime_parser = parsers.DateTimeParser(
                day_first=self.day_first_format,
                min_date=self.min_date,
                locale=self.locale,
                fallback_locales=self.fallback_locales,
                logger=logger,
            )
        return self.datetime_parser
