"""Defaults and input validation shared by the CLI and the flow."""

import os
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv

Source = Literal["fixture", "yfinance"]
FailureDemo = Literal["transient", "invalid"]
FIXTURE_SYMBOL = "XAUUSD-DEMO"
FIXTURE_START = date(2025, 1, 6)
FIXTURE_END = date(2025, 2, 15)
LIVE_SYMBOL = "GC=F"
INITIAL_DAYS = 60
OVERLAP_DAYS = 7


def data_source() -> Source:
    """Read the configured source from the environment or the local .env file.

    Returns:
        The configured source, or "fixture" when DATA_SOURCE is unset.

    Raises:
        ValueError: DATA_SOURCE is neither "fixture" nor "yfinance".
    """
    load_dotenv(".env")
    source = os.getenv("DATA_SOURCE", "fixture")
    if source in ("fixture", "yfinance"):
        return source
    raise ValueError("DATA_SOURCE must be fixture or yfinance.")


def database_url() -> str | None:
    """Read DATABASE_URL without exposing the credential.

    Returns:
        The configured PostgreSQL connection string, or None when unset or empty.
    """
    load_dotenv(".env")
    return os.getenv("DATABASE_URL") or None


def bronze_directory() -> Path:
    """Resolve the directory used for local Bronze snapshots.

    Returns:
        The configured path with a leading tilde expanded, or data/bronze by default.
    """
    load_dotenv(".env")
    return Path(os.getenv("BRONZE_DIR") or "data/bronze").expanduser()


def resolve_symbols(source: Source, symbols: list[str] | None) -> list[str]:
    """Normalize, validate, and sort the requested symbols.

    Args:
        source: Data source, used to choose the default and enforce the fixture symbol.
        symbols: Tickers to normalize, or None to use the source default.

    Returns:
        Sorted, uppercase tickers with duplicates removed.

    Raises:
        ValueError: A ticker has invalid characters or is unsupported for the source.
    """
    default = FIXTURE_SYMBOL if source == "fixture" else LIVE_SYMBOL
    cleaned = sorted({symbol.strip().upper() for symbol in (symbols or [default])})
    if any(not re.fullmatch(r"[A-Z0-9^][A-Z0-9.^=\-]{0,31}", symbol) for symbol in cleaned):
        raise ValueError("Use valid ticker symbols, for example AAPL, GC=F, or XAUUSD=X.")
    if source == "fixture" and cleaned != [FIXTURE_SYMBOL]:
        raise ValueError(f"The synthetic fixture contains only {FIXTURE_SYMBOL}.")
    if source == "yfinance" and FIXTURE_SYMBOL in cleaned:
        raise ValueError(f"{FIXTURE_SYMBOL} is reserved for the synthetic fixture.")
    return cleaned


def resolve_window(source: Source, start: date | None, end: date | None) -> tuple[date, date]:
    """Fill in missing dates and check the requested historical range.

    Args:
        source: Data source, used to choose the date defaults.
        start: Inclusive start date, or None to use the source default.
        end: Exclusive end date, or None to use the source default.

    Returns:
        The resolved pair of dates in start, end order.

    Raises:
        ValueError: The window is empty or reversed, or a live end date is after today UTC.
    """
    default_end = FIXTURE_END if source == "fixture" else datetime.now(UTC).date()
    end = end or default_end
    start = start or (FIXTURE_START if source == "fixture" else end - timedelta(days=INITIAL_DAYS))
    if start >= end:
        raise ValueError("Start date must come before the exclusive end date.")
    if source == "yfinance" and end > default_end:
        raise ValueError("Use today (UTC) or an earlier end date for completed historical bars.")
    return start, end
