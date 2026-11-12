"""Time windows and fixture data for the hourly ingestion lesson."""

from datetime import UTC, datetime, timedelta
from importlib.resources import files

import polars as pl

from market_data.config import Source

FIXTURE_START = datetime(2025, 1, 6, 6, tzinfo=UTC)
FIXTURE_END = datetime(2025, 1, 6, 18, tzinfo=UTC)
INITIAL_HISTORY = timedelta(days=365)
OVERLAP = timedelta(hours=24)


def utc_hour(value: datetime) -> datetime:
    """Normalize a requested hour to UTC, rejecting ambiguous or partial hours.

    Args:
        value: Requested time with an explicit offset.

    Returns:
        The same hour in UTC.

    Raises:
        ValueError: The time lacks an offset or is not on an hour boundary.
    """
    if value.utcoffset() is None:
        raise ValueError(
            "Include a timezone offset in hourly times, for example 2025-01-06T06:00Z."
        )
    value = value.astimezone(UTC)
    if value.minute or value.second or value.microsecond:
        raise ValueError("Hourly windows must start and end on whole UTC hours.")
    return value


def hourly_window(
    source: Source,
    start_at: datetime | None,
    end_at: datetime | None,
    latest: datetime | None,
    now: datetime,
) -> tuple[datetime, datetime]:
    """Choose a backfill or catch-up window containing completed bars only.

    Args:
        source: Fixture or Yahoo Finance.
        start_at: Optional first included hour for an explicit backfill.
        end_at: Optional first excluded hour.
        latest: Latest bar start already stored, if any.
        now: Current wall time used to exclude the still-open hour.

    Returns:
        Inclusive start and exclusive end, both in UTC.

    Raises:
        ValueError: The requested window is empty or includes an unfinished hour.
    """
    current_hour = now.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    end = (
        utc_hour(end_at)
        if end_at is not None
        else (FIXTURE_END if source == "fixture" else current_hour)
    )
    if source == "yfinance" and end > current_hour:
        raise ValueError("The hourly end cannot be later than the current UTC hour.")
    if start_at is not None:
        start = utc_hour(start_at)
    elif latest is not None:
        start = min(latest, end) - OVERLAP
        if source == "fixture":
            start = max(start, FIXTURE_START)
    else:
        start = FIXTURE_START if source == "fixture" else end - INITIAL_HISTORY
    if start >= end:
        raise ValueError("The hourly start must come before the exclusive end.")
    return start, end


def read_hourly_fixture(start: datetime, end: datetime) -> pl.DataFrame:
    """Read synthetic hourly bars with UTC timestamps.

    Args:
        start: First included hour.
        end: First excluded hour.

    Returns:
        Raw fixture rows in the requested window.
    """
    fixture = files("market_data").joinpath("data/hourly_sample.csv")
    with fixture.open("rb") as handle:
        return pl.read_csv(handle, try_parse_dates=True).filter(
            pl.col("Date").is_between(start, end, closed="left")
        )
