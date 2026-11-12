"""Read a source, preserving its fields for Bronze."""

from datetime import UTC, date, datetime
from importlib.resources import files
from pathlib import Path
from uuid import uuid4

import pandas as pd
import polars as pl
import yfinance as yf
from curl_cffi.requests import exceptions as curl_errors
from yfinance.exceptions import YFRateLimitError

from market_data.config import Source


class TransientFetchError(RuntimeError):
    """The provider or network may recover on retry."""


def normalize_yahoo(raw: pd.DataFrame, symbol: str) -> pl.DataFrame:
    """Convert a Yahoo frame to Polars while keeping source column names.

    Args:
        raw: Yahoo's pandas frame with a date index and flat or two-level columns.
        symbol: Ticker to select if the columns have a ticker level.

    Returns:
        A Polars frame with the index exposed as a Date column.

    Raises:
        ValueError: The date index or the column layout is unsupported.
    """
    if isinstance(raw.columns, pd.MultiIndex):
        if raw.columns.nlevels != 2:
            raise ValueError(f"{symbol}: expected two Yahoo column levels.")
        ticker_levels = [
            level for level in range(2) if symbol in raw.columns.get_level_values(level)
        ]
        if len(ticker_levels) != 1:
            raise ValueError(f"{symbol}: could not locate the ticker in Yahoo's columns.")
        raw = raw.xs(symbol, axis=1, level=ticker_levels[0])
    if not isinstance(raw.index, pd.DatetimeIndex):
        raise ValueError(f"{symbol}: Yahoo's row index must contain daily timestamps.")
    if not raw.columns.is_unique or "Date" in raw.columns:
        raise ValueError(f"{symbol}: Yahoo returned duplicate or conflicting columns.")
    # Preserve the session's calendar date even if the index is timezone-aware.
    return pl.from_pandas(raw.rename_axis("Date").reset_index())


def read_fixture(start: date, end: date) -> pl.DataFrame:
    """Read the packaged synthetic price bars for a date window.

    Args:
        start: First included trade date.
        end: First excluded trade date.

    Returns:
        Raw Polars rows with the same column names as the fixture CSV.
    """
    fixture = files("market_data").joinpath("data/market_sample.csv")
    with fixture.open("rb") as handle:
        raw = pl.read_csv(handle)
    return raw.filter(pl.col("Date").str.to_date().is_between(start, end, closed="left"))


def fetch_yahoo(symbol: str, start: date, end: date) -> pl.DataFrame:
    """Download daily Yahoo Finance bars for one ticker.

    Args:
        symbol: Yahoo Finance ticker.
        start: First included trade date.
        end: First excluded trade date.

    Returns:
        Raw Polars rows with unadjusted OHLC fields and source column names.

    Raises:
        TransientFetchError: The request times out, is throttled, or returns no rows.
        ValueError: Yahoo returns an unsupported index or column layout.
    """
    try:
        raw = yf.download(
            symbol,
            start=start.isoformat(),
            end=end.isoformat(),
            interval="1d",
            auto_adjust=False,
            multi_level_index=False,
            ignore_tz=True,
            keepna=True,
            threads=False,
            progress=False,
            timeout=15,
        )
    except (
        YFRateLimitError,
        TimeoutError,
        ConnectionError,
        curl_errors.Timeout,
        curl_errors.ConnectionError,
    ) as exc:
        raise TransientFetchError(f"{symbol}: temporary Yahoo or network failure.") from exc
    if raw is None or len(raw.index) == 0:
        raise TransientFetchError(
            f"{symbol}: Yahoo returned zero rows for [{start}, {end}). "
            "Check the ticker, dates and provider log. Try GC=F for gold futures, or the fixture."
        )
    return normalize_yahoo(raw, symbol)


def save_snapshot(raw: pl.DataFrame, symbol: str, source: Source, directory: Path) -> Path:
    """Write a local Parquet snapshot with ingestion metadata.

    Args:
        raw: Source rows, including the Date column and original price fields.
        symbol: Ticker used in the snapshot metadata and directory name.
        source: Source label saved alongside the raw rows.
        directory: Parent directory for the ingestion date and ticker folders.

    Returns:
        Path to the new Parquet file.
    """
    now = datetime.now(UTC)
    partition = directory / f"ingest_date={now.date()}" / f"symbol={symbol}"
    partition.mkdir(parents=True, exist_ok=True)
    path = partition / f"{source}-{uuid4().hex}.parquet"
    snapshot = raw.with_columns(
        pl.lit(symbol).alias("_symbol"),
        pl.lit(source).alias("_source"),
        pl.lit(now).alias("_ingested_at"),
    )
    temporary = path.with_suffix(".tmp")
    try:
        snapshot.write_parquet(temporary)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path
