"""Convert raw fields to the Silver schema and reject invalid batches."""

from datetime import date, datetime, timedelta

import polars as pl

KEY = ["symbol", "trade_date"]
PRICE_COLUMNS = ["open", "high", "low", "close"]
HOURLY_KEY = ["symbol", "bar_start_utc"]
SOURCE_COLUMNS = {
    "Date": "trade_date",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
}


def rescue_extra_columns(raw: pl.DataFrame) -> pl.DataFrame:
    """Collect unfamiliar source fields in one JSON string per row.

    Pass provider rows before attaching Bronze metadata. The six required
    fields and the optional Adj Close field stay as ordinary columns.
    """
    recognized = SOURCE_COLUMNS.keys() | {"Adj Close"}
    known = [name for name in raw.columns if name in recognized]
    extra = [name for name in raw.columns if name not in recognized]
    if extra:
        rescued = pl.struct(extra).struct.json_encode()
    else:
        rescued = pl.repeat(None, pl.len(), dtype=pl.String)
    return raw.select(*known, rescued.alias("_rescued_data"))


def validate_prices(raw: pl.DataFrame, symbol: str, start: date, end: date) -> pl.DataFrame:
    """Validate one ticker's source rows and convert them to Silver prices.

    Args:
        raw: Source frame containing Date, Open, High, Low, Close, and Volume.
        symbol: Ticker to attach to each validated row.
        start: First permitted trade date.
        end: First excluded trade date.

    Returns:
        Typed OHLCV rows ordered by symbol and trade date.

    Raises:
        ValueError: A required field is missing, a row is invalid, or a key repeats.
            The function rejects the whole batch.
    """
    _check_source(raw, symbol, start, end)

    session_date = pl.col("trade_date")
    if raw.schema["Date"] == pl.String:
        session_date = session_date.str.to_date(format="%Y-%m-%d", strict=False)
    elif isinstance(raw.schema["Date"], pl.Datetime):
        session_date = session_date.dt.date()
    elif raw.schema["Date"] != pl.Date:
        raise ValueError(f"{symbol}: Date must contain dates, timestamps or ISO date strings.")

    # Bronze keeps the original fields. Silver chooses only the fields it promises downstream.
    prices = (
        raw.select(SOURCE_COLUMNS.keys())
        .rename(SOURCE_COLUMNS)
        .with_columns(
            pl.lit(symbol).alias("symbol"),
            session_date.cast(pl.Date, strict=False),
            pl.col(PRICE_COLUMNS).cast(pl.Float64, strict=False),
        )
    )
    checks = {
        "invalid or out-of-range date": ~pl.col("trade_date")
        .is_between(start, end, closed="left")
        .fill_null(False),
        **_ohlcv_checks(),
        "duplicate symbol and date": pl.struct(KEY).is_duplicated(),
    }
    _check_rows(prices, symbol, "trade_date", checks)
    return prices.select(*KEY, *PRICE_COLUMNS, pl.col("volume").cast(pl.Int64)).sort(KEY)


def validate_hourly_prices(
    raw: pl.DataFrame, symbol: str, start: datetime, end: datetime
) -> pl.DataFrame:
    """Convert source hours to UTC and validate the hourly bar key and OHLCV.

    Args:
        raw: Source OHLCV rows with a timezone-aware Date column.
        symbol: Ticker to attach to each bar.
        start: First permitted bar start, inclusive.
        end: First excluded bar start. The bar starting here is still open.

    Returns:
        Typed hourly bars sorted by UTC start time.

    Raises:
        ValueError: The schema, timestamp, price, volume, or key is invalid.
    """
    _check_source(raw, symbol, start, end)
    timestamp = raw.schema["Date"]
    if not isinstance(timestamp, pl.Datetime) or timestamp.time_zone is None:
        raise ValueError(f"{symbol}: hourly bars require timezone-aware timestamps.")
    prices = (
        raw.select(SOURCE_COLUMNS.keys())
        .rename(SOURCE_COLUMNS)
        .rename({"trade_date": "bar_start_utc"})
        .with_columns(
            pl.lit(symbol).alias("symbol"),
            pl.col("bar_start_utc").dt.convert_time_zone("UTC"),
            pl.col(PRICE_COLUMNS).cast(pl.Float64, strict=False),
        )
    )
    bar_start = pl.col("bar_start_utc")
    checks = {
        "invalid or out-of-range bar start": ~bar_start.is_between(
            start, end - timedelta(hours=1)
        ).fill_null(False),
        **_ohlcv_checks(),
        "duplicate symbol and hour": pl.struct(HOURLY_KEY).is_duplicated(),
    }
    _check_rows(prices, symbol, "bar_start_utc", checks)
    return prices.select(*HOURLY_KEY, *PRICE_COLUMNS, pl.col("volume").cast(pl.Int64)).sort(
        HOURLY_KEY
    )


def _check_source(
    raw: pl.DataFrame, symbol: str, start: date | datetime, end: date | datetime
) -> None:
    """Reject missing fields and empty batches before parsing the source data."""
    missing = SOURCE_COLUMNS.keys() - set(raw.columns)
    if missing:
        raise ValueError(f"{symbol}: missing required columns: {', '.join(sorted(missing))}.")
    if raw.is_empty():
        raise ValueError(f"{symbol}: zero rows for [{start}, {end}). Check the requested dates.")


def _ohlcv_checks() -> dict[str, pl.Expr]:
    """Build numeric validation rules shared by both bar sizes."""
    whole_volume = pl.col("volume").cast(pl.Int64, strict=False)
    volume = pl.col("volume").cast(pl.Float64, strict=False)
    return {
        "price must be finite and positive": ~pl.all_horizontal(
            (pl.col(PRICE_COLUMNS).is_finite() & (pl.col(PRICE_COLUMNS) > 0)).fill_null(False)
        ),
        "inconsistent OHLC bounds": (
            (pl.col("low") > pl.min_horizontal("open", "close"))
            | (pl.col("high") < pl.max_horizontal("open", "close"))
        ),
        "volume must be a nonnegative whole number in bigint range": (
            whole_volume.is_null()
            | (~volume.is_finite() | (volume < 0) | (volume != volume.floor())).fill_null(True)
        ),
    }


def _check_rows(
    prices: pl.DataFrame, symbol: str, time_column: str, checks: dict[str, pl.Expr]
) -> None:
    """Reject a batch with a short sample of the offending bar dates or times."""
    for reason, condition in checks.items():
        invalid = prices.filter(condition)
        if invalid.height:
            dates = invalid.get_column(time_column).head(3).to_list()
            raise ValueError(
                f"{symbol}: {reason} in {invalid.height} row(s). Example dates: {dates}."
            )
