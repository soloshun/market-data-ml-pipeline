"""Convert raw fields to the Silver schema and reject invalid batches."""

from datetime import date

import polars as pl

KEY = ["symbol", "trade_date"]
PRICE_COLUMNS = ["open", "high", "low", "close"]
SOURCE_COLUMNS = {
    "Date": "trade_date",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
}


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
    missing = SOURCE_COLUMNS.keys() - set(raw.columns)
    if missing:
        raise ValueError(f"{symbol}: missing required columns: {', '.join(sorted(missing))}.")
    if raw.is_empty():
        raise ValueError(f"{symbol}: zero rows for [{start}, {end}). Check the requested dates.")

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
    whole_volume = pl.col("volume").cast(pl.Int64, strict=False)
    volume = pl.col("volume").cast(pl.Float64, strict=False)
    checks = {
        "invalid or out-of-range date": ~pl.col("trade_date")
        .is_between(start, end, closed="left")
        .fill_null(False),
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
        "duplicate symbol and date": pl.struct(KEY).is_duplicated(),
    }
    for reason, condition in checks.items():
        invalid = prices.filter(condition)
        if invalid.height:
            dates = invalid.get_column("trade_date").head(3).to_list()
            raise ValueError(
                f"{symbol}: {reason} in {invalid.height} row(s). Example dates: {dates}."
            )

    return prices.select(*KEY, *PRICE_COLUMNS, whole_volume).sort(KEY)
