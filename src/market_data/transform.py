"""Calculate trailing, per-symbol features using Polars expressions."""

import polars as pl

from market_data.validate import HOURLY_KEY, KEY


def build_features(prices: pl.DataFrame) -> pl.DataFrame:
    """Compute trailing features for each ticker in trade date order.

    Args:
        prices: Validated rows containing symbol, trade_date, and close.
            Include earlier observations when a requested window needs warm-up history.

    Returns:
        A frame keyed by symbol and trade_date with percent return, 5- and
        20-session moving averages, and 5-return sample volatility. Features
        remain null until enough observations are available.
    """
    return _build_features(prices, KEY, "daily_return")


def build_hourly_features(prices: pl.DataFrame) -> pl.DataFrame:
    """Compute trailing features for each ticker in UTC bar order.

    Args:
        prices: Validated rows containing symbol, bar_start_utc, and close.

    Returns:
        One row per bar with percent return from the previous observed bar,
        5- and 20-bar moving averages, and 5-return sample volatility.
    """
    return _build_features(prices, HOURLY_KEY, "hourly_return")


def _build_features(prices: pl.DataFrame, key: list[str], return_column: str) -> pl.DataFrame:
    """Calculate the same trailing features at either bar size."""
    close = pl.col("close")
    return (
        prices.sort(key)
        .with_columns(
            ((close / close.shift(1) - 1) * 100).over("symbol").alias(return_column),
            close.rolling_mean(window_size=5).over("symbol").alias("sma_5"),
            close.rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        )
        .with_columns(
            pl.col(return_column)
            .rolling_std(window_size=5, ddof=1)
            .over("symbol")
            .alias("volatility_5")
        )
        .select(*key, return_column, "sma_5", "sma_20", "volatility_5")
    )
