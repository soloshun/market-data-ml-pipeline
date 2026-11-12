"""Calculate trailing, per-symbol features using Polars expressions."""

import polars as pl

from market_data.validate import KEY

FEATURE_COLUMNS = ["daily_return", "sma_5", "sma_20", "volatility_5"]


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
    close = pl.col("close")
    return (
        prices.sort(KEY)
        .with_columns(
            ((close / close.shift(1) - 1) * 100).over("symbol").alias("daily_return"),
            close.rolling_mean(window_size=5).over("symbol").alias("sma_5"),
            close.rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        )
        .with_columns(
            pl.col("daily_return")
            .rolling_std(window_size=5, ddof=1)
            .over("symbol")
            .alias("volatility_5")
        )
        .select(*KEY, *FEATURE_COLUMNS)
    )
