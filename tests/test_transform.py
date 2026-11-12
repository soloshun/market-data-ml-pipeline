from datetime import date, timedelta
from statistics import stdev

import polars as pl
import pytest

from market_data.transform import build_features


def example_prices() -> pl.DataFrame:
    dates = [date(2025, 1, 1) + timedelta(days=i) for i in range(25)]
    return pl.DataFrame(
        {
            "symbol": ["AAA"] * 25 + ["BBB"] * 25,
            "trade_date": dates * 2,
            "close": list(range(1, 26)) + [100] * 25,
        }
    )


def test_rolling_windows_and_warm_up_reset_per_symbol():
    features = build_features(example_prices().reverse())
    a = features.filter(pl.col("symbol") == "AAA")
    b = features.filter(pl.col("symbol") == "BBB")
    assert a["trade_date"].is_sorted()
    assert a["daily_return"][0] is None and b["daily_return"][0] is None
    assert a["daily_return"][1] == pytest.approx(100.0)
    assert b["daily_return"][1] == 0
    assert a["sma_5"][3] is None and a["sma_5"][4] == pytest.approx(3)
    assert a["sma_20"][18] is None and a["sma_20"][19] == pytest.approx(10.5)
    assert b["sma_20"][18] is None and b["sma_20"][19] == pytest.approx(100)
    assert a["volatility_5"][4] is None
    assert a["volatility_5"][5] == pytest.approx(stdev([100 / i for i in range(1, 6)]))


def test_future_observations_do_not_change_prior_features():
    prices = example_prices()
    cutoff = date(2025, 1, 23)
    earlier = pl.col("trade_date") <= cutoff
    assert build_features(prices).filter(earlier).equals(build_features(prices.filter(earlier)))
