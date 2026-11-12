from datetime import date

import polars as pl
import pytest

from market_data.config import FIXTURE_END, FIXTURE_START, FIXTURE_SYMBOL
from market_data.ingest import read_fixture
from market_data.validate import validate_prices


@pytest.fixture
def raw():
    return read_fixture(FIXTURE_START, date(2025, 1, 9))


def test_sort_and_select_columns(raw):
    prices = validate_prices(raw.reverse(), FIXTURE_SYMBOL, FIXTURE_START, FIXTURE_END)
    assert prices.columns == ["symbol", "trade_date", "open", "high", "low", "close", "volume"]
    assert prices["trade_date"].is_sorted()
    assert prices.schema["volume"] == pl.Int64
    assert prices.schema["trade_date"] == pl.Date


@pytest.mark.parametrize(
    ("column", "value", "message"),
    [
        ("Date", "nonsense", "invalid or out-of-range date"),
        ("Date", "2025-02-15", "invalid or out-of-range date"),
        ("Volume", -1, "volume must be"),
        ("Volume", 1.25, "volume must be"),
        ("Volume", 2**63, "volume must be"),
        ("Open", None, "price must be finite"),
        ("Open", float("inf"), "price must be finite"),
        ("Close", 0, "price must be finite"),
        ("High", 1, "inconsistent OHLC"),
    ],
)
def test_bad_rows_fail_the_batch(raw, column, value, message):
    with pytest.raises(ValueError, match=message):
        validate_prices(
            raw.with_columns(pl.lit(value).alias(column)),
            FIXTURE_SYMBOL,
            FIXTURE_START,
            FIXTURE_END,
        )


def test_missing_required_field(raw):
    with pytest.raises(ValueError, match="missing required columns: High"):
        validate_prices(raw.drop("High"), FIXTURE_SYMBOL, FIXTURE_START, FIXTURE_END)


def test_duplicate_keys_are_rejected(raw):
    with pytest.raises(ValueError, match="duplicate symbol and date"):
        validate_prices(pl.concat([raw, raw.head(1)]), FIXTURE_SYMBOL, FIXTURE_START, FIXTURE_END)
