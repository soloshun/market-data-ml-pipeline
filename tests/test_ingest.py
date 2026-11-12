from datetime import date

import pandas as pd
import polars as pl
import pytest
from yfinance.exceptions import YFRateLimitError

from market_data.config import FIXTURE_END, FIXTURE_START, FIXTURE_SYMBOL
from market_data.ingest import (
    TransientFetchError,
    fetch_yahoo,
    normalize_yahoo,
    read_fixture,
    save_snapshot,
)
from market_data.validate import validate_prices


def yahoo_frame():
    return pd.DataFrame(
        {"Open": [10], "High": [12], "Low": [9], "Close": [11], "Adj Close": [10], "Volume": [0]},
        index=pd.DatetimeIndex(["2025-01-06"], tz="Asia/Tokyo", name="session"),
    )


@pytest.mark.parametrize("layout", ["flat", "price-first", "ticker-first"])
def test_yahoo_normalization_preserves_fields_and_local_session_date(layout):
    raw = yahoo_frame()
    if layout != "flat":
        raw.columns = pd.MultiIndex.from_product([raw.columns, ["AAPL"]])
        if layout == "ticker-first":
            raw = raw.swaplevel(axis=1)
    bronze = normalize_yahoo(raw, "AAPL")
    assert bronze.columns == ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
    prices = validate_prices(bronze, "AAPL", FIXTURE_START, FIXTURE_END)
    assert prices["trade_date"].item() == date(2025, 1, 6)
    assert prices["volume"].item() == 0


def test_download_options(monkeypatch):
    def download(symbol, **options):
        assert symbol == "AAPL"
        assert options["start"] == "2025-01-06"
        assert options["end"] == "2025-02-15"
        assert options["interval"] == "1d"
        assert options["auto_adjust"] is False
        assert options["multi_level_index"] is False
        assert options["keepna"] is True
        return yahoo_frame()

    monkeypatch.setattr("market_data.ingest.yf.download", download)
    assert fetch_yahoo("AAPL", FIXTURE_START, FIXTURE_END).height == 1


def test_provider_rate_limit_is_retryable(monkeypatch):
    def download(*args, **kwargs):
        raise YFRateLimitError()

    monkeypatch.setattr("market_data.ingest.yf.download", download)
    with pytest.raises(TransientFetchError, match="temporary Yahoo"):
        fetch_yahoo("AAPL", FIXTURE_START, FIXTURE_END)


def test_empty_download_has_actionable_error(monkeypatch):
    monkeypatch.setattr("market_data.ingest.yf.download", lambda *a, **kw: pd.DataFrame())
    with pytest.raises(TransientFetchError, match=r"AAPL: Yahoo returned zero rows"):
        fetch_yahoo("AAPL", FIXTURE_START, FIXTURE_END)


def test_malformed_index_fails_clearly():
    with pytest.raises(ValueError, match="row index"):
        normalize_yahoo(yahoo_frame().reset_index(drop=True), "AAPL")


def test_fixture_range_and_bronze(tmp_path):
    assert read_fixture(FIXTURE_START, FIXTURE_END).height == 30
    raw = read_fixture(FIXTURE_START, date(2025, 1, 8))
    assert raw["Date"].to_list() == ["2025-01-06", "2025-01-07"]
    first = save_snapshot(raw, FIXTURE_SYMBOL, "fixture", tmp_path)
    second = save_snapshot(raw, FIXTURE_SYMBOL, "fixture", tmp_path)
    assert first.parent.name == "symbol=XAUUSD-DEMO"
    assert first.parent.parent.name.startswith("ingest_date=")
    assert first != second
    snapshot = pl.read_parquet(first, hive_partitioning=False)
    assert snapshot.select(raw.columns).equals(raw)
    assert snapshot["_source"].to_list() == ["fixture", "fixture"]
    assert snapshot["_ingested_at"].null_count() == 0
