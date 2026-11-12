import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta

import pandas as pd
import polars as pl
import pytest
from prefect.testing.utilities import prefect_test_harness

from market_data import hourly, ingest, storage
from market_data.config import FIXTURE_SYMBOL
from market_data.flows.hourly_ingestion import ingest_hourly_gold
from market_data.validate import validate_hourly_prices


def test_catch_up_uses_latest_hour_and_excludes_open_hour():
    now = datetime(2026, 11, 17, 12, 38, tzinfo=UTC)
    latest = datetime(2026, 11, 16, 5, tzinfo=UTC)
    start, end = hourly.hourly_window("yfinance", None, None, latest, now)
    assert start == datetime(2026, 11, 15, 5, tzinfo=UTC)
    assert end == datetime(2026, 11, 17, 12, tzinfo=UTC)

    with pytest.raises(ValueError, match="timezone offset"):
        hourly.hourly_window("yfinance", now.replace(tzinfo=None), None, latest, now)
    with pytest.raises(ValueError, match="current UTC hour"):
        hourly.hourly_window("yfinance", None, end + timedelta(hours=1), latest, now)


def test_yahoo_hourly_uses_real_timestamps_and_filters_provider_cutoff(monkeypatch):
    start = datetime(2026, 11, 17, 10, tzinfo=UTC)
    end = start + timedelta(hours=2)
    # A provider response can include the open bar at exactly end.
    index = pd.date_range(start, end, freq="30min").tz_convert("America/New_York")
    values = {"Open": 100, "High": 105, "Low": 98, "Close": 101, "Volume": 10}

    def download(symbol, **kwargs):
        assert symbol == "GC=F"
        assert kwargs["interval"] == "1h" and kwargs["ignore_tz"] is False
        assert (kwargs["start"], kwargs["end"]) == (start, end)
        return pd.DataFrame(values, index=index)

    monkeypatch.setattr(ingest.yf, "download", download)
    raw = ingest.fetch_yahoo_hourly("GC=F", start, end)
    bars = validate_hourly_prices(raw, "GC=F", start, end)
    assert bars["bar_start_utc"].to_list() == [
        start,
        start + timedelta(minutes=30),
        start + timedelta(hours=1),
    ]
    assert bars.schema["bar_start_utc"].time_zone == "UTC"
    with pytest.raises(ValueError, match="duplicate symbol and hour"):
        validate_hourly_prices(pl.concat([raw, raw.head(1)]), "GC=F", start, end)


def test_fixture_catches_up_after_a_gap_without_duplicate_hours(tmp_path, monkeypatch):
    database = tmp_path / "hourly.sqlite3"
    monkeypatch.setenv("SQLITE_DB_PATH", str(database))
    monkeypatch.setenv("PREFECT_SERVER_ANALYTICS_ENABLED", "false")
    initial_end = hourly.FIXTURE_START + timedelta(hours=3)
    with prefect_test_harness():
        first = ingest_hourly_gold(end_at=initial_end)
        assert first.height == 3
        assert storage.latest_hourly_bar(database, FIXTURE_SYMBOL) == initial_end - timedelta(
            hours=1
        )
        recovered = ingest_hourly_gold()
        assert recovered.height == 12
        assert ingest_hourly_gold().equals(recovered)

    with closing(sqlite3.connect(database)) as conn:
        assert conn.execute("SELECT count(*) FROM hourly_prices").fetchone() == (12,)
        assert conn.execute("SELECT count(*) FROM hourly_features").fetchone() == (12,)
        assert conn.execute("SELECT count(*) FROM market_prices").fetchone() == (0,)
    assert storage.latest_hourly_bar(database, FIXTURE_SYMBOL) == hourly.FIXTURE_END - timedelta(
        hours=1
    )
