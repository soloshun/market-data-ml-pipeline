import os
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from market_data import storage
from market_data.config import FIXTURE_END, FIXTURE_START, FIXTURE_SYMBOL
from market_data.ingest import read_fixture
from market_data.validate import validate_prices

pytestmark = pytest.mark.postgres


@pytest.fixture
def database():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set TEST_DATABASE_URL to a disposable PostgreSQL database.")
    with storage.connect(url) as connection:
        connection.autocommit = True
        exists = connection.execute("SELECT to_regnamespace('market_data') IS NOT NULL").fetchone()
        if exists[0]:
            pytest.fail("Integration tests require a database without the market_data schema.")
        schema = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"
        connection.execute(schema.read_text(), prepare=False)
        connection.execute(schema.read_text(), prepare=False)
    try:
        yield url
    finally:
        with storage.connect(url) as connection:
            connection.execute("DROP SCHEMA market_data CASCADE")


@pytest.fixture
def prices():
    raw = read_fixture(FIXTURE_START, FIXTURE_END)
    return validate_prices(raw, FIXTURE_SYMBOL, FIXTURE_START, FIXTURE_END)


def test_repeat_upserts_preserve_counts_and_backfill_updates_later_features(database, prices):
    assert storage.load_market_data(database, prices, "fixture") == 30
    assert storage.load_market_data(database, prices, "fixture") == 30
    assert storage.row_counts(database, FIXTURE_SYMBOL) == [
        ("market_prices", 30, 30),
        ("market_features", 30, 30),
    ]
    assert storage.latest_dates(database, [FIXTURE_SYMBOL]) == {FIXTURE_SYMBOL: date(2025, 2, 14)}

    revised = prices.head(1).with_columns((pl.col("close") + 1).alias("close"))
    assert storage.load_market_data(database, revised, "fixture") == 30
    with storage.connect(database) as connection:
        saved_return = connection.execute(
            """
            SELECT daily_return FROM market_data.market_features
            WHERE symbol = %s AND trade_date = %s
            """,
            (FIXTURE_SYMBOL, prices["trade_date"][1]),
        ).fetchone()[0]
    assert saved_return == pytest.approx((prices["close"][1] / revised["close"][0] - 1) * 100)


def test_gold_error_rolls_back_silver(database, prices, monkeypatch):
    def fail(_prices):
        raise ValueError("simulated feature error")

    monkeypatch.setattr(storage, "build_features", fail)
    with pytest.raises(ValueError, match="simulated feature error"):
        storage.load_market_data(database, prices, "fixture")
    assert storage.row_counts(database, FIXTURE_SYMBOL) == [
        ("market_prices", 0, 0),
        ("market_features", 0, 0),
    ]
