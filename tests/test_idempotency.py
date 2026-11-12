import os
import sqlite3
from contextlib import closing
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from market_data.config import FIXTURE_END, FIXTURE_START, FIXTURE_SYMBOL
from market_data.ingestion import hourly, storage
from market_data.ingestion.sources import read_fixture
from market_data.ingestion.validation import validate_hourly_prices, validate_prices


@pytest.fixture(params=["sqlite", pytest.param("postgres", marks=pytest.mark.postgres)])
def database(request, tmp_path):
    if request.param == "sqlite":
        yield tmp_path / "workshop.sqlite3"
        return

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
    second_date = prices["trade_date"][1]
    if isinstance(database, Path):
        with closing(sqlite3.connect(database)) as connection:
            saved_return = connection.execute(
                "SELECT daily_return FROM market_features WHERE symbol = ? AND trade_date = ?",
                (FIXTURE_SYMBOL, second_date.isoformat()),
            ).fetchone()[0]
    else:
        with storage.connect(database) as connection:
            saved_return = connection.execute(
                """
                SELECT daily_return FROM market_data.market_features
                WHERE symbol = %s AND trade_date = %s
                """,
                (FIXTURE_SYMBOL, second_date),
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


def test_hourly_backfill_revises_later_gold_features(database, monkeypatch):
    start, end = hourly.FIXTURE_START, hourly.FIXTURE_END
    bars = validate_hourly_prices(
        hourly.read_hourly_fixture(start, end), FIXTURE_SYMBOL, start, end
    )
    assert storage.latest_hourly_bar(database, FIXTURE_SYMBOL) is None
    assert storage.load_hourly_prices(database, bars, "fixture") == (12, 12)
    revised = bars.head(1).with_columns(pl.col("close") + 1)
    assert storage.load_hourly_prices(database, revised, "fixture") == (1, 12)
    assert storage.latest_hourly_bar(database, FIXTURE_SYMBOL) == bars["bar_start_utc"][-1]

    def fail(_prices):
        raise ValueError("simulated hourly feature error")

    monkeypatch.setattr(storage, "build_hourly_features", fail)
    with pytest.raises(ValueError, match="simulated hourly feature error"):
        storage.load_hourly_prices(database, bars.head(1), "fixture")

    sqlite = isinstance(database, Path)
    prefix = "" if sqlite else "market_data."
    connection = closing(sqlite3.connect(database)) if sqlite else storage.connect(database)
    with connection as conn:
        silver = conn.execute(
            f"SELECT close FROM {prefix}hourly_prices ORDER BY bar_start_utc"
        ).fetchall()
        gold = conn.execute(
            f"SELECT hourly_return, sma_5 FROM {prefix}hourly_features ORDER BY bar_start_utc"
        ).fetchall()
    assert len(silver) == len(gold) == 12
    assert silver[0][0] == revised["close"][0]
    assert gold[0] == (None, None)
    assert gold[1][0] == pytest.approx((bars["close"][1] / revised["close"][0] - 1) * 100)
    assert gold[4][1] == pytest.approx(sum(row[0] for row in silver[:5]) / 5)
