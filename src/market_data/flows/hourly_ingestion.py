"""Fetch, validate, and store hourly gold bars with Prefect."""

from datetime import UTC, datetime

import polars as pl
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from market_data import config, storage
from market_data.config import Source
from market_data.flows._shared import database_target, retry_transient
from market_data.hourly import hourly_window, read_hourly_fixture
from market_data.ingest import fetch_yahoo_hourly
from market_data.validate import validate_hourly_prices


@task(
    retries=2, retry_delay_seconds=[2, 5], retry_condition_fn=retry_transient, cache_policy=NO_CACHE
)
def fetch_hourly(source: Source, symbol: str, start: datetime, end: datetime) -> pl.DataFrame:
    """Fetch one ticker's completed hourly bars.

    Args:
        source: Fixture or Yahoo Finance.
        symbol: Ticker to fetch.
        start: First included hour.
        end: First excluded hour.

    Returns:
        Raw hourly bars with source columns.
    """
    if source == "fixture":
        return read_hourly_fixture(start, end)
    return fetch_yahoo_hourly(symbol, start, end)


validate_hourly = task(validate_hourly_prices, cache_policy=NO_CACHE)


@task(cache_policy=NO_CACHE)
def store_hourly(
    prices: pl.DataFrame, source: Source, require_db: bool, secret_block: str | None
) -> int:
    """Upsert hourly Silver without exposing credentials in task inputs.

    Args:
        prices: Validated hourly bars.
        source: Source label to store.
        require_db: Require PostgreSQL when there is no Secret block.
        secret_block: Optional Prefect Secret block name.

    Returns:
        Number of hourly bars upserted.
    """
    count = storage.load_hourly_prices(database_target(require_db, secret_block), prices, source)
    get_run_logger().info("Committed %d upserts to hourly_prices.", count)
    return count


@flow(name="hourly-ingestion", persist_result=False)
def ingest_hourly_gold(
    source: Source = "fixture",
    start_at: datetime | None = None,
    end_at: datetime | None = None,
    skip_db: bool = False,
    require_db: bool = False,
    database_secret_block: str | None = None,
) -> pl.DataFrame:
    """Collect hourly gold bars without changing the existing daily tables.

    Args:
        source: Fixture (synthetic gold) or Yahoo Finance (GC=F gold futures).
        start_at: First included UTC hour for a backfill, or None to catch up.
        end_at: First excluded UTC hour, or None for the latest safe cutoff.
        skip_db: Preview the rows without reading or writing the database.
        require_db: Require PostgreSQL when no Secret block is specified.
        database_secret_block: Name of the Prefect Secret with the PostgreSQL URI.

    Returns:
        Validated hourly bars, ordered by UTC start time.

    Raises:
        ValueError: The arguments, time window, or fetched rows are invalid.
    """
    now = datetime.now(UTC)
    if skip_db and (require_db or database_secret_block):
        raise ValueError("An hourly preview cannot also request PostgreSQL.")
    symbol = config.resolve_symbols(source, None)[0]
    latest = None
    if start_at is None and not skip_db:
        database = database_target(require_db, database_secret_block)
        latest = storage.latest_hourly_bar(database, symbol)
    start, end = hourly_window(source, start_at, end_at, latest, now)
    get_run_logger().info(
        "Hourly %s: latest stored bar %s. Fetching [%s, %s), UTC and end exclusive.",
        symbol,
        latest or "not used (initial load, preview, or backfill)",
        start,
        end,
    )
    raw = fetch_hourly(source, symbol, start, end)
    prices = validate_hourly(raw, symbol, start, end)
    get_run_logger().info(
        "Validated %d hourly bars; latest bar starts %s.",
        prices.height,
        prices["bar_start_utc"][-1],
    )
    if not skip_db:
        store_hourly(prices, source, require_db, database_secret_block)
    return prices
