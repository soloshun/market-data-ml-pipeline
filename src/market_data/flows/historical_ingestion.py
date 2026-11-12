"""The Prefect tasks map directly to the steps shown during the workshop."""

from datetime import date, timedelta
from pathlib import Path

import polars as pl
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.runtime import task_run
from prefect.tasks import NO_CACHE

from market_data import config, storage
from market_data.config import OVERLAP_DAYS, Database, FailureDemo, Source
from market_data.ingest import TransientFetchError, fetch_yahoo, read_fixture, save_snapshot
from market_data.transform import build_features
from market_data.validate import validate_prices

Windows = dict[str, tuple[date, date]]
Frames = dict[str, pl.DataFrame]


def retry_transient(_task, _task_run, state) -> bool:
    """Decide whether Prefect should retry a failed fetch task.

    Args:
        _task: Prefect task definition supplied to the callback.
        _task_run: Prefect task run supplied to the callback.
        state: Failed task state containing the exception.

    Returns:
        True when the fetch raised a TransientFetchError.
    """
    return isinstance(state.result(raise_on_failure=False), TransientFetchError)


@task(
    retries=2, retry_delay_seconds=[2, 5], retry_condition_fn=retry_transient, cache_policy=NO_CACHE
)
def fetch_prices(source: Source, windows: Windows, demo_failure: FailureDemo | None) -> Frames:
    """Fetch raw rows for each ticker and log the requested window.

    Args:
        source: Data source to read.
        windows: Mapping of tickers to inclusive starts and exclusive ends.
        demo_failure: Optional fixture exercise that simulates a timeout or missing column.

    Returns:
        Raw Polars frames keyed by ticker.
    """
    logger = get_run_logger()
    if demo_failure == "transient" and task_run.run_count == 1:
        logger.warning("Workshop simulation: first attempt failed. Watch Prefect retry this task.")
        raise TransientFetchError("Simulated network timeout.")

    batches = {}
    for symbol, (start, end) in windows.items():
        raw = read_fixture(start, end) if source == "fixture" else fetch_yahoo(symbol, start, end)
        if demo_failure == "invalid":
            raw = raw.drop("High")
        logger.info(
            "Fetched %d %s rows for %s in [%s, %s).", raw.height, source, symbol, start, end
        )
        batches[symbol] = raw
    return batches


@task(cache_policy=NO_CACHE)
def save_bronze(source: Source, batches: Frames) -> None:
    """Save a local Parquet snapshot for each fetched ticker.

    Args:
        source: Label to record with each snapshot.
        batches: Raw Polars frames keyed by ticker.
    """
    for symbol, raw in batches.items():
        path = save_snapshot(raw, symbol, source, config.bronze_directory())
        get_run_logger().info("Bronze snapshot for %s: %s", symbol, path)


@task(cache_policy=NO_CACHE)
def validate_and_transform(batches: Frames, windows: Windows) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Prepare Silver prices and calculate a Gold preview for the fetched window.

    Args:
        batches: Raw Polars frames keyed by ticker.
        windows: Requested date window for each ticker.

    Returns:
        Validated Silver prices and Gold preview features, in that order.
    """
    prices = pl.concat(
        [validate_prices(raw, symbol, *windows[symbol]) for symbol, raw in batches.items()]
    )
    features = build_features(prices)
    get_run_logger().info(
        "Validated %d Silver rows. Prepared %d Gold preview rows. Rejected/dropped 0 rows.",
        prices.height,
        features.height,
    )
    return prices, features


def _database_target(require_db: bool, secret_block: str | None) -> Database:
    """Load the deployment credential at runtime or use the local database settings."""
    if secret_block:
        try:
            secret = Secret.load(secret_block)
        except ValueError:
            raise ValueError(
                f"Create a Secret block named {secret_block!r} in this Prefect workspace."
            ) from None
        url = secret.get()
        if not isinstance(url, str) or not url.strip():
            raise ValueError(f"Secret block {secret_block!r} needs a PostgreSQL connection string.")
        return url
    return config.database_target(require_postgres=require_db)


@task(cache_policy=NO_CACHE)
def store_market_data(
    prices: pl.DataFrame,
    source: Source,
    skip_db: bool,
    require_db: bool = False,
    database_secret_block: str | None = None,
) -> tuple[int, int] | None:
    """Persist validated prices and features when database loading is enabled.

    Args:
        prices: Validated Silver prices in the expected storage column order.
        source: Source label to store with each price row.
        skip_db: Whether to bypass storage for a preview.
        require_db: Use DATABASE_URL for direct PostgreSQL testing when no block is provided.
        database_secret_block: Prefect Secret block name for a PostgreSQL connection string.

    Returns:
        Silver and Gold upsert counts, or None when the database load was skipped.
    """
    logger = get_run_logger()
    if skip_db:
        logger.warning("Database skipped. No Silver or Gold rows were written.")
        return None
    database = _database_target(require_db, database_secret_block)
    feature_count = storage.load_market_data(database, prices, source)
    destination = f"SQLite at {database}" if isinstance(database, Path) else "PostgreSQL"
    logger.info(
        "Committed %d Silver and %d Gold upserts to %s in one transaction.",
        prices.height,
        feature_count,
        destination,
    )
    return prices.height, feature_count


@flow(name="historical-ingestion", persist_result=False)
def ingest_historical_prices(
    symbols: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    source: Source | None = None,
    incremental: bool = False,
    skip_db: bool = False,
    require_db: bool = False,
    demo_failure: FailureDemo | None = None,
    write_bronze: bool = True,
    database_secret_block: str | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, tuple[int, int] | None]:
    """Fetch historical bars, validate prices, and calculate and optionally load features.

    Args:
        symbols: Requested tickers. The source determines the default ticker.
        start_date: Inclusive start date for a manual window.
        end_date: Exclusive end date. Defaults to the source's latest supported boundary.
        source: Fixture or Yahoo Finance. Defaults to the local configuration.
        incremental: Resume from stored dates with overlap.
        skip_db: Preview without a database write. Bronze can still be written separately.
        require_db: Use DATABASE_URL for direct PostgreSQL testing when no block is provided.
        demo_failure: Optional fixture exercise. Requires a bounded run with skip_db.
        write_bronze: Write local Parquet snapshots before validation when True.
        database_secret_block: Prefect Secret block name for a PostgreSQL connection string.

    Returns:
        Silver prices, Gold preview features, and optional Silver and Gold upsert counts.

    Raises:
        ValueError: The parameters or fetched rows fail validation.
        TransientFetchError: Fetch retries are exhausted.
    """
    source = source or config.data_source()
    symbols = config.resolve_symbols(source, symbols)
    if demo_failure and (source != "fixture" or not skip_db or incremental):
        raise ValueError("Failure demos require fixture mode, --skip-db, and a bounded run.")
    if incremental and start_date is not None:
        raise ValueError("Use backfill for a manual start date.")
    if skip_db and (incremental or require_db or database_secret_block):
        raise ValueError("Catch-up and explicit PostgreSQL runs need database writes.")
    if require_db and not database_secret_block:
        config.database_target(require_postgres=True)
    start, end = config.resolve_window(source, start_date, end_date)
    windows = dict.fromkeys(symbols, (start, end))
    if incremental:
        database = _database_target(require_db, database_secret_block)
        for symbol, latest in storage.latest_dates(database, symbols).items():
            if latest >= end:
                raise ValueError(f"{symbol}: stored date {latest} reaches the exclusive end {end}.")
            windows[symbol] = (latest - timedelta(days=OVERLAP_DAYS - 1), end)

    batches = fetch_prices(source, windows, demo_failure)
    if write_bronze:
        save_bronze(source, batches)
    else:
        get_run_logger().info("Bronze snapshot skipped. Passing fetched data to Silver validation.")
    prices, features = validate_and_transform(batches, windows)
    loaded = store_market_data(prices, source, skip_db, require_db, database_secret_block)
    return prices, features, loaded
