"""Load Silver and Gold in one transaction in SQLite or PostgreSQL."""

import sqlite3
from collections.abc import Generator
from contextlib import closing, contextmanager
from datetime import UTC, date, datetime
from importlib.resources import files
from pathlib import Path

import polars as pl
import psycopg
from psycopg.conninfo import conninfo_to_dict

from market_data.config import Database, Source
from market_data.ingestion.features import build_features, build_hourly_features

UPSERT_PRICES = """
    INSERT INTO {table}
        (symbol, trade_date, open, high, low, close, volume, source)
    VALUES ({values})
    ON CONFLICT (symbol, trade_date) DO UPDATE SET
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
        close = EXCLUDED.close, volume = EXCLUDED.volume,
        source = EXCLUDED.source, ingested_at = CURRENT_TIMESTAMP
"""
UPSERT_FEATURES = """
    INSERT INTO {table}
        (symbol, {time_key}, {return_column}, sma_5, sma_20, volatility_5)
    VALUES ({values})
    ON CONFLICT (symbol, {time_key}) DO UPDATE SET
        {return_column} = EXCLUDED.{return_column}, sma_5 = EXCLUDED.sma_5,
        sma_20 = EXCLUDED.sma_20, volatility_5 = EXCLUDED.volatility_5,
        computed_at = CURRENT_TIMESTAMP
"""
UPSERT_HOURLY = """
    INSERT INTO {table}
        (symbol, bar_start_utc, open, high, low, close, volume, source)
    VALUES ({values})
    ON CONFLICT (symbol, bar_start_utc) DO UPDATE SET
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
        close = EXCLUDED.close, volume = EXCLUDED.volume,
        source = EXCLUDED.source, ingested_at = CURRENT_TIMESTAMP
"""


def connect(url: str) -> psycopg.Connection:
    """Open a PostgreSQL connection and require TLS for remote hosts.

    Args:
        url: PostgreSQL connection string, including credentials where required.

    Returns:
        An open connection. Use it as a context manager to commit or roll back and close it.

    Raises:
        ValueError: The connection string is invalid or remote TLS is disabled.
        psycopg.Error: PostgreSQL rejects the connection or is unreachable.
    """
    try:
        options = conninfo_to_dict(url)
    except (psycopg.ProgrammingError, ValueError):
        raise ValueError("DATABASE_URL is invalid. Use the PostgreSQL Connect URI.") from None
    host = options.get("host")
    sslmode = options.get("sslmode", "require")
    loopback = {"localhost", "127.0.0.1", "::1"}
    local = host in loopback and options.get("hostaddr", host) in loopback
    if not local and sslmode not in {"require", "verify-ca", "verify-full"}:
        raise ValueError("Use sslmode=require or certificate verification for a remote database.")
    return psycopg.connect(url, sslmode=sslmode, connect_timeout=10)


@contextmanager
def _database_cursor(database: Database) -> Generator[sqlite3.Cursor | psycopg.Cursor, None, None]:
    """Open the selected database and manage its transaction and connection lifetime."""
    if isinstance(database, Path):
        database.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(database, timeout=10, isolation_level="IMMEDIATE")) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            schema = files("market_data").joinpath("data/sqlite_schema.sql").read_text()
            conn.executescript(schema)
            with conn:
                yield conn.cursor()
    else:
        with connect(database) as conn, conn.cursor() as cursor:
            yield cursor


def _placeholders(database: Database, count: int = 1) -> str:
    """Build bind placeholders for the selected driver."""
    return ", ".join(["?" if isinstance(database, Path) else "%s"] * count)


def _table(database: Database, name: str) -> str:
    """Qualify a fixed workshop table name for PostgreSQL."""
    return name if isinstance(database, Path) else f"market_data.{name}"


def latest_dates(database: Database, symbols: list[str]) -> dict[str, date]:
    """Read the newest stored trade date for each requested ticker.

    Args:
        database: SQLite file path or PostgreSQL connection string.
        symbols: Tickers to look up in the Silver table.

    Returns:
        Tickers with stored history and their latest trade dates.
    """
    if not symbols:
        return {}
    table = _table(database, "market_prices")
    placeholders = _placeholders(database, len(symbols))
    with _database_cursor(database) as cursor:
        cursor.execute(
            f"""
            SELECT symbol, max(trade_date) FROM {table}
            WHERE symbol IN ({placeholders}) GROUP BY symbol
            """,
            symbols,
        )
        return {
            symbol: date.fromisoformat(latest) if isinstance(latest, str) else latest
            for symbol, latest in cursor.fetchall()
        }


def load_market_data(database: Database, prices: pl.DataFrame, source: Source) -> int:
    """Upsert Silver prices and rebuild affected Gold features in one transaction.

    Args:
        database: SQLite file path or PostgreSQL connection string.
        prices: Validated Silver rows in symbol, trade_date, open, high, low,
            close, volume column order.
        source: Source label recorded with the prices.

    Returns:
        Number of Gold rows upserted across the full history of the affected tickers.
    """
    if prices.is_empty():
        return 0
    symbols = sorted(prices.get_column("symbol").unique().to_list())
    prices_table = _table(database, "market_prices")
    features_table = _table(database, "market_features")
    with _database_cursor(database) as cursor:
        if isinstance(database, Path):
            # SQLite's first write reserves the database for this transaction.
            prices = prices.with_columns(pl.col("trade_date").cast(pl.String))
        else:
            # PostgreSQL allows independent writers. Lock overlapping tickers in sorted order.
            for symbol in symbols:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"market_data:{symbol}",),
                )
        cursor.executemany(
            UPSERT_PRICES.format(table=prices_table, values=_placeholders(database, 8)),
            [(*row, source) for row in prices.iter_rows()],
        )

        # Rebuild from persisted prices so backfills have both warm-up and following rows.
        placeholders = _placeholders(database, len(symbols))
        cursor.execute(
            f"""
            SELECT symbol, trade_date, close FROM {prices_table}
            WHERE symbol IN ({placeholders})
            """,
            symbols,
        )
        features = build_features(
            pl.DataFrame(cursor.fetchall(), schema=["symbol", "trade_date", "close"], orient="row")
        )
        cursor.executemany(
            UPSERT_FEATURES.format(
                table=features_table,
                time_key="trade_date",
                return_column="daily_return",
                values=_placeholders(database, 6),
            ),
            features.rows(),
        )
    return features.height


def row_counts(database: Database, symbol: str) -> list[tuple[str, int, int]]:
    """Count stored rows and distinct keys for one ticker in both workshop tables.

    Args:
        database: SQLite file path or PostgreSQL connection string.
        symbol: Ticker to count in Silver and Gold.

    Returns:
        Tuples of table name, row count, and distinct key count.
    """
    prices = _table(database, "market_prices")
    features = _table(database, "market_features")
    placeholder = _placeholders(database)
    with _database_cursor(database) as cursor:
        cursor.execute(
            f"""
            SELECT 'market_prices', count(*), count(DISTINCT trade_date)
            FROM {prices} WHERE symbol = {placeholder}
            UNION ALL
            SELECT 'market_features', count(*), count(DISTINCT trade_date)
            FROM {features} WHERE symbol = {placeholder}
            """,
            (symbol, symbol),
        )
        return cursor.fetchall()


def latest_hourly_bar(database: Database, symbol: str) -> datetime | None:
    """Read the hourly watermark for one ticker.

    Args:
        database: SQLite file path or PostgreSQL connection string.
        symbol: Ticker to look up in the hourly Silver table.

    Returns:
        Latest stored bar start in UTC, or None for an empty ticker.
    """
    table = _table(database, "hourly_prices")
    with _database_cursor(database) as cursor:
        cursor.execute(
            f"SELECT max(bar_start_utc) FROM {table} WHERE symbol = {_placeholders(database)}",
            (symbol,),
        )
        latest = cursor.fetchone()[0]
    if isinstance(latest, str):
        latest = datetime.fromisoformat(latest)
    return latest.astimezone(UTC) if latest is not None else None


def load_hourly_prices(database: Database, prices: pl.DataFrame, source: Source) -> tuple[int, int]:
    """Upsert hourly Silver and rebuild hourly Gold in one transaction.

    Args:
        database: SQLite file path or PostgreSQL connection string.
        prices: Validated hourly bars in the storage column order.
        source: Source label to store alongside each bar.

    Returns:
        Silver upserts in this batch and Gold upserts across the affected tickers' history.
    """
    if prices.is_empty():
        return 0, 0
    symbols = sorted(prices.get_column("symbol").unique().to_list())
    prices_table = _table(database, "hourly_prices")
    features_table = _table(database, "hourly_features")
    if isinstance(database, Path):
        prices = prices.with_columns(pl.col("bar_start_utc").dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
    with _database_cursor(database) as cursor:
        if not isinstance(database, Path):
            for symbol in symbols:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"market_data:hourly:{symbol}",),
                )
        cursor.executemany(
            UPSERT_HOURLY.format(table=prices_table, values=_placeholders(database, 8)),
            [(*row, source) for row in prices.iter_rows()],
        )
        # Revisions affect later returns and rolling windows, so use stored Silver history.
        cursor.execute(
            f"""
            SELECT symbol, bar_start_utc, close FROM {prices_table}
            WHERE symbol IN ({_placeholders(database, len(symbols))})
            """,
            symbols,
        )
        features = build_hourly_features(
            pl.DataFrame(
                cursor.fetchall(), schema=["symbol", "bar_start_utc", "close"], orient="row"
            )
        )
        cursor.executemany(
            UPSERT_FEATURES.format(
                table=features_table,
                time_key="bar_start_utc",
                return_column="hourly_return",
                values=_placeholders(database, 6),
            ),
            features.rows(),
        )
    return prices.height, features.height
