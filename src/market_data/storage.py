"""Load Silver and Gold in one PostgreSQL transaction."""

from datetime import date

import polars as pl
import psycopg
from psycopg.conninfo import conninfo_to_dict

from market_data.config import Source
from market_data.transform import build_features

UPSERT_PRICES = """
    INSERT INTO market_data.market_prices
        (symbol, trade_date, open, high, low, close, volume, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, trade_date) DO UPDATE SET
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
        close = EXCLUDED.close, volume = EXCLUDED.volume,
        source = EXCLUDED.source, ingested_at = now()
"""
UPSERT_FEATURES = """
    INSERT INTO market_data.market_features
        (symbol, trade_date, daily_return, sma_5, sma_20, volatility_5)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, trade_date) DO UPDATE SET
        daily_return = EXCLUDED.daily_return, sma_5 = EXCLUDED.sma_5,
        sma_20 = EXCLUDED.sma_20, volatility_5 = EXCLUDED.volatility_5, computed_at = now()
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


def latest_dates(url: str, symbols: list[str]) -> dict[str, date]:
    """Read the newest stored trade date for each requested ticker.

    Args:
        url: PostgreSQL connection string.
        symbols: Tickers to look up in the Silver table.

    Returns:
        A mapping of tickers with stored history to their latest trade dates.
    """
    with connect(url) as connection:
        rows = connection.execute(
            """
            SELECT symbol, max(trade_date) FROM market_data.market_prices
            WHERE symbol = ANY(%s) GROUP BY symbol
            """,
            (symbols,),
        ).fetchall()
    return dict(rows)


def load_market_data(url: str, prices: pl.DataFrame, source: Source) -> int:
    """Upsert Silver prices and rebuild affected Gold features in one transaction.

    Args:
        url: PostgreSQL connection string for a database with the workshop schema.
        prices: Validated Silver rows in symbol, trade_date, open, high, low,
            close, volume column order.
        source: Source label recorded with the prices.

    Returns:
        Number of Gold rows upserted across the full history of the affected tickers.

    Raises:
        psycopg.Error: A database operation fails. The transaction rolls back.
    """
    symbols = sorted(prices.get_column("symbol").unique().to_list())
    with connect(url) as connection, connection.cursor() as cursor:
        # Serialize overlapping loads. Sorted acquisition gives multi-symbol runs the same order.
        for symbol in symbols:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"market_data:{symbol}",),
            )
        cursor.executemany(UPSERT_PRICES, ((*row, source) for row in prices.iter_rows()))

        # Rebuild from persisted prices, so small backfills have both warm-up and following rows.
        history = cursor.execute(
            """
            SELECT symbol, trade_date, close FROM market_data.market_prices
            WHERE symbol = ANY(%s)
            """,
            (symbols,),
        ).fetchall()
        features = build_features(
            pl.DataFrame(history, schema=["symbol", "trade_date", "close"], orient="row")
        )
        cursor.executemany(UPSERT_FEATURES, features.iter_rows())
    return features.height


def row_counts(url: str, symbol: str) -> list[tuple[str, int, int]]:
    """Count stored rows and distinct keys in both workshop tables.

    Args:
        url: PostgreSQL connection string.
        symbol: Ticker to count in Silver and Gold.

    Returns:
        Tuples of table name, row count, and distinct key count.
    """
    with connect(url) as connection:
        return connection.execute(
            """
            SELECT 'market_prices', count(*), count(DISTINCT (symbol, trade_date))
            FROM market_data.market_prices WHERE symbol = %s
            UNION ALL
            SELECT 'market_features', count(*), count(DISTINCT (symbol, trade_date))
            FROM market_data.market_features WHERE symbol = %s
            """,
            (symbol, symbol),
        ).fetchall()
