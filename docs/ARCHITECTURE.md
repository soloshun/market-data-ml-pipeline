# Architecture

| Layer | Responsibility | Location |
| --- | --- | --- |
| Source | One download per Yahoo ticker or a packaged synthetic CSV | `ingest.py`, `src/market_data/data/market_sample.csv` |
| Bronze (local lesson) | Preserve provider fields, including adjusted close when present, and attach ticker, source and ingestion time | Local Parquet under `ingest_date=YYYY-MM-DD/symbol=…/` |
| Silver | Validate, sort and retain typed daily OHLCV | `validate.py`, `market_prices` |
| Gold | Calculate daily percent return, trailing means and volatility | `transform.py`, `market_features` |

The source adapter unwraps pandas' date index and, if present, the requested ticker from a two-level column index. It then hands the data to Polars. Bronze preserves the provider fields after this structural normalization. Polars converts dates to session calendar dates, sorts by symbol and date, and rejects a batch with missing fields, out-of-window dates, invalid OHLCV or duplicate keys. An invalid batch can leave Bronze available for investigation. Silver and Gold for that run are unwritten.

## Read the code in this order

| Read | Follow this part of one fixture run |
| --- | --- |
| 1. [cli.py](../src/market_data/cli.py) | Start at `run_command`. See how source, symbols, dates and database options become flow parameters. |
| 2. [flows/historical_ingestion.py](../src/market_data/flows/historical_ingestion.py) | Start at `ingest_historical_prices`. Read the four task calls at the bottom before reading their bodies. |
| 3. [ingest.py](../src/market_data/ingest.py) | Follow `read_fixture`, then `save_snapshot`. Open `normalize_yahoo` when comparing live data with the fixture. |
| 4. [validate.py](../src/market_data/validate.py) | Read `validate_prices` from required fields through type conversion, row checks, and sorting. |
| 5. [transform.py](../src/market_data/transform.py) | Read `build_features` and use `shift(1)` to identify where the previous close comes from. |
| 6. [storage.py](../src/market_data/storage.py) and [SQLite schema](../src/market_data/data/sqlite_schema.sql) | Follow `load_market_data` and compare the `ON CONFLICT` key with each primary key. |
| 7. [test_transform.py](../tests/test_transform.py) and [test_idempotency.py](../tests/test_idempotency.py) | Check where warm-up, backfill effects and rollback are asserted. |

## Orchestration and computation

```mermaid
flowchart TD
    A["CLI or Prefect deployment"] --> B["fetch_prices"]
    B -->|Temporary fetch error| C["Wait and retry, up to twice"]
    C --> B
    B -->|Local snapshot enabled| D["save_bronze"]
    D --> E["validate_and_transform"]
    B -->|Snapshot disabled| E
    E -->|Invalid batch| F["Flow fails before the database write"]
    E -->|Valid batch| G["store_market_data"]
    G -->|Database configured| H["Commit Silver and Gold"]
    G -->|Preview| I["Show features and report database skipped"]
```


Prefect runs `fetch_prices`, then `validate_and_transform` and `store_market_data`. With Bronze enabled, `save_bronze` runs between fetch and validation. Only the fetch task retries. An empty Yahoo result receives a bounded retry because the library sometimes logs an upstream error and returns an empty frame. Schema or data validation errors fail immediately. The two opt-in failures use the fixture and require `--skip-db`.

Polars computes features using session observations for each symbol. Daily return is `100 × (close / previous_close − 1)`. The 5- and 20-observation moving averages include the current close. Five-observation volatility is the sample standard deviation of daily returns in percentage points. The first return is null, SMA values start at the fifth and twentieth observations, and volatility starts at the sixth observation. Every feature uses current and past closes.

## Storage and repeatability

```mermaid
sequenceDiagram
    participant Task as Load task
    participant DB as SQLite or PostgreSQL
    participant PL as Polars
    Task->>DB: Begin the database transaction
    Task->>DB: Upsert Silver price rows
    Task->>DB: Read complete close history for those symbols
    DB-->>Task: Current and historical closes
    Task->>PL: Recompute trailing features
    PL-->>Task: Gold rows
    Task->>DB: Upsert Gold rows
    alt Both writes succeed
        Task->>DB: Commit Silver and Gold
    else A step raises an error
        Task->>DB: Roll back both writes
    end
```

Both tables in each database use the same `(symbol, trade_date)` primary key. Gold references Silver with a foreign key. The app submits parameterized upserts, so an existing key gets updated and a new key gets inserted. The database protects the key even if a duplicate reaches it from a separate process.

The load task holds one database connection. It writes Silver, reads the stored close history for those symbols, recomputes Gold, and commits both tables together. SQLite uses one writer at a time. PostgreSQL uses per-symbol locks so runs for independent symbols can write concurrently. A short backfill can change a later return or moving average. The full-history rebuild handles this and is easy to follow for daily workshop data. It costs more as history grows. Preview features use only the fetched window, so values near its beginning may differ from Gold stored in the database.

The transaction covers both Silver and Gold in either database. Local Parquet snapshots are written earlier and can survive database failure. PostgreSQL symbol locks coordinate writers using this pipeline. If two successful runs carry different source values for the same key, the later transaction can overwrite the earlier one. This is a latest-value dataset. Rerun timestamps and Bronze filenames change by design.

Local runs keep the optional Parquet snapshot under `BRONZE_DIR`. The hosted configuration fetches fresh Yahoo Finance rows and sends them straight to validation. Silver is its first stored layer. The worker loads the Supabase URI from the Prefect Secret block named in the deployment parameters. To replay the exact original provider response in a later hosted version, add durable object storage for Bronze.

## Handoff to the ML workshop

Join Silver and Gold on `(symbol, trade_date)` to build a training dataset. Define prediction time before choosing features. Features using the current close become available after that session closes. Keep future labels separate and split by time for evaluation. Record the data snapshot and feature definition so an experiment can be repeated. Synthetic examples use the reserved symbol `XAUUSD-DEMO`.
