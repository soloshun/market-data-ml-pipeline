from datetime import date

import pytest
from prefect.blocks.system import Secret
from prefect.testing.utilities import prefect_test_harness

from market_data import storage
from market_data.config import FIXTURE_END, FIXTURE_START, FIXTURE_SYMBOL
from market_data.flows import historical_ingestion


def test_fixture_flow_with_retry_and_permanent_validation_failure(tmp_path, monkeypatch):
    monkeypatch.setenv("BRONZE_DIR", str(tmp_path))
    database = tmp_path / "local.sqlite3"
    monkeypatch.setenv("SQLITE_DB_PATH", str(database))
    monkeypatch.setenv("DATABASE_URL", "a local run should ignore this")
    monkeypatch.setenv("PREFECT_SERVER_ANALYTICS_ENABLED", "false")
    with prefect_test_harness():
        prices, features, loaded = historical_ingestion.ingest_historical_prices(
            source="fixture", skip_db=True, demo_failure="transient"
        )
        assert prices.height == features.height == 30
        assert loaded is None
        assert len(list(tmp_path.rglob("*.parquet"))) == 1

        local_prices, local_features, loaded = historical_ingestion.ingest_historical_prices(
            source="fixture", write_bronze=False
        )
        assert local_prices.equals(prices)
        assert local_features.equals(features)
        assert loaded == (30, 30)
        assert storage.row_counts(database, FIXTURE_SYMBOL) == [
            ("market_prices", 30, 30),
            ("market_features", 30, 30),
        ]
        assert len(list(tmp_path.rglob("*.parquet"))) == 1

        with pytest.raises(ValueError, match="missing required columns: High"):
            historical_ingestion.ingest_historical_prices(
                source="fixture", skip_db=True, demo_failure="invalid"
            )
        # A failed validation retains its raw snapshot for inspection.
        assert len(list(tmp_path.rglob("*.parquet"))) == 2

        with pytest.raises(ValueError, match="Create a Secret block"):
            historical_ingestion.ingest_historical_prices(
                source="fixture", incremental=True, database_secret_block="absent"
            )
        Secret(value="postgresql://example.invalid/market").save("workshop-test-database")

        def load_from_block(database, rows, source):
            assert database == "postgresql://example.invalid/market"
            assert rows.equals(prices)
            assert source == "fixture"
            return rows.height

        monkeypatch.setattr(storage, "load_market_data", load_from_block)
        _, _, loaded = historical_ingestion.ingest_historical_prices(
            source="fixture",
            write_bronze=False,
            database_secret_block="workshop-test-database",
        )
        assert loaded == (30, 30)


def test_incremental_window_uses_per_symbol_overlap(monkeypatch):
    monkeypatch.setattr(
        historical_ingestion.storage,
        "latest_dates",
        lambda url, symbols: {FIXTURE_SYMBOL: date(2025, 2, 5)},
    )

    def fetch(source, windows, demo_failure):
        assert source == "fixture"
        assert windows == {FIXTURE_SYMBOL: (date(2025, 1, 30), FIXTURE_END)}
        raise RuntimeError("window checked")

    monkeypatch.setattr(historical_ingestion, "fetch_prices", fetch)
    with pytest.raises(RuntimeError, match="window checked"):
        historical_ingestion.ingest_historical_prices.fn(source="fixture", incremental=True)


def test_default_bounds_and_synthetic_symbol_safety():
    assert historical_ingestion.config.resolve_window("fixture", None, None) == (
        FIXTURE_START,
        FIXTURE_END,
    )
    with pytest.raises(ValueError, match="reserved"):
        historical_ingestion.config.resolve_symbols("yfinance", [FIXTURE_SYMBOL])
