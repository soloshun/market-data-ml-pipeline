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


def test_incremental_run_recovers_missed_batches_and_preserves_keys(tmp_path, monkeypatch):
    database = tmp_path / "catch-up.sqlite3"
    monkeypatch.setenv("SQLITE_DB_PATH", str(database))
    monkeypatch.setenv("PREFECT_SERVER_ANALYTICS_ENABLED", "false")
    real_read = historical_ingestion.read_fixture
    windows = []

    def record_window(start, end):
        windows.append((start, end))
        return real_read(start, end)

    monkeypatch.setattr(historical_ingestion, "read_fixture", record_window)
    with prefect_test_harness():
        _, _, initial = historical_ingestion.ingest_historical_prices(
            source="fixture", end_date=date(2025, 1, 18), write_bronze=False, incremental=True
        )
        assert initial == (10, 10)
        assert storage.latest_dates(database, [FIXTURE_SYMBOL]) == {
            FIXTURE_SYMBOL: date(2025, 1, 17)
        }

        for expected_start, fetched in [(date(2025, 1, 11), 15), (date(2025, 1, 25), 5)]:
            _, _, loaded = historical_ingestion.ingest_historical_prices(
                source="fixture", end_date=date(2025, 2, 1), write_bronze=False, incremental=True
            )
            assert windows[-1] == (expected_start, date(2025, 2, 1))
            assert loaded == (fetched, 20)
            assert storage.row_counts(database, FIXTURE_SYMBOL) == [
                ("market_prices", 20, 20),
                ("market_features", 20, 20),
            ]
            assert storage.latest_dates(database, [FIXTURE_SYMBOL]) == {
                FIXTURE_SYMBOL: date(2025, 1, 31)
            }


def test_default_bounds_and_synthetic_symbol_safety():
    assert historical_ingestion.config.resolve_window("fixture", None, None) == (
        FIXTURE_START,
        FIXTURE_END,
    )
    with pytest.raises(ValueError, match="reserved"):
        historical_ingestion.config.resolve_symbols("yfinance", [FIXTURE_SYMBOL])
