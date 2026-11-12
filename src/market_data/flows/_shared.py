"""Prefect helpers shared by the daily and hourly ingestion flows."""

from prefect.blocks.system import Secret

from market_data import config
from market_data.config import Database
from market_data.ingestion.sources import TransientFetchError


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


def database_target(require_db: bool, secret_block: str | None) -> Database:
    """Load the deployment credential at runtime or use the local database settings.

    Args:
        require_db: Require PostgreSQL when no Secret block is specified.
        secret_block: Optional name of the Prefect Secret containing a PostgreSQL URI.

    Returns:
        A path for SQLite or the PostgreSQL URI.

    Raises:
        ValueError: The requested PostgreSQL credential is missing or empty.
    """
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
