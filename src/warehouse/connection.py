"""Snowflake connectivity.

Inside Airflow the connection comes from the Snowflake provider hook, which
reads the ``snowflake_default`` connection that docker-compose builds from
``.env``. Outside Airflow (setup script, integration tests) the same credentials
are read directly from environment variables.

Credentials are never written to logs, SQL files or Snowflake tables.

Note on the package name: this package is called ``warehouse`` rather than
``snowflake`` so that ``import snowflake.connector`` can never resolve to
project code.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator

from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

DEFAULT_CONN_ID = "snowflake_default"
REQUIRED_ENV_VARS = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")


class SnowflakeConfigurationError(RuntimeError):
    """Raised when no usable Snowflake credentials are available."""


def _connect_via_airflow(conn_id: str) -> Any | None:
    """Open a connection through the Snowflake provider hook.

    Returns ``None`` when Airflow is not installed or the connection is not
    defined, so the caller can fall back to environment variables.
    """
    try:
        from airflow.exceptions import AirflowNotFoundException
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    except ImportError:
        logger.debug("Airflow is not available, using environment variables")
        return None

    try:
        hook = SnowflakeHook(snowflake_conn_id=conn_id)
        connection = hook.get_conn()
    except AirflowNotFoundException:
        logger.warning("Airflow connection %s is not defined, using environment variables", conn_id)
        return None

    logger.debug("connected to Snowflake through Airflow connection %s", conn_id)
    return connection


def _connect_via_env() -> Any:
    """Open a connection using SNOWFLAKE_* environment variables."""
    import snowflake.connector

    missing = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
    if missing:
        raise SnowflakeConfigurationError(
            "missing Snowflake environment variables: " + ", ".join(missing)
        )

    connection = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE") or None,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE") or None,
        database=os.getenv("SNOWFLAKE_DATABASE", "FINANCIAL_MARKET_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA") or None,
        client_session_keep_alive=False,
        application="MarketFlow",
    )
    logger.debug("connected to Snowflake using environment variables")
    return connection


def open_connection(conn_id: str | None = None) -> Any:
    """Open a Snowflake DB-API connection. The caller must close it."""
    conn_id = conn_id or os.getenv("SNOWFLAKE_CONN_ID", DEFAULT_CONN_ID)
    return _connect_via_airflow(conn_id) or _connect_via_env()


@contextmanager
def snowflake_connection(conn_id: str | None = None) -> Iterator[Any]:
    """Context manager yielding a Snowflake connection and closing it after use."""
    connection = open_connection(conn_id)
    try:
        yield connection
    finally:
        try:
            connection.close()
        except Exception:  # pragma: no cover - closing must never mask a real error
            logger.warning("failed to close the Snowflake connection", exc_info=True)
