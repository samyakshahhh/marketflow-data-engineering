"""Shared Airflow building blocks: assets, default arguments, run identity.

Keeping these here means ``dags/`` contains nothing but the three pipeline
definitions, and the DAGs stay small enough to read end to end.
"""

from __future__ import annotations

import os
import uuid
from datetime import timedelta
from typing import Any

from airflow.sdk import Asset

from src.utils.logging_utils import get_logger
from src.warehouse import audit
from src.warehouse.connection import snowflake_connection

logger = get_logger(__name__)

# Assets describe the data each DAG produces. Downstream DAGs are scheduled on
# them, so Silver runs when Bronze has actually been updated instead of at a
# guessed clock time.
#
# The Snowflake provider validates asset URIs as
# snowflake://<account>/<database>/<schema>/<table> and refuses to parse a DAG
# whose URI omits the account, so it is read from the same .env value the
# connection uses. The fallback only exists so the DAGs import in unit tests.
_SNOWFLAKE_ACCOUNT = (os.environ.get("SNOWFLAKE_ACCOUNT") or "local").lower()
_ASSET_PREFIX = f"snowflake://{_SNOWFLAKE_ACCOUNT}/financial_market_db"

BRONZE_STOCK_PRICES_ASSET = Asset(f"{_ASSET_PREFIX}/bronze/stock_prices_raw")
SILVER_STOCK_PRICES_ASSET = Asset(f"{_ASSET_PREFIX}/silver/stock_prices")
GOLD_STOCK_PERFORMANCE_ASSET = Asset(f"{_ASSET_PREFIX}/gold/daily_stock_performance")

OWNER = "data-engineering"
TAGS = ["marketflow", "snowflake"]


# Which medallion layer each DAG writes to, used when a failure has to be
# recorded for a task that never got as far as opening its own audit row.
DAG_LAYERS = {
    "market_api_to_bronze": audit.LAYER_BRONZE,
    "company_profile_ingestion": audit.LAYER_BRONZE,
    "bronze_to_silver": audit.LAYER_SILVER,
    "silver_to_gold": audit.LAYER_GOLD,
}


def new_batch_id() -> str:
    """Unique id for one ingestion batch, carried through every layer."""
    return uuid.uuid4().hex


def run_id_from_context(context: dict[str, Any]) -> str:
    """The Airflow dag_run id, used as RUN_ID in the audit tables."""
    dag_run = context.get("dag_run")
    run_id = getattr(dag_run, "run_id", None)
    if run_id:
        return str(run_id)
    task_instance = context.get("task_instance") or context.get("ti")
    return str(getattr(task_instance, "run_id", "unknown_run"))


def task_id_from_context(context: dict[str, Any]) -> str:
    """Task id, including the map index for dynamically mapped tasks."""
    task_instance = context.get("task_instance") or context.get("ti")
    task_id = str(getattr(task_instance, "task_id", "unknown_task"))
    map_index = getattr(task_instance, "map_index", -1)
    if map_index is not None and map_index >= 0:
        return f"{task_id}[{map_index}]"
    return task_id


def record_task_failure(context: dict[str, Any]) -> None:
    """``on_failure_callback``: write the failure to AUDIT.ETL_RUN_LOG.

    A module level function rather than a closure, so Airflow can serialise the
    DAG by importable reference.
    """
    exception = context.get("exception")
    message = f"{type(exception).__name__}: {exception}" if exception else "task failed"
    dag_id = str(getattr(context.get("dag"), "dag_id", "unknown_dag"))
    try:
        with snowflake_connection() as connection:
            audit.record_failure(
                connection,
                run_id=run_id_from_context(context),
                dag_id=dag_id,
                task_id=task_id_from_context(context),
                layer=DAG_LAYERS.get(dag_id),
                error_message=message,
            )
    except Exception:
        # Never let audit logging hide the original failure.
        logger.exception("could not record the failure in AUDIT.ETL_RUN_LOG")


def default_args() -> dict[str, Any]:
    """Default task arguments: retries with backoff and an audit callback."""
    return {
        "owner": OWNER,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
        "on_failure_callback": record_task_failure,
    }
