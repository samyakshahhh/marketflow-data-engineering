"""### market_api_to_bronze

Fetches end-of-day prices from the Financial Modeling Prep API and appends them
to `BRONZE.STOCK_PRICES_RAW` exactly as they were received.

* The first run for a symbol loads the configured history (`initial_lookback_days`).
* Later runs start a few days before that symbol's watermark, so late vendor
  corrections are picked up without reloading everything.
* Symbols are fetched by a dynamically mapped task - one Airflow task per
  symbol, so a single bad symbol does not take the whole run down with it.
* Structurally unusable API records are quarantined in
  `AUDIT.DATA_QUALITY_ERRORS` instead of being loaded or dropped.

On success the Bronze asset is updated, which is what triggers `bronze_to_silver`.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from airflow.sdk import dag, get_current_context, task

from src.api.fmp_client import FMPClient
from src.orchestration.common import (
    BRONZE_STOCK_PRICES_ASSET,
    TAGS,
    default_args,
    new_batch_id,
    run_id_from_context,
    task_id_from_context,
)
from src.transformations import api_to_bronze
from src.utils.config import get_config
from src.utils.logging_utils import get_logger
from src.warehouse import audit
from src.warehouse.connection import snowflake_connection

logger = get_logger(__name__)

DAG_ID = "market_api_to_bronze"
LAYER = audit.LAYER_BRONZE


@dag(
    dag_id=DAG_ID,
    description="Fetch end-of-day prices from FMP and append them to Snowflake Bronze.",
    schedule="0 6 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=[*TAGS, "bronze"],
    doc_md=__doc__,
)
def market_api_to_bronze() -> None:
    @task
    def prepare_run() -> dict[str, str]:
        """Create the batch id that every layer of this run will carry."""
        context = get_current_context()
        identity = {"run_id": run_id_from_context(context), "batch_id": new_batch_id()}
        logger.info("starting ingestion run_id=%s batch_id=%s", identity["run_id"], identity["batch_id"])
        return identity

    @task
    def build_fetch_plan(identity: dict[str, str]) -> list[dict[str, Any]]:
        """Decide the date range to request for each configured symbol."""
        config = get_config()
        today = datetime.now(timezone.utc).date()
        with snowflake_connection() as connection:
            plan = api_to_bronze.build_fetch_plan(connection, today=today, config=config)
        logger.info("fetch plan built symbols=%d run_id=%s", len(plan), identity["run_id"])
        return plan

    @task(max_active_tis_per_dag=2)
    def ingest_symbol(item: dict[str, Any], identity: dict[str, str]) -> dict[str, Any]:
        """Fetch one symbol and append its records to Bronze."""
        context = get_current_context()
        plan_item = api_to_bronze.FetchPlanItem.from_dict(item)
        run_id, batch_id = identity["run_id"], identity["batch_id"]
        task_id = task_id_from_context(context)

        with snowflake_connection() as connection:
            audit.start_step(
                connection,
                run_id=run_id,
                dag_id=DAG_ID,
                task_id=task_id,
                batch_id=batch_id,
                layer=LAYER,
            )
            with FMPClient() as client:
                result = api_to_bronze.ingest_symbol(
                    connection,
                    client,
                    plan_item,
                    run_id=run_id,
                    batch_id=batch_id,
                )
            audit.finish_step(
                connection,
                run_id=run_id,
                dag_id=DAG_ID,
                task_id=task_id,
                status=audit.STATUS_SUCCESS,
                source_record_count=result.received_records,
                target_record_count=result.loaded_records,
                rejected_record_count=result.rejected_records,
            )
        return result.as_dict()

    @task(outlets=[BRONZE_STOCK_PRICES_ASSET])
    def record_run_audit(identity: dict[str, str], results: list[dict[str, Any]]) -> dict[str, int]:
        """Summarise the run in AUDIT.ETL_RUN_LOG and publish the Bronze asset."""
        context = get_current_context()
        run_id, batch_id = identity["run_id"], identity["batch_id"]
        totals = {
            "received": sum(int(row["received_records"]) for row in results),
            "loaded": sum(int(row["loaded_records"]) for row in results),
            "rejected": sum(int(row["rejected_records"]) for row in results),
        }

        with snowflake_connection() as connection:
            audit.start_step(
                connection,
                run_id=run_id,
                dag_id=DAG_ID,
                task_id=task_id_from_context(context),
                batch_id=batch_id,
                layer=LAYER,
            )
            audit.finish_step(
                connection,
                run_id=run_id,
                dag_id=DAG_ID,
                task_id=task_id_from_context(context),
                status=audit.STATUS_SUCCESS,
                source_record_count=totals["received"],
                target_record_count=totals["loaded"],
                rejected_record_count=totals["rejected"],
            )

        logger.info(
            "bronze ingestion complete run_id=%s batch_id=%s symbols=%d received=%d loaded=%d rejected=%d",
            run_id,
            batch_id,
            len(results),
            totals["received"],
            totals["loaded"],
            totals["rejected"],
        )
        return totals

    identity = prepare_run()
    plan = build_fetch_plan(identity)
    ingested = ingest_symbol.partial(identity=identity).expand(item=plan)
    record_run_audit(identity, ingested)


market_api_to_bronze()
