"""### silver_to_gold

Builds the analytical layer from `SILVER.STOCK_PRICES`:

* `GOLD.DAILY_STOCK_PERFORMANCE` - daily return, 7/30 day moving averages and
  30 day volatility, calculated with Snowflake window functions.
* `GOLD.STOCK_PERFORMANCE_SUMMARY` - trailing 7D/30D/90D metrics per symbol.

Scheduling is asset driven: the DAG runs when `bronze_to_silver` has updated
Silver. Only symbols touched by that window are recalculated, with extra
history read so the 30 day windows are complete at the first recalculated row.

Returns, moving averages and volatility are produced by a single MERGE rather
than three passes over the same rows - the calculations share one window
definition and rewriting the table three times would be wasted work.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from airflow.sdk import dag, get_current_context, task

from src.orchestration.common import (
    GOLD_STOCK_PERFORMANCE_ASSET,
    SILVER_STOCK_PRICES_ASSET,
    TAGS,
    default_args,
    new_batch_id,
    run_id_from_context,
    task_id_from_context,
)
from src.transformations import silver_to_gold as transform
from src.transformations.bronze_to_silver import ProcessingWindow
from src.utils.logging_utils import get_logger
from src.warehouse import audit
from src.warehouse.connection import snowflake_connection

logger = get_logger(__name__)

DAG_ID = "silver_to_gold"
LAYER = audit.LAYER_GOLD


@dag(
    dag_id=DAG_ID,
    description="Build the Gold analytical datasets from Silver stock prices.",
    schedule=[SILVER_STOCK_PRICES_ASSET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=[*TAGS, "gold"],
    doc_md=__doc__,
)
def silver_to_gold() -> None:
    @task
    def prepare_run() -> dict[str, str]:
        context = get_current_context()
        identity = {"run_id": run_id_from_context(context), "batch_id": new_batch_id()}
        logger.info("starting gold run_id=%s batch_id=%s", identity["run_id"], identity["batch_id"])
        return identity

    @task.short_circuit
    def check_silver_availability() -> dict[str, Any]:
        """Find the unprocessed Silver window; skip the run when there is none."""
        with snowflake_connection() as connection:
            window = transform.plan_window(connection)
        if not window.has_work:
            logger.info("no new Silver records - skipping the rest of the run")
            return {}
        return window.as_dict()

    @task
    def calculate_daily_performance(identity: dict[str, str], window: dict[str, Any]) -> int:
        """Recalculate daily returns, moving averages and volatility."""
        context = get_current_context()
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
            rows = transform.calculate_daily_performance(
                connection,
                window=ProcessingWindow.from_dict(window),
                run_id=run_id,
                batch_id=batch_id,
            )
            audit.finish_step(
                connection,
                run_id=run_id,
                dag_id=DAG_ID,
                task_id=task_id,
                status=audit.STATUS_SUCCESS,
                source_record_count=int(window.get("pending_records") or 0),
                target_record_count=rows,
            )
        return rows

    @task
    def calculate_summary_metrics(identity: dict[str, str]) -> int:
        """Rebuild the trailing 7D/30D/90D summary for every symbol."""
        context = get_current_context()
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
            rows = transform.calculate_summary_metrics(connection, run_id=run_id, batch_id=batch_id)
            audit.finish_step(
                connection,
                run_id=run_id,
                dag_id=DAG_ID,
                task_id=task_id,
                status=audit.STATUS_SUCCESS,
                target_record_count=rows,
            )
        return rows

    @task(outlets=[GOLD_STOCK_PERFORMANCE_ASSET])
    def record_run_audit(identity: dict[str, str], window: dict[str, Any]) -> dict[str, int]:
        """Summarise the run, advance the Gold watermark, publish the asset."""
        context = get_current_context()
        run_id, batch_id = identity["run_id"], identity["batch_id"]
        task_id = task_id_from_context(context)
        processing_window = ProcessingWindow.from_dict(window)

        with snowflake_connection() as connection:
            counts = transform.fetch_run_counts(connection, run_id=run_id)
            audit.start_step(
                connection,
                run_id=run_id,
                dag_id=DAG_ID,
                task_id=task_id,
                batch_id=batch_id,
                layer=LAYER,
            )
            audit.finish_step(
                connection,
                run_id=run_id,
                dag_id=DAG_ID,
                task_id=task_id,
                status=audit.STATUS_SUCCESS,
                source_record_count=processing_window.pending_records,
                target_record_count=counts.daily_rows + counts.summary_rows,
            )
            transform.advance_watermark(
                connection,
                window=processing_window,
                run_id=run_id,
                batch_id=batch_id,
            )

        logger.info(
            "gold run complete run_id=%s daily_rows=%d summary_rows=%d processed up to %s",
            run_id,
            counts.daily_rows,
            counts.summary_rows,
            processing_window.to_ts,
        )
        return {"daily_rows": counts.daily_rows, "summary_rows": counts.summary_rows}

    identity = prepare_run()
    window = check_silver_availability()
    daily = calculate_daily_performance(identity, window)
    summary = calculate_summary_metrics(identity)
    audit_step = record_run_audit(identity, window)

    daily >> summary >> audit_step


silver_to_gold()
