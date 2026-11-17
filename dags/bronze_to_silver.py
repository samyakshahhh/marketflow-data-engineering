"""### bronze_to_silver

Turns raw Bronze payloads into clean, typed, de-duplicated `SILVER.STOCK_PRICES`.

Scheduling is asset driven: this DAG runs when `market_api_to_bronze` has
actually updated Bronze, not at a fixed clock time.

The transformation itself is Snowflake SQL. Validation rules live in
`SILVER.VW_STOCK_PRICES_STAGING`, so the quarantine statement, the data quality
summary and the MERGE can never disagree about what "valid" means.

Quarantining, merging and recording the quality results happen inside one
Snowflake transaction: a failure cannot leave Silver updated without the
matching audit trail. The watermark is advanced by a later task, so a crash
in between only causes the same window to be reprocessed - which the MERGE
handles.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from airflow.sdk import dag, get_current_context, task

from src.orchestration.common import (
    BRONZE_STOCK_PRICES_ASSET,
    SILVER_STOCK_PRICES_ASSET,
    TAGS,
    default_args,
    new_batch_id,
    run_id_from_context,
    task_id_from_context,
)
from src.transformations import bronze_to_silver as transform
from src.utils.logging_utils import get_logger
from src.warehouse import audit
from src.warehouse.connection import snowflake_connection
from src.warehouse.loader import transaction

logger = get_logger(__name__)

DAG_ID = "bronze_to_silver"
LAYER = audit.LAYER_SILVER


@dag(
    dag_id=DAG_ID,
    description="Validate, de-duplicate and load Bronze stock prices into Silver.",
    schedule=[BRONZE_STOCK_PRICES_ASSET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=[*TAGS, "silver"],
    doc_md=__doc__,
)
def bronze_to_silver() -> None:
    @task
    def prepare_run() -> dict[str, str]:
        """Identify this run; the batch id is reused by Gold for traceability."""
        context = get_current_context()
        identity = {"run_id": run_id_from_context(context), "batch_id": new_batch_id()}
        logger.info("starting silver run_id=%s batch_id=%s", identity["run_id"], identity["batch_id"])
        return identity

    @task.short_circuit
    def plan_window() -> dict[str, Any]:
        """Find the unprocessed Bronze window; skip the run when there is none."""
        with snowflake_connection() as connection:
            window = transform.plan_window(
                connection,
                table_name=transform.SILVER_STOCK_PRICES,
                pending_sql="10_pending_bronze_stock_prices.sql",
            )
        if not window.has_work:
            logger.info("no new Bronze records - skipping the rest of the run")
            return {}
        return window.as_dict()

    @task
    def transform_to_silver(identity: dict[str, str], window: dict[str, Any]) -> dict[str, Any]:
        """Quarantine, de-duplicate and merge the window in one transaction."""
        context = get_current_context()
        processing_window = transform.ProcessingWindow.from_dict(window)
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
            with transaction(connection):
                result = transform.transform_stock_prices(
                    connection,
                    window=processing_window,
                    run_id=run_id,
                    batch_id=batch_id,
                )
            audit.finish_step(
                connection,
                run_id=run_id,
                dag_id=DAG_ID,
                task_id=task_id,
                status=audit.STATUS_SUCCESS,
                source_record_count=result.source_records,
                target_record_count=result.merged_records,
                rejected_record_count=result.rejected_records,
            )

        return {
            "source_records": result.source_records,
            "valid_records": result.valid_records,
            "invalid_records": result.invalid_records,
            "duplicate_records": result.duplicate_records,
            "merged_records": result.merged_records,
        }

    @task
    def check_data_quality(identity: dict[str, str]) -> list[dict[str, Any]]:
        """Read back the persisted quality results and log the summary.

        A failing check is reported but does not fail the task: invalid records
        are quarantined by design, and a vendor sending a few bad rows should
        not stop the pipeline. The results live in AUDIT.DATA_QUALITY_RESULTS.
        """
        with snowflake_connection() as connection:
            results = audit.fetch_quality_summary(
                connection,
                run_id=identity["run_id"],
                table_name=transform.SILVER_STOCK_PRICES,
            )
        audit.log_quality_summary(results, table_name=transform.SILVER_STOCK_PRICES)
        failed = [row for row in results if str(row.get("STATUS")) != "PASS"]
        if failed:
            logger.warning(
                "%d data quality check(s) reported failing records - see AUDIT.DATA_QUALITY_ERRORS",
                len(failed),
            )
        return [
            {
                "check_name": row.get("CHECK_NAME"),
                "status": row.get("STATUS"),
                "records_checked": row.get("RECORDS_CHECKED"),
                "records_failed": row.get("RECORDS_FAILED"),
            }
            for row in results
        ]

    @task(outlets=[SILVER_STOCK_PRICES_ASSET])
    def advance_watermark(identity: dict[str, str], window: dict[str, Any]) -> dict[str, Any]:
        """Move the Silver watermark and publish the Silver asset."""
        processing_window = transform.ProcessingWindow.from_dict(window)
        with snowflake_connection() as connection:
            transform.advance_watermark(
                connection,
                table_name=transform.SILVER_STOCK_PRICES,
                window=processing_window,
                run_id=identity["run_id"],
                batch_id=identity["batch_id"],
            )
        logger.info(
            "silver run complete run_id=%s processed up to %s",
            identity["run_id"],
            processing_window.to_ts,
        )
        return window

    identity = prepare_run()
    window = plan_window()
    loaded = transform_to_silver(identity, window)
    quality = check_data_quality(identity)
    watermark = advance_watermark(identity, window)

    loaded >> quality >> watermark


bronze_to_silver()
