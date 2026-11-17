"""Audit layer: run log, watermarks and quarantine.

Nothing in the pipeline decides on its own what "already processed" means:
every layer asks :func:`get_watermark`, and a watermark is only advanced by
:func:`set_watermark` after the step that produced the data succeeded. A failed
or half-finished run therefore never moves the pipeline forward.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from src.warehouse.loader import execute_file, fetch_all, fetch_one
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

# Layers used in AUDIT.LOAD_WATERMARK.
LAYER_BRONZE = "BRONZE"
LAYER_SILVER = "SILVER"
LAYER_GOLD = "GOLD"

# Watermarks that apply to a whole table rather than a single symbol.
ALL_SYMBOLS = "_ALL_"

# Lower bound used when a watermark does not exist yet.
EPOCH_TS = "1900-01-01 00:00:00"

STATUS_RUNNING = "RUNNING"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"

_MAX_ERROR_MESSAGE = 5000


def start_step(
    connection: Any,
    *,
    run_id: str,
    dag_id: str,
    task_id: str,
    batch_id: str,
    layer: str,
) -> None:
    """Open the audit row for a pipeline step (idempotent across retries)."""
    execute_file(
        connection,
        "transformations/audit/30_start_run_log.sql",
        {
            "run_id": run_id,
            "dag_id": dag_id,
            "task_id": task_id,
            "batch_id": batch_id,
            "layer": layer,
        },
    )
    logger.info("step started dag=%s task=%s run_id=%s batch_id=%s", dag_id, task_id, run_id, batch_id)


def finish_step(
    connection: Any,
    *,
    run_id: str,
    dag_id: str,
    task_id: str,
    status: str,
    source_record_count: int | None = None,
    target_record_count: int | None = None,
    rejected_record_count: int | None = None,
    error_message: str | None = None,
) -> None:
    """Close the audit row for a pipeline step with its outcome and counts."""
    execute_file(
        connection,
        "transformations/audit/31_finish_run_log.sql",
        {
            "run_id": run_id,
            "dag_id": dag_id,
            "task_id": task_id,
            "status": status,
            "source_record_count": source_record_count,
            "target_record_count": target_record_count,
            "rejected_record_count": rejected_record_count,
            "error_message": (error_message or "")[:_MAX_ERROR_MESSAGE] or None,
        },
    )
    logger.info(
        "step finished dag=%s task=%s status=%s source=%s target=%s rejected=%s",
        dag_id,
        task_id,
        status,
        source_record_count,
        target_record_count,
        rejected_record_count,
    )


def record_failure(
    connection: Any,
    *,
    run_id: str,
    dag_id: str,
    task_id: str,
    batch_id: str | None = None,
    layer: str | None = None,
    error_message: str | None = None,
) -> None:
    """Mark a step as FAILED, inserting the row when the task never opened one."""
    execute_file(
        connection,
        "transformations/audit/38_record_failure.sql",
        {
            "run_id": run_id,
            "dag_id": dag_id,
            "task_id": task_id,
            "batch_id": batch_id,
            "layer": layer,
            "error_message": (error_message or "task failed")[:_MAX_ERROR_MESSAGE],
        },
    )
    logger.error("step failed dag=%s task=%s run_id=%s", dag_id, task_id, run_id)


def get_watermark(
    connection: Any,
    *,
    layer: str,
    table_name: str,
    symbol: str = ALL_SYMBOLS,
) -> dict[str, Any] | None:
    """Return the watermark row, or ``None`` when nothing was ever loaded."""
    return fetch_one(
        connection,
        "transformations/audit/33_select_load_watermark.sql",
        {"layer": layer, "table_name": table_name, "symbol": symbol},
    )


def set_watermark(
    connection: Any,
    *,
    layer: str,
    table_name: str,
    symbol: str = ALL_SYMBOLS,
    run_id: str,
    batch_id: str,
    last_loaded_date: date | str | None = None,
    last_processed_ts: datetime | str | None = None,
) -> None:
    """Advance a watermark. Call this only after the step has succeeded."""
    execute_file(
        connection,
        "transformations/audit/32_merge_load_watermark.sql",
        {
            "layer": layer,
            "table_name": table_name,
            "symbol": symbol,
            "run_id": run_id,
            "batch_id": batch_id,
            "last_loaded_date": _as_text(last_loaded_date),
            "last_processed_ts": _as_text(last_processed_ts),
        },
    )
    logger.info(
        "watermark updated layer=%s table=%s symbol=%s date=%s ts=%s",
        layer,
        table_name,
        symbol,
        last_loaded_date,
        last_processed_ts,
    )


def clear_run_quality_artifacts(connection: Any, *, run_id: str, table_name: str) -> None:
    """Remove quality rows from an earlier attempt of the same run."""
    params = {"run_id": run_id, "table_name": table_name}
    execute_file(connection, "transformations/audit/34_delete_dq_errors_for_run.sql", params)
    execute_file(connection, "transformations/audit/35_delete_dq_results_for_run.sql", params)


def quarantine_record(
    connection: Any,
    *,
    run_id: str,
    batch_id: str,
    table_name: str,
    symbol: str | None,
    trade_date: str | None,
    error_type: str,
    error_message: str,
    raw_record: Any,
) -> None:
    """Store one rejected record in AUDIT.DATA_QUALITY_ERRORS."""
    execute_file(
        connection,
        "transformations/audit/36_insert_dq_error.sql",
        {
            "run_id": run_id,
            "batch_id": batch_id,
            "table_name": table_name,
            "symbol": symbol,
            "trade_date": trade_date,
            "error_type": error_type,
            "error_message": error_message[:1000],
            "raw_record": json.dumps(raw_record, default=str),
        },
    )


def fetch_quality_summary(connection: Any, *, run_id: str, table_name: str) -> list[dict[str, Any]]:
    """Return the data quality results recorded for a run."""
    return fetch_all(
        connection,
        "transformations/audit/37_select_dq_summary.sql",
        {"run_id": run_id, "table_name": table_name},
    )


def log_quality_summary(results: list[dict[str, Any]], *, table_name: str) -> None:
    """Write the data quality summary to the task log."""
    if not results:
        logger.info("no data quality results recorded for %s", table_name)
        return
    logger.info("data quality summary for %s:", table_name)
    for row in results:
        logger.info(
            "  %-20s %-4s failed=%s of %s",
            row.get("CHECK_NAME"),
            row.get("STATUS"),
            row.get("RECORDS_FAILED"),
            row.get("RECORDS_CHECKED"),
        )


def _as_text(value: date | datetime | str | None) -> str | None:
    """Render dates and timestamps the way the SQL files expect them."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)
