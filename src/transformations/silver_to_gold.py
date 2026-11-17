"""Silver -> Gold transformation steps.

The analytics themselves are Snowflake SQL (``sql/transformations/gold``).
This module selects the window, runs the statements and reports row counts.

Incremental window
------------------
Gold processes Silver rows by ``LOADED_AT``:

    from_ts = AUDIT.LOAD_WATERMARK.LAST_PROCESSED_TS (GOLD / GOLD.DAILY_STOCK_PERFORMANCE)
    to_ts   = MAX(LOADED_AT) in Silver at planning time

Only the symbols touched in that window are recalculated, from their earliest
changed trade date onwards, with ``gold_lookback_days`` of extra history so the
30 day windows are complete.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.transformations.bronze_to_silver import ProcessingWindow
from src.utils.config import get_config
from src.utils.logging_utils import get_logger
from src.warehouse import audit
from src.warehouse.loader import execute_file, fetch_one

logger = get_logger(__name__)

GOLD_DAILY_PERFORMANCE = "GOLD.DAILY_STOCK_PERFORMANCE"
GOLD_PERFORMANCE_SUMMARY = "GOLD.STOCK_PERFORMANCE_SUMMARY"

_SQL = "transformations/gold"


@dataclass
class GoldLoadResult:
    """Row counts produced by one Gold run."""

    source_records: int = 0
    daily_rows: int = 0
    summary_rows: int = 0


def plan_window(connection: Any) -> ProcessingWindow:
    """Work out which Silver rows still have to be aggregated."""
    watermark = audit.get_watermark(
        connection, layer=audit.LAYER_GOLD, table_name=GOLD_DAILY_PERFORMANCE
    )
    from_ts = audit.EPOCH_TS
    if watermark and watermark.get("LAST_PROCESSED_TS"):
        from_ts = str(watermark["LAST_PROCESSED_TS"])

    row = fetch_one(connection, f"{_SQL}/20_pending_silver_stock_prices.sql", {"from_ts": from_ts}) or {}
    pending = int(row.get("PENDING_RECORDS") or 0)
    max_loaded_at = row.get("MAX_LOADED_AT")

    window = ProcessingWindow(
        from_ts=from_ts,
        to_ts=str(max_loaded_at) if max_loaded_at is not None else None,
        pending_records=pending,
    )
    logger.info(
        "planned gold window from_ts=%s to_ts=%s pending=%d",
        window.from_ts,
        window.to_ts,
        window.pending_records,
    )
    return window


def calculate_daily_performance(
    connection: Any,
    *,
    window: ProcessingWindow,
    run_id: str,
    batch_id: str,
) -> int:
    """Recalculate returns, moving averages and volatility for the window."""
    if not window.has_work:
        logger.info("nothing to recalculate for %s", GOLD_DAILY_PERFORMANCE)
        return 0

    lookback_days = get_config().load.gold_lookback_days
    rows = execute_file(
        connection,
        f"{_SQL}/21_merge_daily_stock_performance.sql",
        {
            "from_ts": window.from_ts,
            "to_ts": window.to_ts,
            # Negative: DATEADD moves backwards from the first changed trade date.
            "lookback_days": -abs(lookback_days),
            "run_id": run_id,
            "batch_id": batch_id,
        },
    )
    logger.info("gold daily performance rows merged=%d lookback_days=%d", rows, lookback_days)
    return rows


def calculate_summary_metrics(connection: Any, *, run_id: str, batch_id: str) -> int:
    """Rebuild the trailing 7D/30D/90D summary for every symbol."""
    rows = execute_file(
        connection,
        f"{_SQL}/22_merge_stock_performance_summary.sql",
        {"run_id": run_id, "batch_id": batch_id},
    )
    logger.info("gold summary rows merged=%d", rows)
    return rows


def fetch_run_counts(connection: Any, *, run_id: str) -> GoldLoadResult:
    """Row counts written by this run, for the audit log."""
    row = fetch_one(connection, f"{_SQL}/23_gold_record_counts.sql", {"run_id": run_id}) or {}
    return GoldLoadResult(
        daily_rows=int(row.get("DAILY_ROWS") or 0),
        summary_rows=int(row.get("SUMMARY_ROWS") or 0),
    )


def advance_watermark(
    connection: Any,
    *,
    window: ProcessingWindow,
    run_id: str,
    batch_id: str,
) -> None:
    """Move the Gold watermark to the end of the processed window."""
    if not window.has_work:
        return
    audit.set_watermark(
        connection,
        layer=audit.LAYER_GOLD,
        table_name=GOLD_DAILY_PERFORMANCE,
        run_id=run_id,
        batch_id=batch_id,
        last_processed_ts=window.to_ts,
    )
