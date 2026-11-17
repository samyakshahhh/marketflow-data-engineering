"""Bronze -> Silver transformation steps.

All of the heavy lifting is Snowflake SQL (``sql/transformations/silver``);
this module decides *which window* to process, runs the statements in order and
reports counts back to Airflow.

Incremental window
------------------
Silver processes Bronze rows by ingestion timestamp:

    from_ts = AUDIT.LOAD_WATERMARK.LAST_PROCESSED_TS (SILVER / SILVER.STOCK_PRICES)
    to_ts   = MAX(INGESTED_AT) in Bronze at planning time

Pinning ``to_ts`` once means every statement in the run sees exactly the same
set of rows, even if a Bronze load finishes while Silver is running.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.warehouse import audit
from src.warehouse.loader import execute_file, fetch_one
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

SILVER_STOCK_PRICES = "SILVER.STOCK_PRICES"
SILVER_COMPANY_PROFILE = "SILVER.COMPANY_PROFILE"

_SQL = "transformations/silver"


@dataclass(frozen=True)
class ProcessingWindow:
    """The slice of source data a run will process."""

    from_ts: str
    to_ts: str | None
    pending_records: int

    @property
    def has_work(self) -> bool:
        return self.pending_records > 0 and self.to_ts is not None

    def as_dict(self) -> dict[str, Any]:
        """XCom friendly representation."""
        return {
            "from_ts": self.from_ts,
            "to_ts": self.to_ts,
            "pending_records": self.pending_records,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ProcessingWindow":
        return cls(
            from_ts=payload["from_ts"],
            to_ts=payload.get("to_ts"),
            pending_records=int(payload.get("pending_records", 0)),
        )


@dataclass
class SilverLoadResult:
    """Counts produced by one Silver load."""

    source_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    duplicate_records: int = 0
    merged_records: int = 0
    quality_results: list[dict[str, Any]] = field(default_factory=list)

    @property
    def rejected_records(self) -> int:
        return self.invalid_records + self.duplicate_records


def plan_window(
    connection: Any,
    *,
    table_name: str,
    pending_sql: str,
    layer: str = audit.LAYER_SILVER,
) -> ProcessingWindow:
    """Work out which source rows still have to be processed."""
    watermark = audit.get_watermark(connection, layer=layer, table_name=table_name)
    from_ts = audit.EPOCH_TS
    if watermark and watermark.get("LAST_PROCESSED_TS"):
        from_ts = str(watermark["LAST_PROCESSED_TS"])

    row = fetch_one(connection, f"{_SQL}/{pending_sql}", {"from_ts": from_ts}) or {}
    pending = int(row.get("PENDING_RECORDS") or 0)
    max_ingested_at = row.get("MAX_INGESTED_AT")

    window = ProcessingWindow(
        from_ts=from_ts,
        to_ts=str(max_ingested_at) if max_ingested_at is not None else None,
        pending_records=pending,
    )
    logger.info(
        "planned window table=%s from_ts=%s to_ts=%s pending=%d",
        table_name,
        window.from_ts,
        window.to_ts,
        window.pending_records,
    )
    return window


def transform_stock_prices(
    connection: Any,
    *,
    window: ProcessingWindow,
    run_id: str,
    batch_id: str,
) -> SilverLoadResult:
    """Validate, quarantine, de-duplicate and merge one window into Silver.

    The whole window is one Snowflake transaction: either the quarantine rows,
    the merged records and the quality results are all written, or none of them
    are and the watermark stays where it was.
    """
    result = SilverLoadResult()
    if not window.has_work:
        logger.info("nothing to process for %s", SILVER_STOCK_PRICES)
        return result

    params = {"from_ts": window.from_ts, "to_ts": window.to_ts}
    run_params = {**params, "run_id": run_id, "batch_id": batch_id}

    stats = fetch_one(connection, f"{_SQL}/17_stock_price_window_stats.sql", params) or {}
    result.source_records = int(stats.get("TOTAL_RECORDS") or 0)
    result.valid_records = int(stats.get("VALID_RECORDS") or 0)
    result.invalid_records = int(stats.get("INVALID_RECORDS") or 0)

    audit.clear_run_quality_artifacts(connection, run_id=run_id, table_name=SILVER_STOCK_PRICES)

    execute_file(connection, f"{_SQL}/11_quarantine_invalid_stock_prices.sql", run_params)
    result.duplicate_records = execute_file(
        connection, f"{_SQL}/12_quarantine_duplicate_stock_prices.sql", run_params
    )
    result.merged_records = execute_file(connection, f"{_SQL}/13_merge_stock_prices.sql", params)
    execute_file(connection, f"{_SQL}/14_record_stock_price_dq_results.sql", run_params)

    result.quality_results = audit.fetch_quality_summary(
        connection, run_id=run_id, table_name=SILVER_STOCK_PRICES
    )
    audit.log_quality_summary(result.quality_results, table_name=SILVER_STOCK_PRICES)
    logger.info(
        "silver load source=%d valid=%d invalid=%d duplicates=%d merged=%d",
        result.source_records,
        result.valid_records,
        result.invalid_records,
        result.duplicate_records,
        result.merged_records,
    )
    return result


def transform_company_profile(
    connection: Any,
    *,
    window: ProcessingWindow,
    run_id: str,
    batch_id: str,
) -> SilverLoadResult:
    """Merge the latest company profile per symbol into Silver."""
    result = SilverLoadResult()
    if not window.has_work:
        logger.info("nothing to process for %s", SILVER_COMPANY_PROFILE)
        return result

    params = {"from_ts": window.from_ts, "to_ts": window.to_ts}
    result.source_records = window.pending_records
    result.merged_records = execute_file(connection, f"{_SQL}/15_merge_company_profile.sql", params)
    result.valid_records = result.merged_records
    logger.info(
        "company profile load source=%d merged=%d run_id=%s batch_id=%s",
        result.source_records,
        result.merged_records,
        run_id,
        batch_id,
    )
    return result


def advance_watermark(
    connection: Any,
    *,
    table_name: str,
    window: ProcessingWindow,
    run_id: str,
    batch_id: str,
) -> None:
    """Move the Silver watermark to the end of the processed window."""
    if not window.has_work:
        return
    audit.set_watermark(
        connection,
        layer=audit.LAYER_SILVER,
        table_name=table_name,
        run_id=run_id,
        batch_id=batch_id,
        last_processed_ts=window.to_ts,
    )
