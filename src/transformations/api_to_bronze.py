"""API -> Bronze ingestion steps.

Decides what to request per symbol (initial history or incremental window),
validates the response structurally, quarantines unusable records and appends
everything else to Bronze unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Sequence

from src.api.fmp_client import FMPClient
from src.utils.config import PipelineConfig, get_config
from src.utils.logging_utils import get_logger
from src.validation import data_quality as dq
from src.warehouse import audit
from src.warehouse.loader import insert_raw_records

logger = get_logger(__name__)

BRONZE_STOCK_PRICES = "BRONZE.STOCK_PRICES_RAW"
BRONZE_COMPANY_PROFILE = "BRONZE.COMPANY_PROFILE_RAW"

LOAD_TYPE_INITIAL = "INITIAL"
LOAD_TYPE_INCREMENTAL = "INCREMENTAL"

_PRICES_TEMPLATE = "transformations/bronze/insert_stock_prices_raw.sql"
_PROFILE_TEMPLATE = "transformations/bronze/insert_company_profile_raw.sql"


@dataclass(frozen=True)
class FetchPlanItem:
    """One API request to make."""

    symbol: str
    start_date: date
    end_date: date
    load_type: str

    def as_dict(self) -> dict[str, Any]:
        """XCom friendly representation."""
        return {
            "symbol": self.symbol,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "load_type": self.load_type,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "FetchPlanItem":
        return cls(
            symbol=payload["symbol"],
            start_date=date.fromisoformat(payload["start_date"]),
            end_date=date.fromisoformat(payload["end_date"]),
            load_type=payload["load_type"],
        )


@dataclass
class IngestResult:
    """Outcome of ingesting one symbol."""

    symbol: str
    load_type: str
    received_records: int = 0
    loaded_records: int = 0
    rejected_records: int = 0
    duplicate_keys: int = 0
    max_trade_date: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "load_type": self.load_type,
            "received_records": self.received_records,
            "loaded_records": self.loaded_records,
            "rejected_records": self.rejected_records,
            "duplicate_keys": self.duplicate_keys,
            "max_trade_date": self.max_trade_date,
        }


def plan_symbol_load(
    connection: Any,
    symbol: str,
    *,
    today: date,
    config: PipelineConfig | None = None,
) -> FetchPlanItem:
    """Decide the date range to request for one symbol.

    No watermark means this symbol has never been ingested successfully, so the
    configured history is loaded. Otherwise the window starts a few days before
    the last successfully loaded trade date, which picks up late vendor
    corrections; the Silver MERGE makes the overlap harmless.
    """
    config = config or get_config()
    watermark = audit.get_watermark(
        connection,
        layer=audit.LAYER_BRONZE,
        table_name=BRONZE_STOCK_PRICES,
        symbol=symbol,
    )
    last_loaded = watermark.get("LAST_LOADED_DATE") if watermark else None

    if last_loaded is None:
        start_date = today - timedelta(days=config.load.initial_lookback_days)
        load_type = LOAD_TYPE_INITIAL
    else:
        last_date = last_loaded if isinstance(last_loaded, date) else date.fromisoformat(str(last_loaded))
        start_date = last_date - timedelta(days=config.load.incremental_overlap_days)
        load_type = LOAD_TYPE_INCREMENTAL

    # Never request more than one window's worth of data in a single call.
    earliest_allowed = today - timedelta(days=config.load.max_days_per_request)
    start_date = max(start_date, earliest_allowed)
    start_date = min(start_date, today)

    item = FetchPlanItem(symbol=symbol, start_date=start_date, end_date=today, load_type=load_type)
    logger.info(
        "fetch plan symbol=%s load_type=%s from=%s to=%s",
        item.symbol,
        item.load_type,
        item.start_date,
        item.end_date,
    )
    return item


def build_fetch_plan(
    connection: Any,
    *,
    today: date,
    symbols: Sequence[str] | None = None,
    config: PipelineConfig | None = None,
) -> list[dict[str, Any]]:
    """Build the fetch plan for every configured symbol."""
    config = config or get_config()
    symbols = symbols if symbols is not None else config.symbols
    return [plan_symbol_load(connection, symbol, today=today, config=config).as_dict() for symbol in symbols]


def ingest_symbol(
    connection: Any,
    client: FMPClient,
    item: FetchPlanItem,
    *,
    run_id: str,
    batch_id: str,
    config: PipelineConfig | None = None,
) -> IngestResult:
    """Fetch one symbol, quarantine unusable records and append the rest to Bronze."""
    config = config or get_config()
    result = IngestResult(symbol=item.symbol, load_type=item.load_type)

    records = client.get_historical_prices(item.symbol, item.start_date, item.end_date)
    result.received_records = len(records)

    accepted, rejected = dq.partition_price_records(records, item.symbol)
    result.rejected_records = len(rejected)
    result.duplicate_keys = dq.count_duplicate_keys(accepted)

    for rejection in rejected:
        audit.quarantine_record(
            connection,
            run_id=run_id,
            batch_id=batch_id,
            table_name=BRONZE_STOCK_PRICES,
            symbol=rejection.symbol,
            trade_date=rejection.trade_date,
            error_type=rejection.error_type,
            error_message=rejection.error_message,
            raw_record=rejection.record,
        )

    result.loaded_records = insert_raw_records(
        connection,
        _PRICES_TEMPLATE,
        accepted,
        symbol=item.symbol,
        source_system=config.source_system,
        api_endpoint=config.api.historical_prices_endpoint,
        batch_id=batch_id,
        run_id=run_id,
    )

    latest = dq.max_trade_date(accepted)
    result.max_trade_date = latest.isoformat() if latest else None

    summary = dq.summarise(
        received=result.received_records,
        accepted=len(accepted),
        rejected=result.rejected_records,
        duplicate_keys=result.duplicate_keys,
    )
    logger.info("symbol=%s %s", item.symbol, summary.as_log_line())

    # The watermark only moves when data was actually stored.
    if latest is not None and result.loaded_records:
        audit.set_watermark(
            connection,
            layer=audit.LAYER_BRONZE,
            table_name=BRONZE_STOCK_PRICES,
            symbol=item.symbol,
            run_id=run_id,
            batch_id=batch_id,
            last_loaded_date=latest,
        )

    return result


def ingest_company_profile(
    connection: Any,
    client: FMPClient,
    symbol: str,
    *,
    run_id: str,
    batch_id: str,
    config: PipelineConfig | None = None,
) -> IngestResult:
    """Fetch the company profile for one symbol and append it to Bronze."""
    config = config or get_config()
    result = IngestResult(symbol=symbol, load_type=LOAD_TYPE_INCREMENTAL)

    profile = client.get_company_profile(symbol)
    if not profile:
        return result

    result.received_records = 1
    result.loaded_records = insert_raw_records(
        connection,
        _PROFILE_TEMPLATE,
        [profile],
        symbol=symbol,
        source_system=config.source_system,
        api_endpoint=config.api.company_profile_endpoint,
        batch_id=batch_id,
        run_id=run_id,
    )
    return result
