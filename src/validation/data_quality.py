"""Validation applied to API responses before anything reaches Bronze.

Two levels of validation exist in MarketFlow, deliberately:

* here, in Python: *structural* checks on the API response. A payload that is
  not a usable record at all (not an object, no parseable date, no numeric
  close) is quarantined as ``INVALID_API_RECORD`` and never loaded.
* in Snowflake SQL (``SILVER.VW_STOCK_PRICES_STAGING``): *business* checks on
  typed columns - null required fields, OHLC consistency, volume, duplicates.

Everything that is structurally usable goes to Bronze untouched, so Silver and
Gold can always be rebuilt from the raw layer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable

ERROR_INVALID_API_RECORD = "INVALID_API_RECORD"

REQUIRED_PRICE_FIELDS = ("date", "close")


@dataclass(frozen=True)
class RejectedRecord:
    """A record that will be quarantined instead of loaded."""

    record: Any
    symbol: str | None
    trade_date: str | None
    error_type: str
    error_message: str


@dataclass(frozen=True)
class ValidationSummary:
    """Counters for one validation pass, used for logging and the audit log."""

    received: int
    accepted: int
    rejected: int
    duplicate_keys: int

    def as_log_line(self) -> str:
        return (
            f"records received={self.received} valid={self.accepted} "
            f"rejected={self.rejected} duplicate_keys={self.duplicate_keys}"
        )


def parse_trade_date(value: Any) -> date | None:
    """Parse an API date value, accepting ``YYYY-MM-DD`` and ISO timestamps."""
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def parse_number(value: Any) -> float | None:
    """Parse a numeric API value, tolerating numbers sent as strings."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def validate_price_record(record: Any) -> list[str]:
    """Return the structural problems of one API price record."""
    if not isinstance(record, dict):
        return [f"record is {type(record).__name__}, expected an object"]

    problems: list[str] = []
    for field in REQUIRED_PRICE_FIELDS:
        if record.get(field) is None:
            problems.append(f"missing required field '{field}'")

    if record.get("date") is not None and parse_trade_date(record.get("date")) is None:
        problems.append(f"date {record.get('date')!r} is not a valid ISO date")

    if record.get("close") is not None and parse_number(record.get("close")) is None:
        problems.append(f"close {record.get('close')!r} is not numeric")

    return problems


def partition_price_records(
    records: Iterable[Any],
    symbol: str,
) -> tuple[list[dict[str, Any]], list[RejectedRecord]]:
    """Split API records into the ones to load and the ones to quarantine."""
    accepted: list[dict[str, Any]] = []
    rejected: list[RejectedRecord] = []

    for record in records:
        problems = validate_price_record(record)
        if problems:
            raw = record if isinstance(record, dict) else {"value": repr(record)}
            trade_date = parse_trade_date(raw.get("date")) if isinstance(record, dict) else None
            rejected.append(
                RejectedRecord(
                    record=raw,
                    symbol=str(raw.get("symbol") or symbol).upper() if isinstance(record, dict) else symbol.upper(),
                    trade_date=trade_date.isoformat() if trade_date else None,
                    error_type=ERROR_INVALID_API_RECORD,
                    error_message="; ".join(problems),
                )
            )
        else:
            accepted.append(record)

    return accepted, rejected


def count_duplicate_keys(records: Iterable[dict[str, Any]]) -> int:
    """Count records whose (symbol, date) key already appeared in the payload.

    De-duplication itself happens in Snowflake, where the full history is
    visible; this count only makes a noisy API response visible in the logs.
    """
    seen: set[tuple[str, str]] = set()
    duplicates = 0
    for record in records:
        trade_date = parse_trade_date(record.get("date"))
        key = (str(record.get("symbol", "")).upper(), trade_date.isoformat() if trade_date else "")
        if key in seen:
            duplicates += 1
        else:
            seen.add(key)
    return duplicates


def summarise(
    received: int,
    accepted: int,
    rejected: int,
    duplicate_keys: int = 0,
) -> ValidationSummary:
    """Build the counters for one validation pass."""
    return ValidationSummary(
        received=received,
        accepted=accepted,
        rejected=rejected,
        duplicate_keys=duplicate_keys,
    )


def max_trade_date(records: Iterable[dict[str, Any]]) -> date | None:
    """Latest parseable trade date in a set of records."""
    dates = [parse_trade_date(record.get("date")) for record in records]
    valid = [value for value in dates if value is not None]
    return max(valid) if valid else None
