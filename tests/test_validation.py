"""Validation applied to API responses before anything reaches Bronze."""

from __future__ import annotations

from datetime import date

from src.validation import data_quality as dq
from tests.conftest import price_record


def test_a_well_formed_record_has_no_problems() -> None:
    assert dq.validate_price_record(price_record()) == []


def test_missing_required_fields_are_reported() -> None:
    problems = dq.validate_price_record(price_record(close=None))

    assert any("close" in problem for problem in problems)


def test_an_unparseable_date_is_reported() -> None:
    problems = dq.validate_price_record(price_record(date="01-05-2024"))

    assert any("ISO date" in problem for problem in problems)


def test_a_non_numeric_close_is_reported() -> None:
    problems = dq.validate_price_record(price_record(close="n/a"))

    assert any("numeric" in problem for problem in problems)


def test_a_non_object_record_is_reported() -> None:
    assert dq.validate_price_record("not-a-record")


def test_partition_separates_loadable_records_from_rejects() -> None:
    records = [
        price_record(),
        price_record(date="2024-05-02", close=None),
        price_record(date="not-a-date"),
    ]

    accepted, rejected = dq.partition_price_records(records, "AAPL")

    assert len(accepted) == 1
    assert len(rejected) == 2
    assert {rejection.error_type for rejection in rejected} == {dq.ERROR_INVALID_API_RECORD}
    assert rejected[0].symbol == "AAPL"
    # The rejected payload is preserved so it can be inspected in quarantine.
    assert rejected[0].record["date"] == "2024-05-02"


def test_numbers_sent_as_strings_are_accepted() -> None:
    accepted, rejected = dq.partition_price_records([price_record(close="169.30")], "AAPL")

    assert len(accepted) == 1
    assert not rejected


def test_duplicate_business_keys_are_counted() -> None:
    records = [
        price_record(date="2024-05-01"),
        price_record(date="2024-05-01"),
        price_record(date="2024-05-02"),
    ]

    assert dq.count_duplicate_keys(records) == 1


def test_duplicate_count_is_zero_for_distinct_keys() -> None:
    records = [price_record(date="2024-05-01"), price_record(date="2024-05-02")]

    assert dq.count_duplicate_keys(records) == 0


def test_max_trade_date_ignores_unparseable_dates() -> None:
    records = [
        price_record(date="2024-05-01"),
        price_record(date="2024-05-03"),
        price_record(date="broken"),
    ]

    assert dq.max_trade_date(records) == date(2024, 5, 3)


def test_max_trade_date_returns_none_without_usable_records() -> None:
    assert dq.max_trade_date([]) is None


def test_summary_reports_the_counters() -> None:
    summary = dq.summarise(received=100, accepted=97, rejected=3, duplicate_keys=1)

    assert summary.received == 100
    assert summary.accepted == 97
    assert summary.rejected == 3
    assert "received=100" in summary.as_log_line()


def test_parse_helpers_handle_edge_cases() -> None:
    assert dq.parse_trade_date("2024-05-01T00:00:00") == date(2024, 5, 1)
    assert dq.parse_trade_date("") is None
    assert dq.parse_number(True) is None
    assert dq.parse_number("1e3") == 1000.0
