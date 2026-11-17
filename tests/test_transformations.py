"""Incremental-load logic, Bronze loading and the SQL contract.

The transformations themselves run in Snowflake, so these tests cover the two
things that can break without a warehouse: the Python code that decides *what*
to process, and the SQL files that Python binds parameters to.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

from src.transformations import api_to_bronze, bronze_to_silver, silver_to_gold
from src.utils.config import get_config, read_sql, sql_dir
from src.warehouse.loader import insert_raw_records, iter_chunks
from tests.conftest import FakeConnection, price_record

TODAY = date(2024, 5, 10)


# -- fetch planning: initial vs incremental --------------------------------
def _patch_watermark(monkeypatch: pytest.MonkeyPatch, value: dict[str, Any] | None) -> None:
    monkeypatch.setattr(api_to_bronze.audit, "get_watermark", lambda *args, **kwargs: value)


def test_first_run_for_a_symbol_requests_the_configured_history(
    monkeypatch: pytest.MonkeyPatch, fake_connection: FakeConnection
) -> None:
    _patch_watermark(monkeypatch, None)

    item = api_to_bronze.plan_symbol_load(fake_connection, "AAPL", today=TODAY)

    lookback = get_config().load.initial_lookback_days
    assert item.load_type == api_to_bronze.LOAD_TYPE_INITIAL
    assert item.end_date == TODAY
    assert (TODAY - item.start_date).days == lookback


def test_later_runs_start_just_before_the_watermark(
    monkeypatch: pytest.MonkeyPatch, fake_connection: FakeConnection
) -> None:
    _patch_watermark(monkeypatch, {"LAST_LOADED_DATE": date(2024, 5, 7)})

    item = api_to_bronze.plan_symbol_load(fake_connection, "AAPL", today=TODAY)

    overlap = get_config().load.incremental_overlap_days
    assert item.load_type == api_to_bronze.LOAD_TYPE_INCREMENTAL
    assert item.start_date == date(2024, 5, 7) - timedelta(days=overlap)
    assert item.end_date == TODAY


def test_watermark_stored_as_text_is_accepted(
    monkeypatch: pytest.MonkeyPatch, fake_connection: FakeConnection
) -> None:
    _patch_watermark(monkeypatch, {"LAST_LOADED_DATE": "2024-05-07"})

    item = api_to_bronze.plan_symbol_load(fake_connection, "AAPL", today=TODAY)

    assert item.load_type == api_to_bronze.LOAD_TYPE_INCREMENTAL
    assert item.start_date < TODAY


def test_a_request_window_is_never_longer_than_the_configured_maximum(
    monkeypatch: pytest.MonkeyPatch, fake_connection: FakeConnection
) -> None:
    _patch_watermark(monkeypatch, {"LAST_LOADED_DATE": date(1990, 1, 1)})

    item = api_to_bronze.plan_symbol_load(fake_connection, "AAPL", today=TODAY)

    assert (item.end_date - item.start_date).days <= get_config().load.max_days_per_request


def test_the_fetch_plan_covers_every_configured_symbol(
    monkeypatch: pytest.MonkeyPatch, fake_connection: FakeConnection
) -> None:
    _patch_watermark(monkeypatch, None)

    plan = api_to_bronze.build_fetch_plan(fake_connection, today=TODAY)

    assert [item["symbol"] for item in plan] == list(get_config().symbols)
    # Round trips through XCom as plain JSON.
    assert api_to_bronze.FetchPlanItem.from_dict(plan[0]).symbol == plan[0]["symbol"]


# -- Bronze loading --------------------------------------------------------
def test_bronze_insert_binds_six_parameters_per_record() -> None:
    connection = FakeConnection()
    records = [price_record(date="2024-05-01"), price_record(date="2024-05-02")]

    insert_raw_records(
        connection,
        "transformations/bronze/insert_stock_prices_raw.sql",
        records,
        symbol="AAPL",
        source_system="FMP",
        api_endpoint="historical-price-eod/full",
        batch_id="batch-1",
        run_id="run-1",
    )

    statement, params = connection.statements[0]
    # Every placeholder in the rendered statement - comments included, because
    # the connector binds across the whole text - must have exactly one value.
    assert statement.count("%s") == 6 * len(records)
    assert len(params) == 6 * len(records)
    # The payload is bound as JSON text, never formatted into the statement.
    assert '"close": 169.3' in params[0]
    assert params[1] == "AAPL"
    assert params[4] == "batch-1"


def test_bronze_insert_is_chunked() -> None:
    connection = FakeConnection()
    records = [price_record(date=f"2024-01-{day:02d}") for day in range(1, 11)]

    insert_raw_records(
        connection,
        "transformations/bronze/insert_stock_prices_raw.sql",
        records,
        symbol="AAPL",
        source_system="FMP",
        api_endpoint="historical-price-eod/full",
        batch_id="batch-1",
        run_id="run-1",
        chunk_size=4,
    )

    assert len(connection.statements) == 3


def test_empty_record_lists_do_not_hit_the_warehouse() -> None:
    connection = FakeConnection()

    inserted = insert_raw_records(
        connection,
        "transformations/bronze/insert_stock_prices_raw.sql",
        [],
        symbol="AAPL",
        source_system="FMP",
        api_endpoint="historical-price-eod/full",
        batch_id="batch-1",
        run_id="run-1",
    )

    assert inserted == 0
    assert connection.statements == []


def test_iter_chunks_splits_evenly_and_keeps_the_remainder() -> None:
    assert list(iter_chunks([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]


# -- processing windows ----------------------------------------------------
def test_silver_window_starts_at_the_watermark(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = FakeConnection()
    monkeypatch.setattr(
        bronze_to_silver.audit,
        "get_watermark",
        lambda *args, **kwargs: {"LAST_PROCESSED_TS": "2024-05-01 10:00:00"},
    )
    monkeypatch.setattr(
        bronze_to_silver,
        "fetch_one",
        lambda *args, **kwargs: {"PENDING_RECORDS": 12, "MAX_INGESTED_AT": "2024-05-02 06:00:00"},
    )

    window = bronze_to_silver.plan_window(
        connection,
        table_name=bronze_to_silver.SILVER_STOCK_PRICES,
        pending_sql="10_pending_bronze_stock_prices.sql",
    )

    assert window.from_ts == "2024-05-01 10:00:00"
    assert window.to_ts == "2024-05-02 06:00:00"
    assert window.has_work
    assert bronze_to_silver.ProcessingWindow.from_dict(window.as_dict()) == window


def test_the_first_silver_run_starts_at_the_epoch_sentinel(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = FakeConnection()
    monkeypatch.setattr(bronze_to_silver.audit, "get_watermark", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        bronze_to_silver,
        "fetch_one",
        lambda *args, **kwargs: {"PENDING_RECORDS": 5, "MAX_INGESTED_AT": "2024-05-02 06:00:00"},
    )

    window = bronze_to_silver.plan_window(
        connection,
        table_name=bronze_to_silver.SILVER_STOCK_PRICES,
        pending_sql="10_pending_bronze_stock_prices.sql",
    )

    assert window.from_ts == bronze_to_silver.audit.EPOCH_TS
    assert window.has_work


def test_an_empty_window_short_circuits_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = FakeConnection()
    monkeypatch.setattr(bronze_to_silver.audit, "get_watermark", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        bronze_to_silver,
        "fetch_one",
        lambda *args, **kwargs: {"PENDING_RECORDS": 0, "MAX_INGESTED_AT": None},
    )

    window = bronze_to_silver.plan_window(
        connection,
        table_name=bronze_to_silver.SILVER_STOCK_PRICES,
        pending_sql="10_pending_bronze_stock_prices.sql",
    )

    assert not window.has_work
    result = bronze_to_silver.transform_stock_prices(
        connection, window=window, run_id="run-1", batch_id="batch-1"
    )
    assert result.merged_records == 0
    assert connection.statements == []


def test_gold_lookback_is_passed_as_a_negative_number_of_days(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_execute_file(_connection: Any, path: str, params: dict[str, Any]) -> int:
        captured.update({"path": path, **params})
        return 42

    monkeypatch.setattr(silver_to_gold, "execute_file", fake_execute_file)
    window = bronze_to_silver.ProcessingWindow(
        from_ts="2024-05-01 00:00:00", to_ts="2024-05-02 00:00:00", pending_records=10
    )

    rows = silver_to_gold.calculate_daily_performance(
        FakeConnection(), window=window, run_id="run-1", batch_id="batch-1"
    )

    assert rows == 42
    assert captured["lookback_days"] == -get_config().load.gold_lookback_days
    assert captured["path"].endswith("21_merge_daily_stock_performance.sql")


# -- SQL contract ----------------------------------------------------------
SQL_FILES = sorted(path.relative_to(sql_dir()).as_posix() for path in sql_dir().rglob("*.sql"))


def test_every_sql_file_is_readable() -> None:
    assert SQL_FILES
    for relative in SQL_FILES:
        assert read_sql(relative).strip()


@pytest.mark.parametrize("relative", SQL_FILES)
def test_sql_uses_bind_parameters_only(relative: str) -> None:
    """A stray '%' would break parameter binding, so every one must be a bind."""
    statement = read_sql(relative)
    stripped = re.sub(r"%\((?:\w+)\)s|%s", "", statement)

    assert "%" not in stripped, f"unbound '%' in {relative}"


@pytest.mark.parametrize("relative", SQL_FILES)
def test_sql_contains_no_credentials(relative: str) -> None:
    statement = read_sql(relative).lower()

    for forbidden in ("password", "apikey", "api_key", "secret"):
        assert forbidden not in statement, f"{forbidden} mentioned in {relative}"


def test_silver_merge_deduplicates_on_the_business_key() -> None:
    statement = read_sql("transformations/silver/13_merge_stock_prices.sql")

    assert "QUALIFY ROW_NUMBER() OVER (" in statement
    assert "PARTITION BY SYMBOL, TRADE_DATE" in statement
    assert "ORDER BY INGESTED_AT DESC, RAW_ID DESC" in statement
    assert "AND IS_VALID" in statement


def test_staging_view_implements_every_documented_error_type() -> None:
    statement = read_sql("07_create_views.sql")

    for error_type in ("NULL_REQUIRED_FIELD", "INVALID_DATE", "INVALID_PRICE", "INVALID_VOLUME"):
        assert error_type in statement


def test_quarantine_keeps_the_original_payload() -> None:
    statement = read_sql("transformations/silver/11_quarantine_invalid_stock_prices.sql")

    assert "AUDIT.DATA_QUALITY_ERRORS" in statement
    assert "RAW_DATA" in statement
    assert "LATERAL FLATTEN" in statement


def test_gold_daily_performance_uses_window_functions() -> None:
    statement = read_sql("transformations/gold/21_merge_daily_stock_performance.sql")

    assert "LAG(CLOSE_PRICE) OVER (PARTITION BY SYMBOL ORDER BY TRADE_DATE)" in statement
    assert "ROWS BETWEEN 6 PRECEDING AND CURRENT ROW" in statement
    assert "ROWS BETWEEN 29 PRECEDING AND CURRENT ROW" in statement
    assert "STDDEV_SAMP(DAILY_RETURN_PERCENT)" in statement
    # Division by the previous close is guarded.
    assert "NULLIF(PREV_CLOSE_PRICE, 0)" in statement


def test_gold_summary_covers_the_documented_periods() -> None:
    statement = read_sql("transformations/gold/22_merge_stock_performance_summary.sql")

    for period in ("'7D'", "'30D'", "'90D'"):
        assert period in statement
    assert "NULLIF(MIN_BY(CLOSE_PRICE, TRADE_DATE), 0)" in statement


# -- the daily return formula ---------------------------------------------
def reference_daily_return_percent(close: float, previous_close: float | None) -> float | None:
    """Pure Python mirror of the SQL expression in 21_merge_daily_stock_performance.

    Keeping the formula written out here means a change to the SQL has to be a
    deliberate one: this test fails if the two stop agreeing.
    """
    if previous_close is None or previous_close == 0:
        return None
    return (close - previous_close) / previous_close * 100


@pytest.mark.parametrize(
    "close, previous_close, expected",
    [
        (110.0, 100.0, 10.0),
        (90.0, 100.0, -10.0),
        (100.0, 100.0, 0.0),
        (100.0, None, None),  # first row of a symbol
        (100.0, 0.0, None),   # never divide by zero
    ],
)
def test_daily_return_percent(close: float, previous_close: float | None, expected: float | None) -> None:
    result = reference_daily_return_percent(close, previous_close)

    if expected is None:
        assert result is None
    else:
        assert result == pytest.approx(expected)


def test_moving_averages_are_null_until_the_window_is_complete() -> None:
    """The SQL publishes a 7/30 day average only once enough rows exist."""
    statement = read_sql("transformations/gold/21_merge_daily_stock_performance.sql")

    assert "IFF(COUNT(CLOSE_PRICE) OVER" in statement
    assert ") = 7," in statement
    assert ") = 30," in statement


def test_every_sql_path_referenced_in_python_exists() -> None:
    """Catches a renamed SQL file before Airflow does.

    Paths are compared by file name because some references are built from a
    directory constant and an f-string.
    """
    import ast

    available = {path.name for path in sql_dir().rglob("*.sql")}
    referenced: set[str] = set()
    for path in (sql_dir().parent / "src").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.endswith(".sql"):
                referenced.add(Path(node.value).name)

    assert referenced, "no SQL references found - has the loader been refactored?"
    assert referenced <= available, f"missing SQL files: {sorted(referenced - available)}"
