# Python rules

Derived from `src/`, `scripts/` and `tests/`. Follow the surrounding code
first; these are the patterns it consistently uses.

---

## Module structure

- Module docstring explaining purpose and *why* (design reasoning, not a restatement of the code).
- `from __future__ import annotations` as the first import.
- Imports: stdlib, third-party, then `src.*` (absolute imports from the `src` package).
- `logger = get_logger(__name__)` at module level.
- Module-level constants in `UPPER_SNAKE_CASE` (table names such as
  `SILVER_STOCK_PRICES = "SILVER.STOCK_PRICES"`, SQL directory `_SQL = "transformations/silver"`).

## Style

- Full type hints, modern syntax (`dict[str, Any]`, `str | None`).
- Functions that take a connection use keyword-only parameters after `*`:
  `def plan_window(connection: Any, *, table_name: str, ...)`.
- Value objects are `@dataclass(frozen=True)`; result counters are mutable `@dataclass`.
  Objects crossing XCom expose `as_dict()` and `@classmethod from_dict()`.
- Docstrings on public functions: one line, plus a paragraph when the reason is non-obvious.
- Errors: module-specific exception classes (`ConfigError`, `FMPError` hierarchy,
  `SnowflakeConfigurationError`). Never swallow exceptions except where
  documented (failure callback must not mask the original error).
- No formatter or linter is configured in the repository. Match the existing
  style (≈ 100–110 char lines, double quotes, trailing commas in multi-line calls).
  Do not reformat untouched code.

## Reuse these helpers — do not re-implement

| Need | Use |
| --- | --- |
| Configuration | `src.utils.config.get_config()` (cached; `get_config.cache_clear()` in tests) |
| Project root / SQL dir | `project_root()`, `sql_dir()` |
| Read a SQL file | `read_sql("transformations/...")` |
| Run a SQL file | `src.warehouse.loader.execute_file / fetch_one / fetch_all` |
| Bulk Bronze insert | `src.warehouse.loader.insert_raw_records` |
| Transaction | `src.warehouse.loader.transaction` |
| Snowflake connection | `src.warehouse.connection.snowflake_connection()` |
| Run log / watermarks / quarantine | `src.warehouse.audit.*` |
| Run identity in Airflow | `src.orchestration.common.new_batch_id / run_id_from_context / task_id_from_context` |
| Logging / redaction | `src.utils.logging_utils.get_logger / redact / format_counts` |
| API calls | `src.api.fmp_client.FMPClient` (context manager) |
| Structural API validation | `src.validation.data_quality` |
| Processing window | `src.transformations.bronze_to_silver.ProcessingWindow` |

## Architecture constraints

- SQL lives in `sql/` files, never in Python strings (except trivial
  `BEGIN`/`COMMIT`/`ROLLBACK` in `transaction()`).
- Values are always bound, never formatted into SQL.
- Python decides *which window* to process and in what order; Snowflake does
  the transformation. Do not pull datasets into pandas — pandas is not a dependency.
- The package is `src/warehouse`, not `src/snowflake` (avoids shadowing `snowflake.connector`).
- A new data provider = a new client exposing the same methods as `FMPClient`
  (`get_historical_prices`, `get_company_profile`).
- New runtime dependencies go in `requirements.txt` (resolved against the
  Airflow constraints file) and need approval; test-only tools in `requirements-dev.txt`.

## Tests

- pytest, files `tests/test_<area>.py`, functions named as sentences
  (`test_later_runs_start_just_before_the_watermark`).
- No live Snowflake/API in unit tests: use `FakeConnection` and `price_record()`
  from `tests/conftest.py`, `monkeypatch` for watermarks/`fetch_one`, stub sessions for HTTP.
- Tests needing credentials must be marked `@pytest.mark.integration`.
- Tests needing Airflow use `pytest.importorskip("airflow", ...)`.
