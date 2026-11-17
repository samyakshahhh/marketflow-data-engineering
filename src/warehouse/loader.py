"""Statement execution and Bronze loading helpers.

Every statement comes from a file under ``sql/``; this module only binds
parameters and batches rows. That keeps SQL reviewable in one place and makes
SQL injection impossible by construction - values are always bound, never
formatted into the statement.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any, Iterable, Iterator, Sequence

from src.utils.config import read_sql
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

# Placeholder the Bronze insert templates expand into one group per record.
VALUE_ROWS_PLACEHOLDER = "{value_rows}"

# Rows per INSERT when loading Bronze. Large enough to keep round trips low,
# small enough to stay well inside Snowflake's statement size limit.
DEFAULT_CHUNK_SIZE = 500


def iter_chunks(items: Sequence[Any], size: int) -> Iterator[list[Any]]:
    """Yield ``items`` in lists of at most ``size`` elements."""
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def execute(connection: Any, sql: str, params: dict[str, Any] | Sequence[Any] | None = None) -> int:
    """Run one statement and return the number of affected rows."""
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.rowcount or 0


def execute_file(
    connection: Any,
    relative_path: str,
    params: dict[str, Any] | None = None,
) -> int:
    """Run a statement stored under ``sql/`` and return affected rows."""
    rows = execute(connection, read_sql(relative_path), params)
    logger.debug("executed %s rows=%s", relative_path, rows)
    return rows


def fetch_one(
    connection: Any,
    relative_path: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Run a query from ``sql/`` and return its first row as a dict."""
    rows = fetch_all(connection, relative_path, params)
    return rows[0] if rows else None


def fetch_all(
    connection: Any,
    relative_path: str,
    params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Run a query from ``sql/`` and return every row as a dict."""
    from snowflake.connector import DictCursor

    with connection.cursor(DictCursor) as cursor:
        cursor.execute(read_sql(relative_path), params)
        return list(cursor.fetchall())


@contextmanager
def transaction(connection: Any) -> Iterator[Any]:
    """Run a block of statements as one Snowflake transaction."""
    execute(connection, "BEGIN")
    try:
        yield connection
    except Exception:
        execute(connection, "ROLLBACK")
        raise
    execute(connection, "COMMIT")


def insert_raw_records(
    connection: Any,
    template_path: str,
    records: Iterable[dict[str, Any]],
    *,
    symbol: str,
    source_system: str,
    api_endpoint: str,
    batch_id: str,
    run_id: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> int:
    """Append raw API records to a Bronze table.

    The payload is stored as JSON text and parsed by Snowflake into a VARIANT,
    so Bronze keeps the provider's structure exactly as it was received.
    """
    rows = list(records)
    if not rows:
        return 0

    template = read_sql(template_path)
    inserted = 0
    for chunk in iter_chunks(rows, chunk_size):
        value_rows = ", ".join(["(%s, %s, %s, %s, %s, %s)"] * len(chunk))
        statement = template.replace(VALUE_ROWS_PLACEHOLDER, value_rows)
        params: list[Any] = []
        for record in chunk:
            params.extend(
                [
                    json.dumps(record, default=str),
                    str(record.get("symbol") or symbol).upper(),
                    source_system,
                    api_endpoint,
                    batch_id,
                    run_id,
                ]
            )
        inserted += execute(connection, statement, params)

    logger.info("loaded symbol=%s rows=%d batch_id=%s", symbol.upper(), inserted, batch_id)
    return inserted


def run_script(connection: Any, sql_text: str) -> None:
    """Execute a multi-statement DDL script (used by scripts/setup_snowflake.py)."""
    for cursor in connection.execute_string(sql_text):
        cursor.close()
