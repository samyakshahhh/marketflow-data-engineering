"""Shared pytest fixtures.

The unit tests never touch Snowflake or the FMP API: Snowflake is replaced by a
fake connection that records the statements it was given, and HTTP responses
are stubbed. Tests that do need live credentials are marked ``integration``.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.config import get_config, read_sql  # noqa: E402


@pytest.fixture(autouse=True)
def project_root_env(monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the configuration loader at the repository and reset its caches."""
    monkeypatch.setenv("MARKETFLOW_PROJECT_ROOT", str(PROJECT_ROOT))
    get_config.cache_clear()
    read_sql.cache_clear()
    yield PROJECT_ROOT
    get_config.cache_clear()
    read_sql.cache_clear()


class FakeCursor:
    """Minimal DB-API cursor that records statements and returns canned rows."""

    def __init__(self, recorder: "FakeConnection", rows: list[dict[str, Any]] | None = None) -> None:
        self._recorder = recorder
        self._rows = rows or []
        self.rowcount = len(self._rows)

    def execute(self, sql: str, params: Any = None) -> "FakeCursor":
        self._recorder.statements.append((sql, params))
        self.rowcount = self._recorder.rowcount
        return self

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self._recorder.rows)

    def fetchone(self) -> dict[str, Any] | None:
        return self._recorder.rows[0] if self._recorder.rows else None

    def close(self) -> None:
        return None

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


class FakeConnection:
    """Stand-in for a Snowflake connection."""

    def __init__(self, rows: list[dict[str, Any]] | None = None, rowcount: int = 0) -> None:
        self.rows = rows or []
        self.rowcount = rowcount
        self.statements: list[tuple[str, Any]] = []

    def cursor(self, *_args: Any, **_kwargs: Any) -> FakeCursor:
        return FakeCursor(self, self.rows)

    def close(self) -> None:
        return None

    @property
    def executed_sql(self) -> list[str]:
        return [sql for sql, _ in self.statements]


@pytest.fixture
def fake_connection() -> FakeConnection:
    return FakeConnection()


def price_record(**overrides: Any) -> dict[str, Any]:
    """A well-formed FMP price record, with optional field overrides."""
    record = {
        "symbol": "AAPL",
        "date": "2024-05-01",
        "open": 169.58,
        "high": 172.705,
        "low": 169.11,
        "close": 169.3,
        "volume": 50383147,
        "change": -0.28,
        "changePercent": -0.1651,
        "vwap": 170.17,
    }
    record.update(overrides)
    return record
