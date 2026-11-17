"""Configuration loading for MarketFlow.

Business configuration lives in ``config/*.yaml`` and is read once per process.
Credentials never appear here - they are read from environment variables by the
API client and the Snowflake connection module.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

# Repository root: <root>/src/utils/config.py -> parents[2]
_DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONFIG_DIR_NAME = "config"
SQL_DIR_NAME = "sql"


class ConfigError(RuntimeError):
    """Raised when configuration is missing or malformed."""


def project_root() -> Path:
    """Return the project root.

    Inside the Airflow containers the code is mounted somewhere other than the
    repository layout, so ``MARKETFLOW_PROJECT_ROOT`` wins when it is set.
    """
    env_root = os.getenv("MARKETFLOW_PROJECT_ROOT")
    return Path(env_root) if env_root else _DEFAULT_PROJECT_ROOT


@dataclass(frozen=True)
class ApiConfig:
    """Everything needed to talk to the data provider except the API key."""

    base_url: str
    historical_prices_endpoint: str
    company_profile_endpoint: str
    timeout_seconds: int
    max_retries: int
    backoff_factor: float
    request_delay_seconds: float


@dataclass(frozen=True)
class LoadConfig:
    """Incremental-loading behaviour."""

    initial_lookback_days: int
    incremental_overlap_days: int
    max_days_per_request: int
    gold_lookback_days: int


@dataclass(frozen=True)
class SnowflakeConfig:
    """Target database and schema names."""

    database: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    audit_schema: str


@dataclass(frozen=True)
class PipelineConfig:
    """Fully resolved pipeline configuration."""

    source_system: str
    api: ApiConfig
    load: LoadConfig
    snowflake: SnowflakeConfig
    symbols: tuple[str, ...]


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ConfigError(f"configuration file not found: {path}")
    with path.open(encoding="utf-8") as handle:
        content = yaml.safe_load(handle) or {}
    if not isinstance(content, dict):
        raise ConfigError(f"configuration file must contain a mapping: {path}")
    return content


def _load_symbols(config_dir: Path) -> tuple[str, ...]:
    """Enabled symbols, from MARKETFLOW_SYMBOLS or config/symbols.yaml."""
    override = os.getenv("MARKETFLOW_SYMBOLS")
    if override:
        symbols = [part.strip().upper() for part in override.split(",") if part.strip()]
    else:
        entries = _read_yaml(config_dir / "symbols.yaml").get("symbols") or []
        symbols = [
            str(entry["symbol"]).strip().upper()
            for entry in entries
            if isinstance(entry, dict) and entry.get("symbol") and entry.get("enabled", True)
        ]
    if not symbols:
        raise ConfigError("no enabled symbols configured")
    # Preserve order, drop duplicates.
    return tuple(dict.fromkeys(symbols))


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


@lru_cache(maxsize=1)
def get_config() -> PipelineConfig:
    """Load and cache the pipeline configuration.

    Call ``get_config.cache_clear()`` after changing environment variables
    (tests do this).
    """
    config_dir = project_root() / CONFIG_DIR_NAME
    raw = _read_yaml(config_dir / "pipeline.yaml")

    api_raw = raw.get("api") or {}
    load_raw = raw.get("load") or {}
    snowflake_raw = raw.get("snowflake") or {}

    api = ApiConfig(
        base_url=os.getenv("FMP_BASE_URL", api_raw.get("base_url", "")).rstrip("/"),
        historical_prices_endpoint=api_raw.get("historical_prices_endpoint", ""),
        company_profile_endpoint=api_raw.get("company_profile_endpoint", ""),
        timeout_seconds=int(api_raw.get("timeout_seconds", 30)),
        max_retries=int(api_raw.get("max_retries", 3)),
        backoff_factor=float(api_raw.get("backoff_factor", 2.0)),
        request_delay_seconds=float(api_raw.get("request_delay_seconds", 0.0)),
    )
    if not api.base_url or not api.historical_prices_endpoint:
        raise ConfigError("api.base_url and api.historical_prices_endpoint are required")

    load = LoadConfig(
        initial_lookback_days=_int_env(
            "MARKETFLOW_INITIAL_LOOKBACK_DAYS", int(load_raw.get("initial_lookback_days", 730))
        ),
        incremental_overlap_days=int(load_raw.get("incremental_overlap_days", 3)),
        max_days_per_request=int(load_raw.get("max_days_per_request", 1825)),
        gold_lookback_days=int(load_raw.get("gold_lookback_days", 60)),
    )

    snowflake = SnowflakeConfig(
        database=os.getenv("SNOWFLAKE_DATABASE", snowflake_raw.get("database", "FINANCIAL_MARKET_DB")),
        bronze_schema=snowflake_raw.get("bronze_schema", "BRONZE"),
        silver_schema=snowflake_raw.get("silver_schema", "SILVER"),
        gold_schema=snowflake_raw.get("gold_schema", "GOLD"),
        audit_schema=snowflake_raw.get("audit_schema", "AUDIT"),
    )

    return PipelineConfig(
        source_system=str(raw.get("source_system", "FMP")),
        api=api,
        load=load,
        snowflake=snowflake,
        symbols=_load_symbols(config_dir),
    )


def sql_dir() -> Path:
    """Directory holding every SQL statement used by the pipeline."""
    return project_root() / SQL_DIR_NAME


@lru_cache(maxsize=64)
def read_sql(relative_path: str) -> str:
    """Read a SQL file from the ``sql/`` directory.

    All SQL lives in files so that statements can be reviewed, diffed and run
    by hand in Snowsight; Python only binds parameters to them.
    """
    path = sql_dir() / relative_path
    if not path.is_file():
        raise ConfigError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")
