"""Configuration loading."""

from __future__ import annotations

import pytest

from src.utils.config import ConfigError, get_config, read_sql, sql_dir


def test_pipeline_config_loads_expected_values() -> None:
    config = get_config()

    assert config.source_system == "FMP"
    assert config.api.base_url.startswith("https://")
    assert config.api.historical_prices_endpoint
    assert config.api.timeout_seconds > 0
    assert config.api.max_retries >= 1
    assert config.load.initial_lookback_days >= 365
    assert config.load.gold_lookback_days >= 30
    assert config.snowflake.database == "FINANCIAL_MARKET_DB"


def test_symbols_come_from_configuration_and_are_unique() -> None:
    symbols = get_config().symbols

    assert "AAPL" in symbols
    assert len(symbols) == len(set(symbols))
    assert all(symbol == symbol.upper() for symbol in symbols)


def test_symbols_can_be_overridden_by_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MARKETFLOW_SYMBOLS", "tsla, nvda ,tsla")
    get_config.cache_clear()

    assert get_config().symbols == ("TSLA", "NVDA")


def test_initial_lookback_can_be_overridden_by_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MARKETFLOW_INITIAL_LOOKBACK_DAYS", "90")
    get_config.cache_clear()

    assert get_config().load.initial_lookback_days == 90


def test_invalid_numeric_override_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MARKETFLOW_INITIAL_LOOKBACK_DAYS", "two years")
    get_config.cache_clear()

    with pytest.raises(ConfigError):
        get_config()


def test_read_sql_returns_file_contents_and_rejects_missing_files() -> None:
    statement = read_sql("transformations/silver/13_merge_stock_prices.sql")

    assert "MERGE INTO SILVER.STOCK_PRICES" in statement
    assert (sql_dir() / "01_create_database.sql").is_file()

    with pytest.raises(ConfigError):
        read_sql("transformations/silver/does_not_exist.sql")
