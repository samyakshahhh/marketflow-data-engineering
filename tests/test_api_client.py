"""FMP client: response parsing, error handling and credential hygiene."""

from __future__ import annotations

import json
from datetime import date
from typing import Any

import pytest
import requests

from src.api.fmp_client import (
    FMPAuthenticationError,
    FMPClient,
    FMPRateLimitError,
    FMPResponseError,
    normalise_price_payload,
)
from src.utils.logging_utils import redact
from tests.conftest import price_record


class StubResponse:
    """Just enough of requests.Response for the client under test."""

    def __init__(self, payload: Any = None, status_code: int = 200, text: str | None = None) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = text if text is not None else json.dumps(payload)

    def json(self) -> Any:
        if self._payload is None:
            raise ValueError("no JSON object could be decoded")
        return self._payload


class StubSession:
    """Records the requests the client makes and replays canned responses."""

    def __init__(self, response: StubResponse | Exception) -> None:
        self._response = response
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, params: dict[str, Any], timeout: int) -> StubResponse:
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        if isinstance(self._response, Exception):
            raise self._response
        return self._response

    def close(self) -> None:
        return None


def build_client(response: StubResponse | Exception) -> tuple[FMPClient, StubSession]:
    session = StubSession(response)
    return FMPClient(api_key="test-key", session=session), session  # type: ignore[arg-type]


# -- response normalisation ------------------------------------------------
def test_normalise_accepts_the_stable_list_payload() -> None:
    records = normalise_price_payload([price_record(), price_record(date="2024-05-02")], "aapl")

    assert len(records) == 2
    assert {record["symbol"] for record in records} == {"AAPL"}
    assert records[0]["close"] == 169.3


def test_normalise_accepts_the_legacy_historical_payload() -> None:
    payload = {"symbol": "MSFT", "historical": [{"date": "2024-05-01", "close": 400.0}]}

    records = normalise_price_payload(payload, "MSFT")

    assert records == [{"symbol": "MSFT", "date": "2024-05-01", "close": 400.0}]


def test_normalise_returns_empty_list_for_empty_payloads() -> None:
    assert normalise_price_payload(None, "AAPL") == []
    assert normalise_price_payload([], "AAPL") == []


def test_normalise_raises_on_an_error_payload() -> None:
    with pytest.raises(FMPAuthenticationError):
        normalise_price_payload({"Error Message": "Invalid API KEY."}, "AAPL")


def test_normalise_rejects_unexpected_record_types() -> None:
    with pytest.raises(FMPResponseError):
        normalise_price_payload(["not-a-record"], "AAPL")


# -- requests --------------------------------------------------------------
def test_get_historical_prices_sends_symbol_and_date_range() -> None:
    client, session = build_client(StubResponse([price_record()]))

    records = client.get_historical_prices("aapl", date(2024, 4, 1), date(2024, 5, 1))

    assert len(records) == 1
    call = session.calls[0]
    assert call["url"].endswith("historical-price-eod/full")
    assert call["params"]["symbol"] == "AAPL"
    assert call["params"]["from"] == "2024-04-01"
    assert call["params"]["to"] == "2024-05-01"
    assert call["params"]["apikey"] == "test-key"
    assert call["timeout"] > 0


def test_get_historical_prices_rejects_an_inverted_date_range() -> None:
    client, _ = build_client(StubResponse([]))

    with pytest.raises(ValueError):
        client.get_historical_prices("AAPL", date(2024, 5, 1), date(2024, 4, 1))


def test_missing_api_key_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FMP_API_KEY", raising=False)

    with pytest.raises(FMPAuthenticationError):
        FMPClient()


@pytest.mark.parametrize("status_code", [401, 403])
def test_unauthorised_responses_raise_authentication_errors(status_code: int) -> None:
    client, _ = build_client(StubResponse({}, status_code=status_code))

    with pytest.raises(FMPAuthenticationError):
        client.get_historical_prices("AAPL", date(2024, 4, 1), date(2024, 5, 1))


def test_rate_limited_responses_raise_a_rate_limit_error() -> None:
    client, _ = build_client(StubResponse({}, status_code=429))

    with pytest.raises(FMPRateLimitError):
        client.get_historical_prices("AAPL", date(2024, 4, 1), date(2024, 5, 1))


def test_server_errors_raise_a_response_error() -> None:
    client, _ = build_client(StubResponse(None, status_code=500, text="upstream failure"))

    with pytest.raises(FMPResponseError):
        client.get_historical_prices("AAPL", date(2024, 4, 1), date(2024, 5, 1))


def test_non_json_responses_raise_a_response_error() -> None:
    client, _ = build_client(StubResponse(None, status_code=200, text="<html>"))

    with pytest.raises(FMPResponseError):
        client.get_historical_prices("AAPL", date(2024, 4, 1), date(2024, 5, 1))


def test_transport_errors_are_wrapped_and_do_not_leak_the_api_key() -> None:
    failure = requests.ConnectionError("failed for url: https://fmp.test/prices?apikey=test-key")
    client, _ = build_client(failure)

    with pytest.raises(FMPResponseError) as excinfo:
        client.get_historical_prices("AAPL", date(2024, 4, 1), date(2024, 5, 1))

    assert "test-key" not in str(excinfo.value)


def test_redact_removes_the_api_key_from_urls() -> None:
    assert redact("https://fmp.test/prices?symbol=AAPL&apikey=secret") == (
        "https://fmp.test/prices?symbol=AAPL&apikey=***"
    )


# -- company profile -------------------------------------------------------
def test_company_profile_returns_the_first_record() -> None:
    client, _ = build_client(StubResponse([{"symbol": "AAPL", "companyName": "Apple Inc.", "sector": "Technology"}]))

    profile = client.get_company_profile("aapl")

    assert profile is not None
    assert profile["symbol"] == "AAPL"
    assert profile["companyName"] == "Apple Inc."


def test_company_profile_returns_none_when_the_payload_is_empty() -> None:
    client, _ = build_client(StubResponse([]))

    assert client.get_company_profile("AAPL") is None
