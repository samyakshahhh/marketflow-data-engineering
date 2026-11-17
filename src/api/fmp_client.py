"""Financial Modeling Prep (FMP) REST client.

The rest of the pipeline only depends on this module's small surface
(``get_historical_prices`` / ``get_company_profile`` returning plain
dictionaries), so a different market-data provider can be swapped in by writing
another client with the same two methods - no DAG or SQL change required.

The API key is read from the ``FMP_API_KEY`` environment variable and is never
logged or written to Snowflake.
"""

from __future__ import annotations

import os
import time
from datetime import date
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils.config import ApiConfig, PipelineConfig, get_config
from src.utils.logging_utils import get_logger, redact

logger = get_logger(__name__)

API_KEY_ENV_VAR = "FMP_API_KEY"
_RETRYABLE_STATUS = (429, 500, 502, 503, 504)


class FMPError(RuntimeError):
    """Base class for every error raised by the client."""


class FMPAuthenticationError(FMPError):
    """The API key is missing, invalid or not entitled to the endpoint."""


class FMPRateLimitError(FMPError):
    """The API rate limit was exhausted even after retrying."""


class FMPResponseError(FMPError):
    """The API answered with an error payload or an unusable response."""


def normalise_price_payload(payload: Any, symbol: str) -> list[dict[str, Any]]:
    """Return a flat list of price records regardless of the endpoint shape.

    The ``/stable`` endpoint returns a list of records; the legacy ``/api/v3``
    endpoint wraps them in ``{"symbol": ..., "historical": [...]}``. Both are
    accepted so the base URL can be changed in configuration alone.
    """
    if payload is None:
        return []

    if isinstance(payload, dict):
        _raise_for_error_payload(payload)
        records = payload.get("historical")
        if records is None:
            # A single record object is still valid input.
            records = [payload] if payload else []
    elif isinstance(payload, list):
        records = payload
    else:
        raise FMPResponseError(f"unexpected response type for {symbol}: {type(payload).__name__}")

    normalised: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            raise FMPResponseError(f"unexpected record type for {symbol}: {type(record).__name__}")
        # The legacy endpoint omits the symbol inside each record.
        normalised.append({"symbol": symbol.upper(), **record})
    return normalised


def _raise_for_error_payload(payload: dict[str, Any]) -> None:
    """FMP reports problems as a JSON body with HTTP 200 or 4xx."""
    message = payload.get("Error Message") or payload.get("error") or payload.get("message")
    if not message:
        return
    text = str(message)
    lowered = text.lower()
    if "api key" in lowered or "apikey" in lowered:
        raise FMPAuthenticationError(text)
    raise FMPResponseError(text)


class FMPClient:
    """Thin, retrying HTTP client for the endpoints MarketFlow needs."""

    def __init__(
        self,
        api_key: str | None = None,
        config: PipelineConfig | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self._config: ApiConfig = (config or get_config()).api
        self._api_key = api_key or os.getenv(API_KEY_ENV_VAR, "")
        if not self._api_key:
            raise FMPAuthenticationError(
                f"{API_KEY_ENV_VAR} is not set - add it to .env before running the pipeline"
            )
        self._owns_session = session is None
        self._session = session or self._build_session()
        self._last_request_at = 0.0

    # -- session ---------------------------------------------------------
    def _build_session(self) -> requests.Session:
        """Session with connection pooling and backoff on transient failures."""
        retry = Retry(
            total=self._config.max_retries,
            connect=self._config.max_retries,
            read=self._config.max_retries,
            status=self._config.max_retries,
            backoff_factor=self._config.backoff_factor,
            status_forcelist=_RETRYABLE_STATUS,
            allowed_methods=frozenset(["GET"]),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.mount("http://", HTTPAdapter(max_retries=retry))
        return session

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def __enter__(self) -> "FMPClient":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    # -- requests --------------------------------------------------------
    def _throttle(self) -> None:
        """Keep a minimum gap between requests to stay inside the free tier."""
        delay = self._config.request_delay_seconds
        if delay <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        if self._last_request_at and elapsed < delay:
            time.sleep(delay - elapsed)

    def _get(self, endpoint: str, params: dict[str, Any]) -> Any:
        url = f"{self._config.base_url}/{endpoint.lstrip('/')}"
        request_params = {key: value for key, value in params.items() if value is not None}
        request_params["apikey"] = self._api_key

        self._throttle()
        logger.info("GET %s params=%s", url, {k: v for k, v in request_params.items() if k != "apikey"})
        try:
            response = self._session.get(url, params=request_params, timeout=self._config.timeout_seconds)
        except requests.RequestException as exc:
            raise FMPResponseError(f"request to {url} failed: {redact(str(exc))}") from exc
        finally:
            self._last_request_at = time.monotonic()

        if response.status_code in (401, 403):
            raise FMPAuthenticationError(
                f"authentication failed for {endpoint} (HTTP {response.status_code}) - check {API_KEY_ENV_VAR}"
            )
        if response.status_code == 429:
            raise FMPRateLimitError(f"rate limit exhausted for {endpoint} after retries")
        if response.status_code >= 400:
            raise FMPResponseError(
                f"{endpoint} returned HTTP {response.status_code}: {redact(response.text[:300])}"
            )

        try:
            return response.json()
        except ValueError as exc:
            raise FMPResponseError(f"{endpoint} returned a non JSON response") from exc

    # -- endpoints -------------------------------------------------------
    def get_historical_prices(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """Return end-of-day price records for ``symbol`` between two dates.

        One request covers the whole range: the endpoint is not paginated, and
        requesting day by day would waste the request budget.
        """
        if start_date > end_date:
            raise ValueError(f"start_date {start_date} is after end_date {end_date}")

        payload = self._get(
            self._config.historical_prices_endpoint,
            {"symbol": symbol.upper(), "from": start_date.isoformat(), "to": end_date.isoformat()},
        )
        records = normalise_price_payload(payload, symbol)
        logger.info(
            "fetched symbol=%s from=%s to=%s records=%d",
            symbol.upper(),
            start_date,
            end_date,
            len(records),
        )
        return records

    def get_company_profile(self, symbol: str) -> dict[str, Any] | None:
        """Return the company profile for ``symbol``, or None when empty."""
        payload = self._get(self._config.company_profile_endpoint, {"symbol": symbol.upper()})
        if isinstance(payload, dict):
            _raise_for_error_payload(payload)
            payload = [payload]
        if not isinstance(payload, list) or not payload:
            logger.warning("no company profile returned for symbol=%s", symbol.upper())
            return None
        profile = payload[0]
        if not isinstance(profile, dict):
            raise FMPResponseError(f"unexpected profile payload for {symbol}")
        return {"symbol": symbol.upper(), **profile}
