"""Logging helpers.

Airflow configures the root logger, so modules only ask for a named logger and
log structured, greppable messages. Credentials are never logged: the API key
is stripped from any URL before it reaches a log record.
"""

from __future__ import annotations

import logging
import re
from typing import Any

_API_KEY_PATTERN = re.compile(r"(apikey=)[^&\s]+", re.IGNORECASE)

LOGGER_PREFIX = "marketflow"


def get_logger(name: str) -> logging.Logger:
    """Return a logger namespaced under ``marketflow``."""
    suffix = name.split(".")[-1] if name else "pipeline"
    return logging.getLogger(f"{LOGGER_PREFIX}.{suffix}")


def redact(text: str) -> str:
    """Remove an API key from a URL or message before logging it."""
    return _API_KEY_PATTERN.sub(r"\1***", text)


def format_counts(**counts: Any) -> str:
    """Render counters as ``key=value`` pairs for a single log line."""
    return " ".join(f"{key}={value}" for key, value in counts.items() if value is not None)
