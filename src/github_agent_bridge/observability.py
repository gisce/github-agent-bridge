from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import Any

from .models import utc_now
from .persistence import Database, ObservabilityRepository


DEFAULT_PROCESS_SAMPLE_RETENTION_SECONDS = 24 * 60 * 60
SENTRY_DSN_ENV = "GITHUB_AGENT_BRIDGE_SENTRY_DSN"
SENTRY_ENVIRONMENT_ENV = "GITHUB_AGENT_BRIDGE_SENTRY_ENVIRONMENT"
SENTRY_RELEASE_ENV = "GITHUB_AGENT_BRIDGE_SENTRY_RELEASE"
SENTRY_TRACES_SAMPLE_RATE_ENV = "GITHUB_AGENT_BRIDGE_SENTRY_TRACES_SAMPLE_RATE"
SENTRY_PROFILES_SAMPLE_RATE_ENV = "GITHUB_AGENT_BRIDGE_SENTRY_PROFILES_SAMPLE_RATE"

_SENTRY_INITIALIZED = False
_SENTRY_LAST_RESULT: dict[str, Any] | None = None


def configure_sentry(*, service: str, env: dict[str, str] | None = None) -> dict[str, Any]:
    """Initialize Sentry when configured, without making sentry-sdk mandatory."""
    global _SENTRY_INITIALIZED, _SENTRY_LAST_RESULT

    values = os.environ if env is None else env
    dsn = _first_env(values, SENTRY_DSN_ENV, "SENTRY_DSN")
    if not dsn:
        return {"enabled": False, "reason": "missing_dsn"}
    if _SENTRY_INITIALIZED:
        return _SENTRY_LAST_RESULT or {"enabled": True, "service": service}

    try:
        sentry_sdk = importlib.import_module("sentry_sdk")
    except ImportError:
        _SENTRY_LAST_RESULT = {"enabled": False, "reason": "sentry_sdk_missing"}
        return _SENTRY_LAST_RESULT

    from . import __version__

    release = _first_env(values, SENTRY_RELEASE_ENV, "SENTRY_RELEASE") or f"github-agent-bridge@{__version__}"
    environment = _first_env(values, SENTRY_ENVIRONMENT_ENV, "SENTRY_ENVIRONMENT") or None
    options: dict[str, Any] = {
        "dsn": dsn,
        "release": release,
        "environment": environment,
        "send_default_pii": False,
        # FastAPI/Starlette HTTP exceptions are deliberate responses. Operational
        # failures remain visible through their API response and audit record;
        # reporting them as unhandled errors creates duplicate Sentry incidents.
        "ignore_errors": [
            "fastapi.exceptions.HTTPException",
            "starlette.exceptions.HTTPException",
        ],
    }
    traces_sample_rate = _sample_rate(values, SENTRY_TRACES_SAMPLE_RATE_ENV, "SENTRY_TRACES_SAMPLE_RATE")
    profiles_sample_rate = _sample_rate(values, SENTRY_PROFILES_SAMPLE_RATE_ENV, "SENTRY_PROFILES_SAMPLE_RATE")
    if traces_sample_rate is not None:
        options["traces_sample_rate"] = traces_sample_rate
    if profiles_sample_rate is not None:
        options["profiles_sample_rate"] = profiles_sample_rate

    sentry_sdk.init(**options)
    sentry_sdk.set_tag("service", service)
    sentry_sdk.set_tag("component", "github-agent-bridge")
    _SENTRY_INITIALIZED = True
    _SENTRY_LAST_RESULT = {
        "enabled": True,
        "service": service,
        "release": release,
        "environment": environment,
    }
    return _SENTRY_LAST_RESULT


def record_monitor_observation(
    db: str | Path,
    metrics: dict[str, Any],
    alerts: list[str],
    *,
    process_sample_retention_seconds: int = DEFAULT_PROCESS_SAMPLE_RETENTION_SECONDS,
) -> None:
    path = Path(db).expanduser()
    if not path.exists():
        return
    ObservabilityRepository(Database(path)).record_monitor_observation(
        utc_now(), metrics, alerts, process_sample_retention_seconds
    )


def recent_process_samples(db: str | Path, *, limit: int = 60) -> list[dict[str, Any]]:
    path = Path(db).expanduser()
    if not path.exists():
        return []
    return [
        sample.to_dict()
        for sample in ObservabilityRepository(Database(path)).recent_process_samples(
            _coerce_limit(limit, maximum=500)
        )
    ]


def list_alerts(db: str | Path, *, include_resolved: bool = False, limit: int = 50) -> list[dict[str, Any]]:
    path = Path(db).expanduser()
    if not path.exists():
        return []
    return [
        alert.to_dict()
        for alert in ObservabilityRepository(Database(path)).list_alerts(
            include_resolved=include_resolved,
            limit=_coerce_limit(limit, maximum=200),
        )
    ]


def _coerce_limit(value: int, *, maximum: int) -> int:
    return max(1, min(value, maximum))


def _first_env(values: dict[str, str], *names: str) -> str:
    for name in names:
        value = values.get(name, "").strip()
        if value:
            return value
    return ""


def _sample_rate(values: dict[str, str], *names: str) -> float | None:
    raw = _first_env(values, *names)
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    if value < 0 or value > 1:
        return None
    return value
