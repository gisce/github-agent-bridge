from __future__ import annotations

import argparse
import asyncio
import base64
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
import json
import os
import shutil
import secrets
import shlex
import sqlite3
import subprocess
import sys
from tempfile import TemporaryDirectory
import threading
import urllib.error
import urllib.parse
import urllib.request
import re
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from . import __version__
from .autoupdate import apply_update_plan, complete_pending_reload, load_update_state, plan_update, record_update_plan, save_update_state
from .cancellation import cancel_running_job
from .cli import DEFAULT_DB
from .feedback import (
    approve_proposal,
    delete_rule,
    list_applicable_rules,
    list_events,
    list_proposals,
    list_repositories,
    list_rules,
    reject_proposal,
    update_rule_scope,
)
from .dashboard_data import (
    get_job_detail,
    inspect_db_read_only,
    job_logs,
    job_session,
    job_session_events,
    job_session_transcript,
    list_all_job_actor_logins,
    list_job_actors,
    list_jobs,
    metrics_summary,
    transcript_entry_from_session_event,
)
from .monitor import monitor
from .mcp import MCPServer, authenticate_token, create_token, list_tokens, revoke_token, update_token_owner
from .observability import configure_sentry, list_alerts, recent_process_samples
from .queue import JobQueue
from .policy import Policy
from .systemd_status import allowed_unit_names, stream_journal_lines, systemd_status
from .web_push import delete_subscription, save_subscription, subscription_status
from .webhook import persist_shadow_delivery, verify_signature, webhook_notification


DEFAULT_HOST = os.getenv("GITHUB_AGENT_BRIDGE_DASHBOARD_HOST", "127.0.0.1")
DEFAULT_PORT = int(os.getenv("GITHUB_AGENT_BRIDGE_DASHBOARD_PORT", "8765"))
SESSION_COOKIE = "gab_dashboard_session"
OAUTH_STATE_COOKIE = "gab_dashboard_oauth_state"
GITHUB_AUTHORIZE_URL = "https://github.com/login/oauth/authorize"
GITHUB_TOKEN_URL = "https://github.com/login/oauth/access_token"
GITHUB_USER_URL = "https://api.github.com/user"
GITHUB_TEAMS_URL = "https://api.github.com/user/teams"
PROJECT_REPOSITORY_URL = "https://github.com/gisce/github-agent-bridge"
SESSION_VERSION = 1
WEBHOOK_TIMESERIES_MAX_DAYS = 366
WEBHOOK_TIMESERIES_MAX_HOURLY_DAYS = 31
WEBHOOK_COVERAGE_EVENT_GLOBS = (
    "issue_comment:created:*",
    "pull_request_review_comment:created:*",
    "pull_request_review:created:*",
    "commit_comment:created:*",
    "workflow_run:workflow_run_failed:*",
)


def _expand_systemd_home_specifier(value: str) -> str:
    if value == "%h":
        return str(Path.home())
    if value.startswith("%h/"):
        return str(Path.home() / value[3:])
    return value


def _knowledge_actor(item: dict[str, Any]) -> str:
    actor = item.get("trigger_actor") or (item.get("actor") if item.get("actor") != "github" else "")
    return str(actor or "").strip().lower()


def _knowledge_item_owned_by(item: dict[str, Any], login: str) -> bool:
    user = str(login or "").strip().lower()
    if not user:
        return False
    actor = _knowledge_actor(item)
    if actor == user:
        return True
    source_event = item.get("source_event")
    if isinstance(source_event, dict) and _knowledge_item_owned_by(source_event, user):
        return True
    source_events = item.get("source_event_details")
    if isinstance(source_events, list):
        return any(isinstance(event, dict) and _knowledge_item_owned_by(event, user) for event in source_events)
    return False


def _mark_manageable_knowledge(items: list[dict[str, Any]], profile: dict[str, Any]) -> list[dict[str, Any]]:
    if profile.get("is_admin"):
        return [{**item, "can_manage": True} for item in items]
    login = str(profile.get("login") or "")
    return [{**item, "can_manage": _knowledge_item_owned_by(item, login)} for item in items]


def _parse_webhook_datetime(value: str, parameter: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"invalid_{parameter}_datetime",
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _webhook_datetime_value(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _webhook_coverage_predicate(column: str) -> str:
    return "(" + " OR ".join(f"{column} GLOB '{pattern}'" for pattern in WEBHOOK_COVERAGE_EVENT_GLOBS) + ")"


def _webhook_coverage_window(
    con: sqlite3.Connection,
    *,
    retention_days: int,
    grace_seconds: int,
) -> tuple[str, str]:
    end = datetime.now(UTC) - timedelta(seconds=grace_seconds)
    start = end - timedelta(days=retention_days)
    first = con.execute("SELECT MIN(created_at) FROM webhook_shadow_receipts").fetchone()[0]
    if first:
        start = max(start, _parse_webhook_datetime(str(first), "webhook_created_at"))
    if start > end:
        start = end
    return _webhook_datetime_value(start), _webhook_datetime_value(end)


def _encode_webhook_delivery_cursor(created_at: str, delivery_id: str) -> str:
    payload = json.dumps([created_at, delivery_id], separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_webhook_delivery_cursor(cursor: str) -> tuple[str, str]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invalid_cursor") from exc
    if not (
        isinstance(payload, list)
        and len(payload) == 2
        and all(isinstance(value, str) and value for value in payload)
    ):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invalid_cursor")
    return payload[0], payload[1]


def _webhook_admin_url(target_type: str, target: str, hook_id: str) -> str | None:
    if target_type == "organization" and target and target != "unknown":
        return f"https://github.com/organizations/{urllib.parse.quote(target, safe='')}/settings/hooks/{urllib.parse.quote(hook_id, safe='')}"
    if target_type == "repository" and "/" in target:
        owner, repository = target.split("/", 1)
        return (
            f"https://github.com/{urllib.parse.quote(owner, safe='')}/"
            f"{urllib.parse.quote(repository, safe='')}/settings/hooks/{urllib.parse.quote(hook_id, safe='')}"
        )
    return None


def _webhook_hook_status(row: sqlite3.Row) -> str:
    if not row["active"]:
        return "inactive"
    if row["last_event_at"]:
        return "receiving"
    if row["last_ping_at"]:
        return "quiet"
    return "never_seen"


def _webhook_hook_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["hook_id"],
        "target": row["target"],
        "target_type": row["target_type"],
        "name": row["name"],
        "active": bool(row["active"]),
        "events": json.loads(row["events_json"] or "[]"),
        "content_type": row["content_type"],
        "ssl_verify": None if row["insecure_ssl"] is None else not bool(row["insecure_ssl"]),
        "delivery_url": row["delivery_url"],
        "github_api_url": row["github_api_url"],
        "ping_url": row["ping_url"],
        "deliveries_url": row["deliveries_url"],
        "github_created_at": row["github_created_at"],
        "github_updated_at": row["github_updated_at"],
        "last_ping_at": row["last_ping_at"],
        "last_event_at": row["last_event_at"],
        "last_delivery_id": row["last_delivery_id"],
        "last_event_name": row["last_event_name"],
        "last_action": row["last_action"],
        "last_repository": row["last_repository"],
        "last_result": row["last_result"],
        "status": _webhook_hook_status(row),
        "admin_url": _webhook_admin_url(row["target_type"], row["target"], row["hook_id"]),
    }


def _webhook_delivery_payload(row: sqlite3.Row) -> dict[str, Any]:
    hook = None
    if row["hook_id"]:
        hook = {
            "id": row["hook_id"],
            "target": row["hook_target"],
            "target_type": row["hook_target_type"],
            "admin_url": (
                _webhook_admin_url(row["hook_target_type"], row["hook_target"], row["hook_id"])
                if row["hook_target"] and row["hook_target_type"] else None
            ),
        }
    return {
        "delivery_id": row["delivery_id"],
        "hook_id": row["hook_id"],
        "hook": hook,
        "event_name": row["event_name"],
        "action": row["action"],
        "event_key": row["event_key"],
        "repository": row["repository"],
        "status": row["status"],
        "enqueue_status": row["enqueue_status"],
        "job_id": row["job_id"],
        "duplicate_count": row["duplicate_count"],
        "created_at": row["created_at"],
    }


def _webhook_ping_endpoint(row: sqlite3.Row) -> str:
    hook_id = str(row["hook_id"])
    target = str(row["target"])
    if row["target_type"] == "organization":
        path = f"/orgs/{urllib.parse.quote(target, safe='')}/hooks/{urllib.parse.quote(hook_id, safe='')}/pings"
    elif row["target_type"] == "repository" and target.count("/") == 1:
        owner, repository = target.split("/", 1)
        path = (
            f"/repos/{urllib.parse.quote(owner, safe='')}/{urllib.parse.quote(repository, safe='')}"
            f"/hooks/{urllib.parse.quote(hook_id, safe='')}/pings"
        )
    else:
        raise ValueError("webhook_ping_target_invalid")
    parsed = urllib.parse.urlparse(str(row["ping_url"] or ""))
    if (
        parsed.scheme != "https"
        or parsed.netloc != "api.github.com"
        or parsed.path != path
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("webhook_ping_url_invalid")
    return path


def _request_webhook_ping(endpoint: str, *, gh_bin: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [gh_bin, "api", "--method", "POST", endpoint],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=15,
    )


class DashboardConfig:
    def __init__(
        self,
        *,
        db: str | Path = DEFAULT_DB,
        secret_key: str | None = None,
        oauth_client_id: str | None = None,
        oauth_client_secret: str | None = None,
        allowed_users: set[str] | None = None,
        allowed_orgs: set[str] | None = None,
        allowed_teams: set[str] | None = None,
        admin_users: set[str] | None = None,
        admin_teams: set[str] | None = None,
        require_auth: bool = True,
        static_dir: str | Path | None = None,
        public_url: str | None = None,
        web_push_public_key: str | None = None,
        webhook_secrets: tuple[str, ...] | None = None,
        webhook_secrets_by_owner: dict[str, tuple[str, ...]] | None = None,
        webhook_max_bytes: int | None = None,
        webhook_retention_days: int | None = None,
        webhook_coverage_grace_seconds: int | None = None,
        webhook_mode: str | None = None,
        webhook_policy: str | Path | None = None,
        webhook_primary_ack: bool | None = None,
    ) -> None:
        self.db = Path(db).expanduser()
        self.secret_key = secret_key or os.getenv("GITHUB_AGENT_BRIDGE_DASHBOARD_SECRET_KEY", "")
        self.oauth_client_id = oauth_client_id or os.getenv("GITHUB_OAUTH_CLIENT_ID", "")
        self.oauth_client_secret = oauth_client_secret or os.getenv("GITHUB_OAUTH_CLIENT_SECRET", "")
        self.allowed_users = allowed_users if allowed_users is not None else _csv_env("GITHUB_AGENT_BRIDGE_DASHBOARD_ALLOWED_USERS")
        self.allowed_orgs = allowed_orgs if allowed_orgs is not None else _csv_env("GITHUB_AGENT_BRIDGE_DASHBOARD_ALLOWED_ORGS")
        self.allowed_teams = allowed_teams if allowed_teams is not None else _csv_env("GITHUB_AGENT_BRIDGE_DASHBOARD_ALLOWED_TEAMS")
        self.admin_users = admin_users if admin_users is not None else _csv_env("GITHUB_AGENT_BRIDGE_DASHBOARD_ADMIN_USERS")
        self.admin_teams = admin_teams if admin_teams is not None else _csv_env("GITHUB_AGENT_BRIDGE_DASHBOARD_ADMIN_TEAMS")
        self.require_auth = require_auth
        self.static_dir = Path(static_dir or os.getenv("GITHUB_AGENT_BRIDGE_DASHBOARD_STATIC_DIR", Path(__file__).with_name("dashboard_static"))).expanduser()
        self.public_url = (public_url if public_url is not None else os.getenv("GITHUB_AGENT_BRIDGE_DASHBOARD_PUBLIC_URL", "")).rstrip("/")
        self.web_push_public_key = web_push_public_key if web_push_public_key is not None else os.getenv("GITHUB_AGENT_BRIDGE_WEB_PUSH_VAPID_PUBLIC_KEY", "")
        self.webhook_secrets = webhook_secrets if webhook_secrets is not None else tuple(
            value for value in (
                os.getenv("GITHUB_AGENT_BRIDGE_WEBHOOK_SECRET", ""),
                os.getenv("GITHUB_AGENT_BRIDGE_WEBHOOK_PREVIOUS_SECRET", ""),
            ) if value
        )
        self.webhook_secrets_by_owner = webhook_secrets_by_owner if webhook_secrets_by_owner is not None else _webhook_secrets_by_owner_env()
        self.webhook_max_bytes = webhook_max_bytes or int(os.getenv("GITHUB_AGENT_BRIDGE_WEBHOOK_MAX_BYTES", "1048576"))
        self.webhook_retention_days = webhook_retention_days or int(os.getenv("GITHUB_AGENT_BRIDGE_WEBHOOK_RETENTION_DAYS", "30"))
        self.webhook_coverage_grace_seconds = max(
            0,
            (
                webhook_coverage_grace_seconds
                if webhook_coverage_grace_seconds is not None
                else int(os.getenv("GITHUB_AGENT_BRIDGE_WEBHOOK_COVERAGE_GRACE_SECONDS", "600"))
            ),
        )
        self.webhook_mode = (webhook_mode or os.getenv("GITHUB_AGENT_BRIDGE_WEBHOOK_MODE", "shadow")).lower()
        if self.webhook_mode not in {"shadow", "canary", "primary"}:
            raise ValueError("GITHUB_AGENT_BRIDGE_WEBHOOK_MODE must be shadow, canary, or primary")
        policy_value = _expand_systemd_home_specifier(
            str(webhook_policy or os.getenv("GITHUB_AGENT_BRIDGE_WEBHOOK_POLICY", ""))
        )
        self.webhook_policy = Path(policy_value).expanduser() if policy_value else None
        if self.webhook_mode in {"canary", "primary"} and self.webhook_policy is None:
            raise ValueError("GITHUB_AGENT_BRIDGE_WEBHOOK_POLICY is required in canary or primary mode")
        self.webhook_primary_ack = (
            webhook_primary_ack
            if webhook_primary_ack is not None
            else os.getenv("GITHUB_AGENT_BRIDGE_WEBHOOK_PRIMARY_ACK", "").lower() in {"1", "true", "yes"}
        )
        if self.webhook_mode == "primary" and not self.webhook_primary_ack:
            raise ValueError("primary webhook mode requires GITHUB_AGENT_BRIDGE_WEBHOOK_PRIMARY_ACK=true")

    @property
    def oauth_ready(self) -> bool:
        return bool(self.secret_key and self.oauth_client_id and self.oauth_client_secret)

    @property
    def has_authorization_policy(self) -> bool:
        return bool(self.allowed_users or self.allowed_orgs or self.allowed_teams or self.admin_users or self.admin_teams)

    @property
    def has_admin_policy(self) -> bool:
        return bool(self.admin_users or self.admin_teams)


def _csv_env(name: str) -> set[str]:
    raw = os.getenv(name, "")
    return {part.strip().lower() for part in raw.split(",") if part.strip()}


def _webhook_secrets_by_owner_env() -> dict[str, tuple[str, ...]]:
    raw = os.getenv("GITHUB_AGENT_BRIDGE_WEBHOOK_SECRETS_BY_OWNER", "")
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("GITHUB_AGENT_BRIDGE_WEBHOOK_SECRETS_BY_OWNER must be valid JSON") from exc
    if not isinstance(parsed, dict):
        raise ValueError("GITHUB_AGENT_BRIDGE_WEBHOOK_SECRETS_BY_OWNER must be a JSON object")
    result: dict[str, tuple[str, ...]] = {}
    for owner, values in parsed.items():
        if not isinstance(owner, str) or not owner.strip() or not isinstance(values, list):
            raise ValueError("webhook owner secrets must map owner names to JSON arrays")
        owner_secrets = tuple(value for value in values if isinstance(value, str) and value)
        if not owner_secrets or len(owner_secrets) != len(values):
            raise ValueError("each webhook owner must have one or more non-empty string secrets")
        result[owner.strip().lower()] = owner_secrets
    return result


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def _autoupdate_systemd_units() -> dict[str, str]:
    return {
        "executor": _env("GITHUB_AGENT_BRIDGE_EXECUTOR_UNIT", "github-agent-bridge.service"),
        "dashboard": _env("GITHUB_AGENT_BRIDGE_DASHBOARD_UNIT", "github-agent-bridge-dashboard.service"),
        "webhook": _env("GITHUB_AGENT_BRIDGE_WEBHOOK_UNIT", "github-agent-bridge-webhook.service"),
        "reader": _env("GITHUB_AGENT_BRIDGE_READER_TIMER_UNIT", "github-agent-bridge-reader.timer"),
        "monitor": _env("GITHUB_AGENT_BRIDGE_MONITOR_TIMER_UNIT", "github-agent-bridge-monitor.timer"),
        "feedback": _env("GITHUB_AGENT_BRIDGE_FEEDBACK_TIMER_UNIT", "github-agent-bridge-feedback.timer"),
    }


def _dashboard_autoupdate_plan(db: str | Path) -> dict[str, Any]:
    return plan_update(
        db,
        repo=_env("GITHUB_AGENT_BRIDGE_AUTOUPDATE_REPO", "gisce/github-agent-bridge"),
        repo_dir=_env("GITHUB_AGENT_BRIDGE_AUTOUPDATE_REPO_DIR", "."),
        target_tag=_env("GITHUB_AGENT_BRIDGE_AUTOUPDATE_TARGET_TAG") or None,
        gh_bin=_env("GITHUB_AGENT_BRIDGE_GH_BIN", "gh"),
        systemd_units=_autoupdate_systemd_units(),
    )


def _dashboard_apply_autoupdate(plan: dict[str, Any], db: str | Path) -> dict[str, Any]:
    install_command = _env("GITHUB_AGENT_BRIDGE_AUTOUPDATE_INSTALL_COMMAND")
    return apply_update_plan(
        plan,
        db=db,
        repo=_env("GITHUB_AGENT_BRIDGE_AUTOUPDATE_REPO", "gisce/github-agent-bridge"),
        backup_dir=_env("GITHUB_AGENT_BRIDGE_AUTOUPDATE_BACKUP_DIR") or None,
        install_command=shlex.split(install_command) if install_command else None,
        systemctl_bin=_env("GITHUB_AGENT_BRIDGE_SYSTEMCTL_BIN", "systemctl"),
    )


def _record_dashboard_autoupdate_plan(db: str | Path, plan: dict[str, Any], *, applied: bool) -> dict[str, Any]:
    state = record_update_plan(db, plan)
    if applied:
        state["dashboard_applied_at"] = state["updated_at"]
    else:
        state.pop("dashboard_applied_at", None)
        state["executor_reload_pending"] = False
    save_update_state(JobQueue(db), state)
    return state


def _redacted_headers() -> dict[str, str]:
    return {"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"}


def _snapshot_dashboard_static(static_dir: Path) -> tuple[Path, TemporaryDirectory | None]:
    if not static_dir.is_dir():
        return static_dir, None
    snapshot = TemporaryDirectory(prefix="github-agent-bridge-dashboard-")
    runtime_static_dir = Path(snapshot.name) / "dashboard_static"
    shutil.copytree(static_dir, runtime_static_dir)
    return runtime_static_dir, snapshot


def _dashboard_public_url(request: Request) -> str:
    url, _ = _dashboard_public_url_with_source(request)
    return url


def _dashboard_public_url_with_source(request: Request) -> tuple[str, str]:
    cfg: DashboardConfig = request.app.state.dashboard_config
    if cfg.public_url:
        return cfg.public_url, "configured"

    forwarded_host = request.headers.get("x-forwarded-host", "").split(",", 1)[0].strip()
    host = forwarded_host or request.headers.get("host", "").strip()
    if not host:
        return str(request.base_url).rstrip("/"), "request"

    forwarded_proto = request.headers.get("x-forwarded-proto", "").split(",", 1)[0].strip()
    scheme = forwarded_proto or request.url.scheme
    forwarded_prefix = request.headers.get("x-forwarded-prefix", "").split(",", 1)[0].strip().rstrip("/")
    source = "forwarded" if forwarded_host or forwarded_proto or forwarded_prefix else "request"
    return f"{scheme}://{host}{forwarded_prefix}", source


def _sse_headers() -> dict[str, str]:
    return {
        **_redacted_headers(),
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }


def _sse_event(event: str, data: dict[str, Any], *, event_id: int | None = None) -> str:
    prefix = f"id: {event_id}\n" if event_id is not None else ""
    return f"{prefix}event: {event}\ndata: {json.dumps(data, separators=(',', ':'))}\n\n"


def _transcript_sse_key(entry: dict[str, Any]) -> str:
    return json.dumps(entry, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _bearer_token(request: Request) -> str:
    authorization = request.headers.get("authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="mcp_token_required")
    return token.strip()


async def _sleep_or_shutdown(shutdown_event: asyncio.Event | None, sleep_seconds: float) -> bool:
    if shutdown_event is None:
        await asyncio.sleep(sleep_seconds)
        return False
    if shutdown_event.is_set():
        return True
    try:
        await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_seconds)
    except asyncio.TimeoutError:
        return False
    return True


async def _session_stream_events(db: str | Path, job_id: int, *, after_id: int | None = None, sleep_seconds: float = 2.0, shutdown_event: asyncio.Event | None = None):
    last_id = after_id or 0
    sent_transcript_keys: set[str] = set()
    while shutdown_event is None or not shutdown_event.is_set():
        emitted = False
        events = job_session_events(db, job_id, after_id=last_id, limit=100)
        for event in events:
            if shutdown_event is not None and shutdown_event.is_set():
                return
            last_id = int(event["id"])
            emitted = True
            yield _sse_event("session_event", event, event_id=last_id)
            entry = transcript_entry_from_session_event(event)
            if entry is not None:
                key = _transcript_sse_key(entry)
                if key not in sent_transcript_keys:
                    sent_transcript_keys.add(key)
                    yield _sse_event("transcript_entry", {"job_id": job_id, "entry": entry})
        transcript = job_session_transcript(db, job_id, limit=500)
        for entry in transcript:
            if shutdown_event is not None and shutdown_event.is_set():
                return
            key = _transcript_sse_key(entry)
            if key in sent_transcript_keys:
                continue
            sent_transcript_keys.add(key)
            emitted = True
            yield _sse_event("transcript_entry", {"job_id": job_id, "entry": entry})
        if not emitted:
            yield _sse_event("session_heartbeat", {"job_id": job_id, "last_event_id": last_id})
        if await _sleep_or_shutdown(shutdown_event, sleep_seconds):
            return


async def _journal_stream_events(unit: str, *, shutdown_event: asyncio.Event | None = None):
    try:
        stream = stream_journal_lines(unit)
        line_task: asyncio.Task | None = None
        shutdown_task: asyncio.Task | None = None
        try:
            while shutdown_event is None or not shutdown_event.is_set():
                line_task = asyncio.create_task(anext(stream))
                if shutdown_event is None:
                    try:
                        line = await line_task
                    except StopAsyncIteration:
                        return
                else:
                    shutdown_task = asyncio.create_task(shutdown_event.wait())
                    done, pending = await asyncio.wait({line_task, shutdown_task}, return_when=asyncio.FIRST_COMPLETED)
                    for task in pending:
                        task.cancel()
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)
                    if shutdown_task in done:
                        line_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await line_task
                        return
                    try:
                        line = line_task.result()
                    except StopAsyncIteration:
                        return
                yield _sse_event("journal_line", {"unit": unit, "line": line})
        finally:
            pending_tasks = [task for task in (line_task, shutdown_task) if task is not None and not task.done()]
            for task in pending_tasks:
                task.cancel()
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
            await stream.aclose()
    except FileNotFoundError:
        yield _sse_event("journal_error", {"unit": unit, "error": "journalctl_not_found"})


def _sign(config: DashboardConfig, value: str) -> str:
    import hmac
    import hashlib

    digest = hmac.new(config.secret_key.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{value}.{digest}"


def _unsign(config: DashboardConfig, value: str) -> str | None:
    import hmac

    try:
        raw, digest = value.rsplit(".", 1)
    except ValueError:
        return None
    expected = _sign(config, raw).rsplit(".", 1)[1]
    return raw if hmac.compare_digest(digest, expected) else None


def _encode_session(user: dict[str, Any], *, is_admin: bool = False) -> str:
    payload = {
        "v": SESSION_VERSION,
        "login": str(user.get("login", "")).lower(),
        "avatar_url": str(user.get("avatar_url", "")),
        "html_url": str(user.get("html_url", "")),
        "is_admin": bool(is_admin),
    }
    data = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _decode_session(value: str) -> dict[str, Any] | None:
    try:
        padded = value + "=" * (-len(value) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
    except (ValueError, TypeError, json.JSONDecodeError):
        return _profile_from_login(value) if value else None
    login = str(payload.get("login", "")).lower()
    if not login:
        return None
    fallback = _profile_from_login(login)
    return {
        "login": login,
        "avatar_url": str(payload.get("avatar_url") or fallback["avatar_url"]),
        "html_url": str(payload.get("html_url") or fallback["html_url"]),
        "is_admin": bool(payload.get("is_admin", False)),
    }


def _profile_from_login(login: str) -> dict[str, Any]:
    user = str(login).lower()
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,37}[a-z0-9])?", user):
        return {"login": user, "avatar_url": "", "html_url": "", "is_admin": False}
    return {
        "login": user,
        "avatar_url": f"https://github.com/{user}.png?size=80",
        "html_url": f"https://github.com/{user}",
        "is_admin": False,
    }


def _known_mcp_user_profiles(config: DashboardConfig, *, current_login: str = "") -> list[dict[str, Any]]:
    logins = {login.lower() for login in config.allowed_users | config.admin_users if login}
    if current_login:
        logins.add(current_login.lower())
    for actor_login in list_all_job_actor_logins(config.db):
        login = str(actor_login).strip().lower()
        if login:
            logins.add(login)
    for token in list_tokens(config.db, include_revoked=True):
        login = str(token.get("user_login") or "").strip().lower()
        if login:
            logins.add(login)
    return sorted((_profile_from_login(login) for login in logins), key=lambda item: item["login"])


def _require_known_mcp_owner(config: DashboardConfig, owner: str, *, current_login: str) -> str:
    clean_owner = owner.strip().lower().lstrip("@")
    known = {profile["login"] for profile in _known_mcp_user_profiles(config, current_login=current_login)}
    if clean_owner not in known:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="mcp_token_owner_unknown")
    return clean_owner


def _github_json(url: str, token: str) -> Any:
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json", "Authorization": f"Bearer {token}", "User-Agent": "github-agent-bridge-dashboard"})
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def _team_key(team: dict[str, Any]) -> str | None:
    org = team.get("organization")
    if not isinstance(org, dict):
        return None
    org_login = str(org.get("login", "")).lower()
    slug = str(team.get("slug", "")).lower()
    if not org_login or not slug:
        return None
    return f"{org_login}/{slug}"


def _exchange_code(config: DashboardConfig, code: str) -> str:
    data = urllib.parse.urlencode({
        "client_id": config.oauth_client_id,
        "client_secret": config.oauth_client_secret,
        "code": code,
    }).encode("utf-8")
    req = urllib.request.Request(GITHUB_TOKEN_URL, data=data, headers={"Accept": "application/json", "User-Agent": "github-agent-bridge-dashboard"})
    with urllib.request.urlopen(req, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    token = payload.get("access_token")
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="oauth_token_exchange_failed")
    return str(token)


def _is_allowed(config: DashboardConfig, login: str, token: str | None = None) -> bool:
    user = login.lower()
    if _is_admin(config, login, token):
        return True
    if config.allowed_users and user in config.allowed_users:
        return True
    if config.allowed_orgs and token:
        try:
            orgs = _github_json("https://api.github.com/user/orgs", token)
        except (urllib.error.URLError, TimeoutError):
            return False
        if any(str(org.get("login", "")).lower() in config.allowed_orgs for org in orgs if isinstance(org, dict)):
            return True
    if config.allowed_teams and token:
        try:
            teams = _github_json(GITHUB_TEAMS_URL, token)
        except (urllib.error.URLError, TimeoutError):
            return False
        return any(key in config.allowed_teams for key in (_team_key(team) for team in teams if isinstance(team, dict)) if key)
    return not config.has_authorization_policy


def _is_admin(config: DashboardConfig, login: str, token: str | None = None) -> bool:
    user = login.lower()
    if config.admin_users and user in config.admin_users:
        return True
    if config.admin_teams and token:
        try:
            teams = _github_json(GITHUB_TEAMS_URL, token)
        except (urllib.error.URLError, TimeoutError):
            return False
        return any(key in config.admin_teams for key in (_team_key(team) for team in teams if isinstance(team, dict)) if key)
    return False


def _webhook_schema_initializer(config: DashboardConfig):
    lock = threading.Lock()
    ready = False

    def ensure() -> None:
        nonlocal ready
        if ready:
            return
        with lock:
            if not ready:
                JobQueue(config.db)
                ready = True

    return ensure


async def _receive_github_webhook(
    request: Request,
    config: DashboardConfig,
    ensure_webhook_schema,
) -> dict[str, Any]:
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
        raise HTTPException(status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, detail="application_json_required")
    delivery_id = request.headers.get("x-github-delivery", "").strip()
    event_name = request.headers.get("x-github-event", "").strip()
    hook_id = request.headers.get("x-github-hook-id", "").strip() or None
    signature = request.headers.get("x-hub-signature-256", "").strip()
    if not delivery_id or not event_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="github_headers_required")
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > config.webhook_max_bytes:
                raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="payload_too_large")
        except ValueError:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invalid_content_length")
    raw_payload = await request.body()
    if len(raw_payload) > config.webhook_max_bytes:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="payload_too_large")
    try:
        payload = json.loads(raw_payload)
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invalid_json")
    repository = payload.get("repository") if isinstance(payload, dict) else None
    full_name = repository.get("full_name") if isinstance(repository, dict) else None
    owner = str(full_name or "").partition("/")[0].lower()
    if config.webhook_secrets_by_owner:
        webhook_secrets = config.webhook_secrets_by_owner.get(owner, ())
        if not webhook_secrets:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="repository_owner_not_allowed")
    else:
        webhook_secrets = config.webhook_secrets
    if not verify_signature(raw_payload, signature, webhook_secrets):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid_signature")
    ensure_webhook_schema()
    enqueue_status = None
    job_id = None
    if config.webhook_mode in {"canary", "primary"}:
        policy = Policy.from_file(config.webhook_policy)
        repo = str(full_name or "").lower()
        sender = payload.get("sender") if isinstance(payload.get("sender"), dict) else {}
        sender_login = str(sender.get("login") or "").lower()
        if config.webhook_mode == "canary" and repo not in policy.webhook_canary_repos:
            enqueue_status = "outside_canary"
        elif sender_login in policy.bot_logins:
            enqueue_status = "ignored_bot"
        else:
            notification = webhook_notification(
                event_name,
                delivery_id,
                payload,
                bot_logins=policy.bot_logins,
            )
            if notification is None:
                enqueue_status = "ignored"
            else:
                job, enqueue_status = JobQueue(config.db).ingest(
                    notification, policy, source="webhook", source_key=delivery_id,
                )
                job_id = job.id if job else None
    receipt = persist_shadow_delivery(
        config.db,
        delivery_id=delivery_id,
        event_name=event_name,
        raw_payload=raw_payload,
        hook_id=hook_id,
        retention_days=config.webhook_retention_days,
        enqueue_status=enqueue_status,
        job_id=job_id,
    )
    response = {
        "mode": config.webhook_mode,
        "status": receipt.status,
        "event_key": receipt.event_key,
    }
    if config.webhook_mode != "shadow":
        response.update({"enqueue_status": enqueue_status, "job_id": job_id})
    return response


def create_webhook_app(config: DashboardConfig | None = None) -> FastAPI:
    configure_sentry(service="webhook-ingress")
    config = config or DashboardConfig()
    ensure_webhook_schema = _webhook_schema_initializer(config)
    app = FastAPI(title="GitHub Agent Bridge Webhook Ingress")
    app.state.dashboard_config = config

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "service": "github-agent-bridge-webhook-ingress"}

    @app.post("/api/webhooks/github")
    async def github_webhook(request: Request) -> dict[str, Any]:
        return await _receive_github_webhook(request, config, ensure_webhook_schema)

    return app


def create_app(config: DashboardConfig | None = None) -> FastAPI:
    configure_sentry(service="dashboard")
    config = config or DashboardConfig()
    runtime_static_dir, static_snapshot = _snapshot_dashboard_static(config.static_dir)
    shutdown_event = asyncio.Event()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.dashboard_shutdown_event = shutdown_event
        try:
            yield
        finally:
            shutdown_event.set()

    app = FastAPI(title="GitHub Agent Bridge Dashboard API", lifespan=lifespan)
    app.state.dashboard_config = config
    app.state.dashboard_static_snapshot = static_snapshot
    app.state.dashboard_shutdown_event = shutdown_event
    ensure_webhook_schema = _webhook_schema_initializer(config)

    assets_dir = runtime_static_dir / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="dashboard-assets")

    async def current_user(request: Request) -> str:
        profile = await current_profile(request)
        return str(profile["login"])

    async def current_admin_profile(request: Request) -> dict[str, Any]:
        profile = await current_profile(request)
        if not profile.get("is_admin"):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="admin_required")
        return profile

    async def current_profile(request: Request) -> dict[str, Any]:
        cfg: DashboardConfig = request.app.state.dashboard_config
        if not cfg.require_auth:
            return {"login": "test", "avatar_url": "", "html_url": "", "is_admin": True}
        signed = request.cookies.get(SESSION_COOKIE)
        if not signed or not cfg.secret_key:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="not_authenticated")
        raw = _unsign(cfg, signed)
        profile = _decode_session(raw) if raw else None
        if not profile:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="not_authorized")
        return profile

    def can_cancel_job(job_id: int, profile: dict[str, Any]) -> bool:
        if profile.get("is_admin"):
            return True
        login = str(profile.get("login") or "").strip().lower()
        if not login:
            return False
        job = JobQueue(config.db).get(job_id)
        if job is None:
            return False
        actors = [job.trigger_actor, *JobQueue(config.db).coalesced_trigger_actors(job_id)]
        return login in {str(actor or "").strip().lstrip("@").lower() for actor in actors if actor}

    async def require_dashboard_profile_or_login(request: Request) -> RedirectResponse | None:
        try:
            await current_profile(request)
        except HTTPException as exc:
            if exc.status_code == status.HTTP_401_UNAUTHORIZED and config.oauth_ready:
                return RedirectResponse("/auth/login", status_code=status.HTTP_302_FOUND)
            raise
        return None

    @app.exception_handler(sqlite3.OperationalError)
    async def database_unavailable(_: Request, exc: sqlite3.OperationalError) -> JSONResponse:
        return JSONResponse({"error": "database_unavailable", "detail": str(exc)}, status_code=status.HTTP_503_SERVICE_UNAVAILABLE, headers=_redacted_headers())

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        metrics = inspect_db_read_only(config.db)
        return {
            "ok": bool(metrics.get("db_exists") and metrics.get("schema_ok", True)),
            "service": "github-agent-bridge-dashboard",
            "db_exists": bool(metrics.get("db_exists")),
            "schema_ok": bool(metrics.get("schema_ok", True)),
            "oauth_configured": config.oauth_ready,
            "read_only": False,
        }

    @app.post("/api/webhooks/github")
    async def github_webhook_shadow(request: Request) -> dict[str, Any]:
        return await _receive_github_webhook(request, config, ensure_webhook_schema)

    @app.get("/api/webhooks/github/status")
    @app.get("/api/webhooks/github/summary")
    def github_webhook_shadow_summary(_: dict[str, Any] = Depends(current_admin_profile)) -> dict[str, Any]:
        ensure_webhook_schema()
        with sqlite3.connect(config.db) as con:
            con.row_factory = sqlite3.Row
            window_start, window_end = _webhook_coverage_window(
                con,
                retention_days=config.webhook_retention_days,
                grace_seconds=config.webhook_coverage_grace_seconds,
            )
            receipt = con.execute(
                "SELECT COALESCE(SUM(CASE WHEN status='observed' THEN 1 ELSE 0 END),0) observed,"
                "COALESCE(SUM(CASE WHEN status='unsupported' THEN 1 ELSE 0 END),0) unsupported,"
                "COALESCE(SUM(duplicate_count),0) duplicate_deliveries FROM webhook_shadow_receipts"
            ).fetchone()
            coverage = con.execute(
                "WITH email_events AS ("
                " SELECT event_key,MIN(created_at) created_at FROM ingest_receipts"
                f" WHERE source='email' AND julianday(created_at)>=julianday(?) AND julianday(created_at)<=julianday(?) AND {_webhook_coverage_predicate('event_key')}"
                " GROUP BY event_key"
                "), webhook_events AS ("
                " SELECT event_key,MIN(created_at) created_at FROM webhook_shadow_receipts"
                f" WHERE julianday(created_at)>=julianday(?) AND julianday(created_at)<=julianday(?) AND {_webhook_coverage_predicate('event_key')}"
                " GROUP BY event_key"
                ") SELECT"
                " (SELECT COUNT(*) FROM email_events) imap_events,"
                " (SELECT COUNT(*) FROM webhook_events) webhook_events,"
                " (SELECT COUNT(*) FROM email_events JOIN webhook_events USING(event_key)) both_events,"
                " (SELECT AVG(ABS((julianday(w.created_at)-julianday(i.created_at))*86400000.0))"
                "  FROM email_events i JOIN webhook_events w USING(event_key)) match_delay_ms",
                (window_start, window_end, window_start, window_end),
            ).fetchone()
            inventory = con.execute(
                "SELECT (SELECT COUNT(*) FROM webhook_hooks) hooks,"
                "(SELECT COUNT(*) FROM webhook_shadow_receipts) deliveries"
            ).fetchone()
            enqueue = {
                str(row["enqueue_status"]): int(row["count"])
                for row in con.execute(
                    "SELECT enqueue_status,COUNT(*) count FROM webhook_shadow_receipts "
                    "WHERE enqueue_status IS NOT NULL GROUP BY enqueue_status"
                )
            }
        counts = {
            name: count
            for name, count in (("observed", receipt["observed"]), ("unsupported", receipt["unsupported"]))
            if count
        }
        return {
            "mode": config.webhook_mode,
            "configured": bool(config.webhook_secrets or config.webhook_secrets_by_owner),
            "receipts": counts,
            "duplicate_deliveries": receipt["duplicate_deliveries"],
            "cross_source_matches": coverage["both_events"],
            "enqueue": enqueue,
            "totals": {"hooks": inventory["hooks"], "deliveries": inventory["deliveries"]},
            "coverage": {
                "both": coverage["both_events"],
                "imap_only": max(coverage["imap_events"] - coverage["both_events"], 0),
                "webhook_only": max(coverage["webhook_events"] - coverage["both_events"], 0),
                "imap_eligible": coverage["imap_events"],
                "ratio": (
                    coverage["both_events"] / coverage["imap_events"]
                    if coverage["imap_events"] else None
                ),
                "mean_match_delay_ms": (
                    round(coverage["match_delay_ms"], 1)
                    if coverage["match_delay_ms"] is not None else None
                ),
                "window_start": window_start,
                "window_end": window_end,
                "grace_seconds": config.webhook_coverage_grace_seconds,
            },
        }

    @app.get("/api/webhooks/github/exceptions")
    def github_webhook_shadow_exceptions(
        limit: int = Query(50, ge=1, le=100),
        _: dict[str, Any] = Depends(current_admin_profile),
    ) -> dict[str, Any]:
        """Return bounded, actionable shadow divergences without loading all receipts."""
        ensure_webhook_schema()
        with sqlite3.connect(config.db) as con:
            con.row_factory = sqlite3.Row
            window_start, window_end = _webhook_coverage_window(
                con,
                retention_days=config.webhook_retention_days,
                grace_seconds=config.webhook_coverage_grace_seconds,
            )
            rows = con.execute(
                "WITH email_events AS ("
                " SELECT event_key,source_key,created_at FROM ingest_receipts"
                f" WHERE source='email' AND julianday(created_at)>=julianday(?) AND julianday(created_at)<=julianday(?) AND {_webhook_coverage_predicate('event_key')}"
                "), webhook_events AS ("
                " SELECT event_key,delivery_id,created_at,repository FROM webhook_shadow_receipts"
                f" WHERE julianday(created_at)>=julianday(?) AND julianday(created_at)<=julianday(?) AND {_webhook_coverage_predicate('event_key')}"
                "), candidates AS ("
                " SELECT 'imap_only' kind,i.event_key,i.source_key reference,i.created_at,NULL repository "
                " FROM email_events i WHERE NOT EXISTS ("
                "  SELECT 1 FROM webhook_events w WHERE w.event_key=i.event_key"
                " ) UNION ALL "
                " SELECT 'webhook_only',w.event_key,w.delivery_id,w.created_at,w.repository "
                " FROM webhook_events w WHERE NOT EXISTS ("
                "  SELECT 1 FROM email_events i WHERE i.event_key=w.event_key"
                " ) UNION ALL "
                " SELECT 'unmatchable',NULL,w.delivery_id,w.created_at,w.repository "
                " FROM webhook_shadow_receipts w WHERE w.event_key IS NULL"
                " AND julianday(w.created_at)>=julianday(?) AND julianday(w.created_at)<=julianday(?)"
                ") SELECT kind,event_key,reference,created_at,repository FROM candidates "
                "ORDER BY created_at DESC LIMIT ?",
                (
                    window_start, window_end, window_start, window_end,
                    window_start, window_end, limit,
                ),
            ).fetchall()
        return {
            "exceptions": [dict(row) for row in rows],
            "window_start": window_start,
            "window_end": window_end,
            "grace_seconds": config.webhook_coverage_grace_seconds,
        }

    @app.get("/api/webhooks/github/timeseries")
    def github_webhook_shadow_timeseries(
        from_value: str | None = Query(None, alias="from"),
        to_value: str | None = Query(None, alias="to"),
        bucket: str = Query("day", pattern="^(hour|day)$"),
        _: dict[str, Any] = Depends(current_admin_profile),
    ) -> dict[str, Any]:
        ensure_webhook_schema()
        end = _parse_webhook_datetime(to_value, "to") if to_value else datetime.now(UTC)
        default_days = min(config.webhook_retention_days, WEBHOOK_TIMESERIES_MAX_DAYS)
        start = _parse_webhook_datetime(from_value, "from") if from_value else end - timedelta(days=default_days)
        maximum = WEBHOOK_TIMESERIES_MAX_HOURLY_DAYS if bucket == "hour" else WEBHOOK_TIMESERIES_MAX_DAYS
        if start >= end:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invalid_time_range")
        if end - start > timedelta(days=maximum):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="time_range_too_large")
        start_value = _webhook_datetime_value(start)
        end_value = _webhook_datetime_value(end)
        bucket_expression = "substr(created_at,1,13) || ':00:00Z'" if bucket == "hour" else "substr(created_at,1,10)"
        with sqlite3.connect(config.db) as con:
            rows = con.execute(
                f"SELECT {bucket_expression}, "
                "SUM(CASE WHEN status='observed' THEN 1 ELSE 0 END), "
                "SUM(duplicate_count), SUM(CASE WHEN status='unsupported' THEN 1 ELSE 0 END) "
                "FROM webhook_shadow_receipts WHERE created_at>=? AND created_at<? "
                "GROUP BY 1 ORDER BY 1",
                (start_value, end_value),
            ).fetchall()
        return {
            "from": start_value,
            "to": end_value,
            "bucket": bucket,
            "points": [
                {"bucket": row[0], "observed": row[1], "duplicate": row[2], "unsupported": row[3]}
                for row in rows
            ],
        }

    @app.get("/api/webhooks/github/hooks")
    def github_webhook_shadow_hooks(
        limit: int = Query(50, ge=1, le=100),
        cursor: str | None = Query(None),
        _: dict[str, Any] = Depends(current_admin_profile),
    ) -> dict[str, Any]:
        ensure_webhook_schema()
        where = ""
        parameters: list[Any] = []
        if cursor:
            cursor_updated_at, cursor_hook_id = _decode_webhook_delivery_cursor(cursor)
            where = "WHERE updated_at<? OR (updated_at=? AND hook_id<?)"
            parameters.extend((cursor_updated_at, cursor_updated_at, cursor_hook_id))
        parameters.append(limit + 1)
        with sqlite3.connect(config.db) as con:
            con.row_factory = sqlite3.Row
            hook_rows = con.execute(
                "SELECT hook_id,target,target_type,name,active,events_json,content_type,insecure_ssl,delivery_url,"
                "github_api_url,ping_url,deliveries_url,github_created_at,github_updated_at,last_ping_at,last_event_at,"
                "last_delivery_id,last_event_name,last_action,last_repository,last_result,updated_at "
                f"FROM webhook_hooks {where} ORDER BY updated_at DESC,hook_id DESC LIMIT ?",
                parameters,
            ).fetchall()
        page = hook_rows[:limit]
        next_cursor = None
        if len(hook_rows) > limit and page:
            next_cursor = _encode_webhook_delivery_cursor(page[-1]["updated_at"], page[-1]["hook_id"])
        return {
            "hooks": [_webhook_hook_payload(row) for row in page],
            "next_cursor": next_cursor,
        }

    @app.get("/api/webhooks/github/hooks/{hook_id}")
    def github_webhook_shadow_hook_detail(
        hook_id: str,
        _: dict[str, Any] = Depends(current_admin_profile),
    ) -> dict[str, Any]:
        ensure_webhook_schema()
        with sqlite3.connect(config.db) as con:
            con.row_factory = sqlite3.Row
            hook = con.execute(
                "SELECT hook_id,target,target_type,name,active,events_json,content_type,insecure_ssl,delivery_url,"
                "github_api_url,ping_url,deliveries_url,github_created_at,github_updated_at,last_ping_at,last_event_at,"
                "last_delivery_id,last_event_name,last_action,last_repository,last_result,updated_at "
                "FROM webhook_hooks WHERE hook_id=?",
                (hook_id,),
            ).fetchone()
            if hook is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="webhook_hook_not_found")
            stats = con.execute(
                "SELECT COUNT(*) deliveries,COALESCE(SUM(duplicate_count),0) duplicates,"
                "SUM(CASE WHEN status='unsupported' THEN 1 ELSE 0 END) unsupported "
                "FROM webhook_shadow_receipts WHERE hook_id=?",
                (hook_id,),
            ).fetchone()
            recent = con.execute(
                "SELECT r.delivery_id,r.hook_id,r.event_name,r.action,r.event_key,r.repository,r.status,"
                "r.enqueue_status,r.job_id,"
                "r.duplicate_count,r.created_at,h.target hook_target,h.target_type hook_target_type "
                "FROM webhook_shadow_receipts r LEFT JOIN webhook_hooks h ON h.hook_id=r.hook_id "
                "WHERE r.hook_id=? ORDER BY r.created_at DESC,r.delivery_id DESC LIMIT 20",
                (hook_id,),
            ).fetchall()
            recent_actions = con.execute(
                "SELECT id,action,actor,status,detail,created_at,completed_at "
                "FROM webhook_hook_actions WHERE hook_id=? ORDER BY created_at DESC,id DESC LIMIT 10",
                (hook_id,),
            ).fetchall()
        return {
            "hook": _webhook_hook_payload(hook),
            "stats": {
                "deliveries": stats["deliveries"],
                "duplicates": stats["duplicates"],
                "unsupported": stats["unsupported"] or 0,
            },
            "recent_deliveries": [_webhook_delivery_payload(row) for row in recent],
            "recent_actions": [dict(row) for row in recent_actions],
        }

    @app.post("/api/webhooks/github/hooks/{hook_id}/ping")
    def github_webhook_hook_ping(
        hook_id: str,
        profile: dict[str, Any] = Depends(current_admin_profile),
    ) -> dict[str, Any]:
        ensure_webhook_schema()
        created_at = datetime.now(UTC).isoformat()
        with sqlite3.connect(config.db) as con:
            con.row_factory = sqlite3.Row
            hook = con.execute(
                "SELECT hook_id,target,target_type,ping_url FROM webhook_hooks WHERE hook_id=?",
                (hook_id,),
            ).fetchone()
            if hook is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="webhook_hook_not_found")
            try:
                endpoint = _webhook_ping_endpoint(hook)
            except ValueError as exc:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
            cursor = con.execute(
                "INSERT INTO webhook_hook_actions(hook_id,action,actor,status,created_at) VALUES(?,?,?,?,?)",
                (hook_id, "ping", str(profile["login"]), "requested", created_at),
            )
            action_id = int(cursor.lastrowid)
            con.commit()

        def finish(action_status: str, detail: str) -> None:
            with sqlite3.connect(config.db) as con:
                con.execute(
                    "UPDATE webhook_hook_actions SET status=?,detail=?,completed_at=? WHERE id=?",
                    (action_status, detail[:1000], datetime.now(UTC).isoformat(), action_id),
                )
                con.commit()

        try:
            result = _request_webhook_ping(endpoint, gh_bin=_env("GITHUB_AGENT_BRIDGE_GH_BIN", "gh"))
        except FileNotFoundError as exc:
            finish("failed", "gh executable not found")
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="github_cli_unavailable") from exc
        except subprocess.TimeoutExpired as exc:
            finish("failed", "GitHub ping request timed out")
            raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail="github_webhook_ping_timeout") from exc
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or f"gh exited {result.returncode}"
            finish("failed", detail)
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="github_webhook_ping_failed")
        finish("succeeded", "GitHub accepted the ping request")
        return {
            "action_id": action_id,
            "hook_id": hook_id,
            "status": "succeeded",
            "detail": "GitHub accepted the ping request; configuration will refresh when delivery arrives.",
        }

    @app.get("/api/webhooks/github/deliveries")
    def github_webhook_shadow_deliveries(
        limit: int = Query(50, ge=1, le=100),
        cursor: str | None = Query(None),
        hook_id: str | None = Query(None),
        event_name: str | None = Query(None),
        repository: str | None = Query(None),
        result: str | None = Query(None),
        enqueue_status: str | None = Query(None),
        _: dict[str, Any] = Depends(current_admin_profile),
    ) -> dict[str, Any]:
        ensure_webhook_schema()
        clauses: list[str] = []
        parameters: list[Any] = []
        if cursor:
            cursor_created_at, cursor_delivery_id = _decode_webhook_delivery_cursor(cursor)
            clauses.append("(r.created_at<? OR (r.created_at=? AND r.delivery_id<?))")
            parameters.extend((cursor_created_at, cursor_created_at, cursor_delivery_id))
        for column, value in (
            ("r.hook_id", hook_id),
            ("r.event_name", event_name),
            ("r.repository", repository),
            ("r.status", result),
            ("r.enqueue_status", enqueue_status),
        ):
            if value:
                clauses.append(f"{column}=?")
                parameters.append(value)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        parameters.append(limit + 1)
        with sqlite3.connect(config.db) as con:
            con.row_factory = sqlite3.Row
            delivery_rows = con.execute(
                "SELECT r.delivery_id,r.hook_id,r.event_name,r.action,r.event_key,r.repository,r.status,"
                "r.enqueue_status,r.job_id,"
                "r.duplicate_count,r.created_at,h.target hook_target,h.target_type hook_target_type "
                f"FROM webhook_shadow_receipts r LEFT JOIN webhook_hooks h ON h.hook_id=r.hook_id {where} "
                "ORDER BY r.created_at DESC,r.delivery_id DESC LIMIT ?",
                parameters,
            ).fetchall()
        page = delivery_rows[:limit]
        next_cursor = None
        if len(delivery_rows) > limit and page:
            next_cursor = _encode_webhook_delivery_cursor(page[-1]["created_at"], page[-1]["delivery_id"])
        return {
            "deliveries": [_webhook_delivery_payload(row) for row in page],
            "next_cursor": next_cursor,
        }

    @app.get("/api/webhooks/github/deliveries/{delivery_id}")
    def github_webhook_delivery_detail(
        delivery_id: str,
        _: dict[str, Any] = Depends(current_admin_profile),
    ) -> dict[str, Any]:
        ensure_webhook_schema()
        with sqlite3.connect(config.db) as con:
            con.row_factory = sqlite3.Row
            row = con.execute(
                "SELECT r.delivery_id,r.hook_id,r.event_name,r.action,r.event_key,r.repository,r.status,"
                "r.enqueue_status,r.duplicate_count,r.created_at,r.payload_hash,r.payload_json,"
                "h.target hook_target,h.target_type hook_target_type,"
                "j.id job_id,j.work_key job_work_key,j.status job_status,j.action job_action,"
                "j.decision job_decision,j.work_intent job_work_intent,j.updated_at job_updated_at "
                "FROM webhook_shadow_receipts r "
                "LEFT JOIN webhook_hooks h ON h.hook_id=r.hook_id "
                "LEFT JOIN ingest_receipts i ON i.source='webhook' AND i.source_key=r.delivery_id "
                "LEFT JOIN jobs j ON j.id=COALESCE(r.job_id,i.job_id) WHERE r.delivery_id=?",
                (delivery_id,),
            ).fetchone()
        if row is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="webhook_delivery_not_found")
        payload = json.loads(row["payload_json"]) if row["payload_json"] else None
        job = None
        if row["job_id"] is not None:
            job = {
                "id": row["job_id"], "work_key": row["job_work_key"], "status": row["job_status"],
                "action": row["job_action"], "decision": row["job_decision"],
                "work_intent": row["job_work_intent"], "updated_at": row["job_updated_at"],
            }
        return {
            "delivery": _webhook_delivery_payload(row),
            "payload_hash": row["payload_hash"],
            "payload": payload,
            "job": job,
        }

    def dashboard_index() -> FileResponse:
        index = runtime_static_dir / "index.html"
        if not index.exists():
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="dashboard_ui_not_built")
        return FileResponse(index, headers=_redacted_headers())

    @app.get("/")
    async def dashboard(request: Request) -> Response:
        redirect = await require_dashboard_profile_or_login(request)
        if redirect is not None:
            return redirect
        return dashboard_index()

    @app.get("/service-worker.js")
    async def service_worker(request: Request) -> Response:
        redirect = await require_dashboard_profile_or_login(request)
        if redirect is not None:
            return redirect
        worker = runtime_static_dir / "service-worker.js"
        if not worker.exists():
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="dashboard_service_worker_not_built")
        return FileResponse(worker, headers={**_redacted_headers(), "Service-Worker-Allowed": "/"})

    @app.get("/jobs/{job_path:path}")
    async def dashboard_job(job_path: str, request: Request) -> Response:
        redirect = await require_dashboard_profile_or_login(request)
        if redirect is not None:
            return redirect
        return dashboard_index()

    @app.get("/knowledge/{knowledge_path:path}")
    async def dashboard_knowledge(knowledge_path: str, request: Request) -> Response:
        redirect = await require_dashboard_profile_or_login(request)
        if redirect is not None:
            return redirect
        return dashboard_index()

    @app.get("/mcp")
    @app.get("/mcp/{mcp_path:path}")
    async def dashboard_mcp(request: Request, mcp_path: str = "") -> Response:
        redirect = await require_dashboard_profile_or_login(request)
        if redirect is not None:
            return redirect
        return dashboard_index()

    @app.get("/system/{system_path:path}")
    async def dashboard_system(system_path: str, request: Request) -> Response:
        redirect = await require_dashboard_profile_or_login(request)
        if redirect is not None:
            return redirect
        return dashboard_index()

    @app.get("/webhooks")
    @app.get("/webhooks/{webhook_path:path}")
    async def dashboard_webhooks(request: Request, webhook_path: str = "") -> Response:
        redirect = await require_dashboard_profile_or_login(request)
        if redirect is not None:
            return redirect
        return dashboard_index()

    @app.get("/api/status")
    def api_status(request: Request, profile: dict[str, Any] = Depends(current_profile)) -> dict[str, Any]:
        metrics = inspect_db_read_only(config.db)
        dashboard_url, dashboard_url_source = _dashboard_public_url_with_source(request)
        admin_actions = [
            "retry_job",
            "dismiss_job",
            "cancel_job",
            "approve_knowledge_proposal",
            "reject_knowledge_proposal",
            "update_knowledge_rule_scope",
            "delete_knowledge_rule",
            "create_mcp_token",
            "revoke_mcp_token",
            "ping_webhook",
        ]
        if profile.get("is_admin"):
            admin_actions.extend(["view_autoupdate_plan", "refresh_autoupdate_plan", "apply_autoupdate", "complete_autoupdate_reload", "pause_executor", "resume_executor"])
        return {
            "service": "github-agent-bridge-dashboard",
            "read_only": False,
            "dashboard_url": dashboard_url,
            "dashboard_url_source": dashboard_url_source,
            "admin_actions": admin_actions,
            "webhook_configured": bool(config.webhook_secrets or config.webhook_secrets_by_owner) if profile.get("is_admin") else False,
            "metrics": metrics,
            "autoupdate": load_update_state(JobQueue(config.db)) if profile.get("is_admin") else {},
            "executor_pause": metrics.get("executor_pause", {"paused": False}),
        }

    @app.post("/api/executor/pause")
    def api_executor_pause(profile: dict[str, Any] = Depends(current_admin_profile)) -> dict[str, Any]:
        queue = JobQueue(config.db)
        queue.pause_executor(f"dashboard:{profile['login']}")
        return {"executor_pause": queue.executor_pause_state()}

    @app.post("/api/executor/resume")
    def api_executor_resume(_: dict[str, Any] = Depends(current_admin_profile)) -> dict[str, Any]:
        queue = JobQueue(config.db)
        queue.resume_executor()
        return {"executor_pause": queue.executor_pause_state()}

    @app.post("/api/autoupdate/refresh")
    def api_autoupdate_refresh(_: dict[str, Any] = Depends(current_admin_profile)) -> dict[str, Any]:
        plan = _dashboard_autoupdate_plan(config.db)
        state = _record_dashboard_autoupdate_plan(config.db, plan, applied=False)
        return {"plan": plan, "state": state}

    @app.post("/api/autoupdate/apply")
    def api_autoupdate_apply(_: dict[str, Any] = Depends(current_admin_profile)) -> dict[str, Any]:
        plan = _dashboard_autoupdate_plan(config.db)
        execution = _dashboard_apply_autoupdate(plan, config.db)
        state = _record_dashboard_autoupdate_plan(
            config.db,
            plan,
            applied=bool(execution.get("applied") and not execution.get("blocked")),
        )
        payload = {"plan": plan, "execution": execution, "state": state}
        if execution.get("blocked") or not execution.get("applied"):
            return JSONResponse(payload, status_code=status.HTTP_409_CONFLICT)
        return payload

    @app.post("/api/autoupdate/complete-pending")
    def api_autoupdate_complete_pending(_: dict[str, Any] = Depends(current_admin_profile)) -> dict[str, Any]:
        state = load_update_state(JobQueue(config.db))
        if not state.get("dashboard_applied_at"):
            payload = {
                "completion": {
                    "completed": False,
                    "blocked": ["autoupdate_not_applied"],
                    "commands": [],
                    "state": state,
                },
                "state": state,
            }
            return JSONResponse(payload, status_code=status.HTTP_409_CONFLICT)
        completion = complete_pending_reload(
            config.db,
            systemctl_bin=_env("GITHUB_AGENT_BRIDGE_SYSTEMCTL_BIN", "systemctl"),
        )
        payload = {"completion": completion, "state": load_update_state(JobQueue(config.db))}
        if completion.get("blocked") or not completion.get("completed"):
            return JSONResponse(payload, status_code=status.HTTP_409_CONFLICT)
        return payload

    @app.get("/api/about")
    def api_about(_: str = Depends(current_user)) -> dict[str, Any]:
        return {
            "service": "github-agent-bridge-dashboard",
            "version": __version__,
            "repository_url": PROJECT_REPOSITORY_URL,
        }

    @app.get("/api/me")
    def api_me(profile: dict[str, Any] = Depends(current_profile)) -> dict[str, Any]:
        return {"user": profile}

    @app.get("/api/web-push/config")
    def api_web_push_config(profile: dict[str, Any] = Depends(current_profile)) -> dict[str, Any]:
        return {
            "public_key": config.web_push_public_key,
            "configured": bool(config.web_push_public_key),
            "status": subscription_status(config.db, str(profile["login"])),
        }

    @app.post("/api/web-push/subscriptions")
    async def api_web_push_subscribe(request: Request, profile: dict[str, Any] = Depends(current_profile)) -> dict[str, Any]:
        if not config.web_push_public_key:
            return JSONResponse(
                {"detail": "web_push_not_configured"},
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        try:
            payload = await request.json()
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invalid_json") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="subscription_required")
        try:
            subscription = save_subscription(config.db, str(profile["login"]), payload)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return {"subscription": subscription, "status": subscription_status(config.db, str(profile["login"]))}

    @app.delete("/api/web-push/subscriptions")
    async def api_web_push_unsubscribe(request: Request, profile: dict[str, Any] = Depends(current_profile)) -> dict[str, Any]:
        try:
            payload = await request.json()
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invalid_json") from exc
        endpoint = str(payload.get("endpoint") or "") if isinstance(payload, dict) else ""
        if not endpoint:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="subscription_endpoint_required")
        removed = delete_subscription(config.db, str(profile["login"]), endpoint)
        return {"removed": removed, "status": subscription_status(config.db, str(profile["login"]))}

    @app.get("/api/jobs")
    def api_jobs(
        _: str = Depends(current_user),
        status_filter: str | None = Query(default=None, alias="status"),
        repo: str | None = None,
        thread: int | None = None,
        action: str | None = None,
        intent: str | None = None,
        actor: str | None = None,
        since: str | None = None,
        until: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        return {
            "jobs": list_jobs(
                config.db,
                status_filter=status_filter,
                repo=repo,
                thread=thread,
                action=action,
                intent=intent,
                actor=actor,
                since=since,
                until=until,
                limit=limit,
            )
        }

    @app.get("/api/jobs/actors")
    def api_job_actors(_: str = Depends(current_user), limit: int = 100) -> dict[str, Any]:
        return {"actors": list_job_actors(config.db, limit=limit)}

    @app.get("/api/jobs/{job_id}")
    def api_job(job_id: int, _: str = Depends(current_user)) -> dict[str, Any]:
        job = get_job_detail(config.db, job_id)
        if job is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job_not_found")
        return {"job": job}

    @app.get("/api/jobs/{job_id}/logs")
    def api_job_logs(job_id: int, limit: int = 100, _: str = Depends(current_user)) -> dict[str, Any]:
        return {"logs": job_logs(config.db, job_id, limit=limit)}

    @app.post("/api/jobs/{job_id}/retry")
    def api_job_retry(job_id: int, profile: dict[str, Any] = Depends(current_admin_profile)) -> dict[str, Any]:
        if get_job_detail(config.db, job_id) is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job_not_found")
        if not JobQueue(config.db).retry(job_id, actor=str(profile["login"])):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="job_not_retryable")
        job = get_job_detail(config.db, job_id)
        return {"job": job, "detail": "job_requeued"}

    @app.post("/api/jobs/{job_id}/dismiss")
    def api_job_dismiss(job_id: int, profile: dict[str, Any] = Depends(current_admin_profile)) -> dict[str, Any]:
        if get_job_detail(config.db, job_id) is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job_not_found")
        if not JobQueue(config.db).dismiss(job_id, f"dismissed by @{profile['login']}"):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="job_not_dismissable")
        job = get_job_detail(config.db, job_id)
        return {"job": job, "detail": "job_dismissed"}

    @app.post("/api/jobs/{job_id}/cancel")
    async def api_job_cancel(job_id: int, request: Request, profile: dict[str, Any] = Depends(current_profile)) -> dict[str, Any]:
        if get_job_detail(config.db, job_id) is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job_not_found")
        if not can_cancel_job(job_id, profile):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="job_cancel_not_allowed")
        try:
            payload = await request.json()
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invalid_json") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="cancel_payload_required")
        reason = str(payload.get("reason") or "").strip() or None
        result = cancel_running_job(JobQueue(config.db), job_id, actor=str(profile["login"]), reason=reason)
        if not result.cancelled:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="job_not_running")
        job = get_job_detail(config.db, job_id)
        return {
            "job": job,
            "detail": "job_cancelled",
            "signalled": result.signalled,
            "signal_detail": result.detail,
            "followup_url": result.followup_url,
        }

    @app.get("/api/jobs/{job_id}/session")
    def api_job_session(job_id: int, _: str = Depends(current_user)) -> dict[str, Any]:
        session = job_session(config.db, job_id)
        if session is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job_not_found")
        return {"session": session}

    @app.get("/api/jobs/{job_id}/session/events")
    def api_job_session_events(job_id: int, after_id: int | None = None, limit: int = 100, _: str = Depends(current_user)) -> dict[str, Any]:
        if job_session(config.db, job_id) is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job_not_found")
        return {"events": job_session_events(config.db, job_id, after_id=after_id, limit=limit)}

    @app.get("/api/jobs/{job_id}/session/transcript")
    def api_job_session_transcript(job_id: int, limit: int = 500, _: str = Depends(current_user)) -> dict[str, Any]:
        if job_session(config.db, job_id) is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job_not_found")
        return {"entries": job_session_transcript(config.db, job_id, limit=limit)}

    @app.get("/api/jobs/{job_id}/session/stream")
    def api_job_session_stream(job_id: int, after_id: int | None = None, _: str = Depends(current_user)) -> StreamingResponse:
        if job_session(config.db, job_id) is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job_not_found")

        return StreamingResponse(
            _session_stream_events(config.db, job_id, after_id=after_id, shutdown_event=app.state.dashboard_shutdown_event),
            media_type="text/event-stream",
            headers=_sse_headers(),
        )

    @app.get("/api/metrics/summary")
    def api_metrics(timezone: str = "UTC", _: str = Depends(current_user)) -> dict[str, Any]:
        return {"metrics": metrics_summary(config.db, timezone_name=timezone)}

    @app.get("/api/processes")
    def api_processes(_: str = Depends(current_user)) -> dict[str, Any]:
        report = monitor(config.db)
        metrics = report.metrics
        samples = recent_process_samples(config.db, limit=60)
        latest_sample = samples[-1] if samples else None
        running_jobs = metrics.get("running_jobs", [])
        return {
            "running_jobs": running_jobs,
            "executor": {
                "service": metrics.get("executor_service", "unknown"),
                "pid": metrics.get("executor_pid"),
                "children": metrics.get("executor_children", []),
                "workers": metrics.get("worker_heartbeats", []),
                "expected_workers": metrics.get("executor_worker_count", 0),
            },
            "signals": {
                "worker_liveness": {
                    "state": (
                        "live"
                        if metrics.get("executor_worker_count", 0)
                        and metrics.get("worker_heartbeats_live", 0) == metrics.get("executor_worker_count", 0)
                        else "degraded"
                    ),
                    "live_count": metrics.get("worker_heartbeats_live", 0),
                    "expected_count": metrics.get("executor_worker_count", 0),
                },
                "live_process": {
                    "state": "live" if metrics.get("executor_children") else "no_child_process",
                    "child_count": len(metrics.get("executor_children", []) or []),
                },
                "process_activity": {
                    "state": "active" if latest_sample and latest_sample.get("active_since_last_sample") else "quiet",
                    "idle_seconds": latest_sample.get("idle_seconds") if latest_sample else None,
                    "sample_ts": latest_sample.get("ts") if latest_sample else None,
                },
                "semantic_progress": [job for job in running_jobs if job.get("semantic_progress")],
                "visible_progress": [job for job in running_jobs if job.get("visible_progress")],
            },
            "alerts": report.alerts,
            "samples": samples,
            "detail": "Worker liveness, process state, persisted process activity samples, semantic job progress and visible OpenClaw output are reported separately.",
        }

    @app.get("/api/systemd")
    def api_systemd(_: str = Depends(current_user)) -> dict[str, Any]:
        return systemd_status()

    @app.get("/api/systemd/journal/stream")
    def api_systemd_journal_stream(unit: str, _: str = Depends(current_user)) -> StreamingResponse:
        if unit not in allowed_unit_names():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="systemd_unit_not_allowed")
        return StreamingResponse(_journal_stream_events(unit, shutdown_event=app.state.dashboard_shutdown_event), media_type="text/event-stream", headers=_sse_headers())

    @app.get("/api/alerts")
    def api_alerts(include_resolved: bool = False, limit: int = 50, _: str = Depends(current_user)) -> dict[str, Any]:
        return {
            "alerts": list_alerts(config.db, include_resolved=include_resolved, limit=limit),
            "detail": "Persistent monitor alert observations; unresolved alerts are active.",
        }

    @app.get("/api/knowledge")
    def api_knowledge(
        profile: dict[str, Any] = Depends(current_profile),
        repo: str | None = None,
        proposal_status: str | None = Query(default=None, alias="status"),
        limit: int = 50,
    ) -> dict[str, Any]:
        scope = f"repo:{repo.strip().lower()}" if repo and repo.strip() else ""
        status_filter = (proposal_status or "").strip().lower()
        proposals = list_proposals(config.db, status=status_filter, limit=limit)
        if scope:
            proposals = [item for item in proposals if item["scope"] == scope or item["scope"].startswith(f"{scope}:")]
        events = list_events(config.db, scope=scope, limit=limit)
        rules = list_applicable_rules(config.db, repo.strip().lower(), min_confidence=0) if scope else list_rules(config.db, min_confidence=0)
        proposals = _mark_manageable_knowledge(proposals, profile)
        events = _mark_manageable_knowledge(events, profile)
        rules = _mark_manageable_knowledge(rules, profile)
        return {
            "repositories": list_repositories(config.db),
            "events": events,
            "proposals": proposals,
            "rules": rules,
            "summary": {
                "events": len(events),
                "rules": len(rules),
                "proposed": sum(1 for item in proposals if item["status"] == "proposed"),
                "approved": sum(1 for item in proposals if item["status"] == "approved"),
                "rejected": sum(1 for item in proposals if item["status"] == "rejected"),
                "errors": sum(1 for item in proposals if item["status"] == "error"),
            },
        }

    @app.post("/api/knowledge/proposals/{proposal_id}/approve")
    def api_knowledge_approve(proposal_id: str, _: dict[str, Any] = Depends(current_admin_profile)) -> dict[str, Any]:
        proposal = approve_proposal(config.db, proposal_id, react=True, gh_bin=os.getenv("GITHUB_AGENT_BRIDGE_GH_BIN", "gh"))
        if proposal is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="knowledge_proposal_not_found")
        return {"proposal": proposal, "detail": "knowledge_proposal_approved"}

    @app.post("/api/knowledge/proposals/{proposal_id}/reject")
    def api_knowledge_reject(proposal_id: str, _: dict[str, Any] = Depends(current_admin_profile)) -> dict[str, Any]:
        proposal = reject_proposal(config.db, proposal_id)
        if proposal is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="knowledge_proposal_not_found")
        return {"proposal": proposal, "detail": "knowledge_proposal_rejected"}

    @app.delete("/api/knowledge/rules/{rule_id}")
    def api_knowledge_rule_delete(rule_id: str, profile: dict[str, Any] = Depends(current_profile)) -> dict[str, Any]:
        rule = next((item for item in list_rules(config.db, min_confidence=0) if item["id"] == rule_id), None)
        if rule is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="knowledge_rule_not_found")
        if not profile.get("is_admin") and not _knowledge_item_owned_by(rule, str(profile.get("login") or "")):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="knowledge_rule_owner_required")
        if not delete_rule(config.db, rule_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="knowledge_rule_not_found")
        return {"detail": "knowledge_rule_deleted"}

    @app.patch("/api/knowledge/rules/{rule_id}")
    def api_knowledge_rule_update(rule_id: str, payload: dict[str, Any], profile: dict[str, Any] = Depends(current_profile)) -> dict[str, Any]:
        existing = next((item for item in list_rules(config.db, min_confidence=0) if item["id"] == rule_id), None)
        if existing is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="knowledge_rule_not_found")
        if not profile.get("is_admin") and not _knowledge_item_owned_by(existing, str(profile.get("login") or "")):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="knowledge_rule_owner_required")
        try:
            rule = update_rule_scope(config.db, rule_id, str(payload.get("scope") or ""))
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        if rule is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="knowledge_rule_not_found")
        return {"rule": {**rule, "can_manage": True}, "detail": "knowledge_rule_updated"}

    @app.get("/api/mcp/tokens")
    def api_mcp_tokens(profile: dict[str, Any] = Depends(current_profile), include_revoked: bool = False) -> dict[str, Any]:
        owner = None if profile.get("is_admin") else str(profile.get("login") or "")
        return {"tokens": list_tokens(config.db, include_revoked=include_revoked, user_login=owner)}

    @app.get("/api/mcp/users")
    def api_mcp_users(profile: dict[str, Any] = Depends(current_profile)) -> dict[str, Any]:
        if not profile.get("is_admin"):
            return {"users": [_profile_from_login(str(profile.get("login") or ""))]}
        return {"users": _known_mcp_user_profiles(config, current_login=str(profile.get("login") or ""))}

    @app.post("/api/mcp/tokens")
    def api_mcp_token_create(payload: dict[str, Any], profile: dict[str, Any] = Depends(current_profile)) -> dict[str, Any]:
        login = str(profile.get("login") or "")
        requested_owner = str(payload.get("user_login") or "").strip()
        if requested_owner and not profile.get("is_admin") and requested_owner.lower().lstrip("@") != login.lower():
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="admin_required")
        owner = _require_known_mcp_owner(config, requested_owner or login, current_login=login) if profile.get("is_admin") else requested_owner or login
        try:
            created = create_token(config.db, str(payload.get("name") or ""), expires_at=payload.get("expires_at"), user_login=owner, created_by=login)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return {"token": created["token"], "record": created["record"], "detail": "mcp_token_created"}

    @app.patch("/api/mcp/tokens/{token_id}")
    def api_mcp_token_update(token_id: str, payload: dict[str, Any], profile: dict[str, Any] = Depends(current_profile)) -> dict[str, Any]:
        if not profile.get("is_admin"):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="admin_required")
        login = str(profile.get("login") or "")
        owner = _require_known_mcp_owner(config, str(payload.get("user_login") or ""), current_login=login)
        try:
            record = update_token_owner(config.db, token_id, owner)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        if record is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="mcp_token_not_found")
        return {"token": record, "detail": "mcp_token_updated"}

    @app.delete("/api/mcp/tokens/{token_id}")
    def api_mcp_token_revoke(token_id: str, profile: dict[str, Any] = Depends(current_profile)) -> dict[str, Any]:
        owner = None if profile.get("is_admin") else str(profile.get("login") or "")
        if not revoke_token(config.db, token_id, user_login=owner):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="mcp_token_not_found")
        return {"detail": "mcp_token_revoked"}

    @app.post("/api/mcp")
    @app.post("/api/mcp/")
    async def api_mcp_http(request: Request) -> Response:
        if authenticate_token(config.db, _bearer_token(request)) is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid_mcp_token")
        try:
            payload = await request.json()
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invalid_json") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="mcp_request_must_be_object")

        response = MCPServer(config.db).handle(payload)
        if response is None:
            return Response(status_code=status.HTTP_202_ACCEPTED, headers=_redacted_headers())
        return JSONResponse(response, headers=_redacted_headers())

    @app.get("/api/events/stream")
    def api_events(_: str = Depends(current_user)) -> Response:
        return Response("event: ready\ndata: {}\n\n", media_type="text/event-stream", headers=_sse_headers())

    @app.get("/auth/login")
    def login() -> RedirectResponse:
        if not config.oauth_ready:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="oauth_not_configured")
        state = secrets.token_urlsafe(24)
        scopes = ["read:user"]
        if config.allowed_orgs or config.allowed_teams or config.admin_teams:
            scopes.append("read:org")
        params = urllib.parse.urlencode({"client_id": config.oauth_client_id, "scope": " ".join(scopes), "state": state})
        response = RedirectResponse(f"{GITHUB_AUTHORIZE_URL}?{params}", status_code=status.HTTP_302_FOUND)
        response.set_cookie(OAUTH_STATE_COOKIE, _sign(config, state), httponly=True, secure=True, samesite="lax", max_age=600)
        return response

    @app.get("/auth/callback")
    def callback(code: str, state: str, request: Request) -> RedirectResponse:
        if not config.oauth_ready:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="oauth_not_configured")
        signed_state = request.cookies.get(OAUTH_STATE_COOKIE)
        if not signed_state or _unsign(config, signed_state) != state:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="oauth_state_mismatch")
        token = _exchange_code(config, code)
        user = _github_json(GITHUB_USER_URL, token)
        login = str(user.get("login", ""))
        if not login or not _is_allowed(config, login, token):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="not_authorized")
        is_admin = _is_admin(config, login, token)
        response = RedirectResponse("/", status_code=status.HTTP_302_FOUND)
        response.set_cookie(SESSION_COOKIE, _sign(config, _encode_session(user, is_admin=is_admin)), httponly=True, secure=True, samesite="lax")
        response.delete_cookie(OAUTH_STATE_COOKIE)
        return response

    return app


app = create_app()


def build_parser(*, ingress: bool = False) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=Path(sys.argv[0]).name)
    parser.add_argument("--db", default=os.getenv("GITHUB_AGENT_BRIDGE_DASHBOARD_DB", os.getenv("GITHUB_AGENT_BRIDGE_DB", DEFAULT_DB)))
    parser.add_argument("--host", default=os.getenv("GITHUB_AGENT_BRIDGE_WEBHOOK_HOST", DEFAULT_HOST) if ingress else DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=int(os.getenv("GITHUB_AGENT_BRIDGE_WEBHOOK_PORT", "8766")) if ingress else DEFAULT_PORT)
    parser.add_argument("--fd", type=int, help="serve an inherited systemd socket file descriptor")
    if not ingress:
        parser.add_argument("--no-auth", action="store_true", help="disable auth for isolated local development only")
    return parser


def _serve_uvicorn(application: FastAPI, args: argparse.Namespace) -> int:
    try:
        import uvicorn
    except ImportError:
        print("uvicorn is required; install github-agent-bridge[dashboard]", file=sys.stderr)
        return 2
    if args.fd is not None:
        uvicorn.run(application, fd=args.fd)
    else:
        uvicorn.run(application, host=args.host, port=args.port)
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return _serve_uvicorn(create_app(DashboardConfig(db=args.db, require_auth=not args.no_auth)), args)


def webhook_main(argv: list[str] | None = None) -> int:
    args = build_parser(ingress=True).parse_args(argv)
    return _serve_uvicorn(create_webhook_app(DashboardConfig(db=args.db)), args)


if __name__ == "__main__":
    raise SystemExit(main())
