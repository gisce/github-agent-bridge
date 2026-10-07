from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable

from .actors import normalize_github_login
from .models import utc_now
from .queue import JobQueue
from .persistence import WebPushRepository, WebPushSubscription

PushSender = Callable[[dict[str, Any], dict[str, Any]], None]
_APP_ICON_CACHE: dict[str, str | None] = {}


def _repository(db: str | Path) -> WebPushRepository:
    queue = JobQueue(db)
    return WebPushRepository(queue.database)


def _validate_subscription(subscription: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    endpoint = str(subscription.get("endpoint") or "").strip()
    keys = subscription.get("keys")
    if not endpoint or not endpoint.startswith("https://"):
        raise ValueError("subscription_endpoint_required")
    if not isinstance(keys, dict) or not keys.get("p256dh") or not keys.get("auth"):
        raise ValueError("subscription_keys_required")
    return endpoint, subscription


def save_subscription(db: str | Path, user_login: str, subscription: dict[str, Any]) -> dict[str, Any]:
    login = normalize_github_login(user_login).lower()
    if not login:
        raise ValueError("user_login_required")
    endpoint, payload = _validate_subscription(subscription)
    now = utc_now()
    return _repository(db).save(login, endpoint, payload, now).to_dict()


def delete_subscription(db: str | Path, user_login: str, endpoint: str) -> bool:
    login = normalize_github_login(user_login).lower()
    now = utc_now()
    return _repository(db).disable(login, endpoint, now)


def subscription_status(db: str | Path, user_login: str) -> dict[str, Any]:
    login = normalize_github_login(user_login).lower()
    subscriptions = _repository(db).active_for_user(login)
    return {
        "enabled": bool(subscriptions),
        "subscriptions": [
            {
                "id": subscription.id,
                "endpoint": subscription.endpoint,
                "updated_at": subscription.updated_at,
                "last_success_at": subscription.last_success_at,
                "last_error": subscription.last_error,
            }
            for subscription in subscriptions
        ],
    }


def notify_job_completion(
    db: str | Path,
    *,
    actors: list[str],
    job_id: int,
    work_key: str,
    status: str,
    summary: str,
    detail: str | None = None,
    followup_url: str | None = None,
    dashboard_url: str | None = None,
    sender: PushSender | None = None,
) -> dict[str, Any]:
    recipients = _recipient_logins(actors)
    if not recipients:
        return {"recipients": [], "attempted": 0, "sent": 0, "failed": 0}
    subscriptions = _active_subscriptions(db, recipients)
    attempted = sent = failed = 0
    payload = _job_completion_payload(
        job_id=job_id,
        work_key=work_key,
        status=status,
        summary=summary,
        detail=detail,
        followup_url=followup_url,
        dashboard_url=dashboard_url,
    )
    push = sender or _send_web_push
    for row in subscriptions:
        attempted += 1
        try:
            push(row.subscription, payload)
        except Exception as exc:
            failed += 1
            _mark_delivery(db, row.id, error=str(exc)[:500])
        else:
            sent += 1
            _mark_delivery(db, row.id)
    return {"recipients": recipients, "attempted": attempted, "sent": sent, "failed": failed}


def _recipient_logins(actors: list[str]) -> list[str]:
    recipients: list[str] = []
    seen: set[str] = set()
    for actor in actors:
        login = normalize_github_login(actor)
        if not login or login.endswith("[bot]"):
            continue
        key = login.lower()
        if key == "github" or key in seen:
            continue
        seen.add(key)
        recipients.append(key)
    return recipients


def _active_subscriptions(
    db: str | Path, recipients: list[str]
) -> list[WebPushSubscription]:
    return _repository(db).active_for_recipients(recipients)


def _job_completion_payload(
    *,
    job_id: int,
    work_key: str,
    status: str,
    summary: str,
    detail: str | None,
    followup_url: str | None,
    dashboard_url: str | None,
) -> dict[str, Any]:
    base_url = (dashboard_url or os.getenv("GITHUB_AGENT_BRIDGE_DASHBOARD_PUBLIC_URL", "")).rstrip("/")
    dashboard_job_url = f"{base_url}/jobs/{job_id}" if base_url else f"/jobs/{job_id}"
    icon_url = _notification_icon_url()
    return {
        "title": f"Bridge job {status}",
        "body": f"{work_key} finished with status {status}",
        "tag": f"gab-job-{job_id}",
        "url": dashboard_job_url,
        "job_url": dashboard_job_url,
        "github_url": followup_url,
        "followup_url": followup_url,
        "job_id": job_id,
        "work_key": work_key,
        "status": status,
        "summary": summary,
        "detail": detail,
        "icon": icon_url or None,
        "timestamp": utc_now(),
    }


def _notification_icon_url() -> str | None:
    configured = os.getenv("GITHUB_AGENT_BRIDGE_WEB_PUSH_ICON_URL", "").strip()
    if configured:
        return configured
    app_id = _github_app_id()
    if app_id:
        return _github_app_avatar_url(app_id)
    return _github_app_avatar_url_from_slug(_github_app_slug())


def _github_app_id() -> str:
    for name in ("GITHUB_AGENT_BRIDGE_GITHUB_APP_ID", "GITHUB_APP_ID"):
        value = os.getenv(name, "").strip()
        if value.isdigit():
            return value
    return ""


def _github_app_slug() -> str:
    for name in ("GITHUB_AGENT_BRIDGE_GITHUB_APP_SLUG", "GITHUB_APP_SLUG"):
        value = os.getenv(name, "").strip().strip("/")
        if value:
            return value
    return ""


def _github_app_avatar_url(app_id: str) -> str:
    return f"https://avatars.githubusercontent.com/in/{app_id}?s=192&v=4"


def _github_app_avatar_url_from_slug(slug: str) -> str | None:
    if not slug:
        return None
    if slug in _APP_ICON_CACHE:
        return _APP_ICON_CACHE[slug]
    try:
        data = _github_app_metadata(slug)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError):
        _APP_ICON_CACHE[slug] = None
        return None
    app_id = data.get("id") if isinstance(data, dict) else None
    if isinstance(app_id, bool):
        icon_url = None
    elif isinstance(app_id, int) or (isinstance(app_id, str) and app_id.isdigit()):
        icon_url = _github_app_avatar_url(str(app_id))
    else:
        icon_url = None
    _APP_ICON_CACHE[slug] = icon_url
    return icon_url


def _github_app_metadata(slug: str) -> dict[str, Any]:
    quoted_slug = urllib.parse.quote(slug, safe="")
    req = urllib.request.Request(
        f"https://api.github.com/apps/{quoted_slug}",
        headers={"Accept": "application/vnd.github+json", "User-Agent": "github-agent-bridge"},
    )
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def _send_web_push(subscription: dict[str, Any], payload: dict[str, Any]) -> None:
    private_key = os.getenv("GITHUB_AGENT_BRIDGE_WEB_PUSH_VAPID_PRIVATE_KEY", "").strip()
    contact = os.getenv("GITHUB_AGENT_BRIDGE_WEB_PUSH_VAPID_CONTACT", "mailto:admin@example.com").strip()
    if not private_key:
        raise RuntimeError("web_push_vapid_private_key_not_configured")
    from pywebpush import webpush

    webpush(
        subscription_info=subscription,
        data=json.dumps(payload, separators=(",", ":")),
        vapid_private_key=private_key,
        vapid_claims={"sub": contact},
    )


def _mark_delivery(db: str | Path, subscription_id: int, error: str | None = None) -> None:
    _repository(db).mark_delivery(subscription_id, utc_now(), error)
