from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import Notification, utc_now


@dataclass(frozen=True)
class ShadowReceipt:
    delivery_id: str
    event_name: str
    action: str | None
    event_key: str | None
    repository: str | None
    status: str


def verify_signature(payload: bytes, signature: str, secrets: tuple[str, ...]) -> bool:
    if not signature.startswith("sha256=") or not secrets:
        return False
    supplied = signature.removeprefix("sha256=")
    return any(
        hmac.compare_digest(
            hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest(),
            supplied,
        )
        for secret in secrets
    )


def canonical_webhook_event_key(event_name: str, payload: dict[str, Any]) -> str | None:
    action = str(payload.get("action") or "")
    repository = payload.get("repository") or {}
    repo = str(repository.get("full_name") or "").lower()
    if not repo:
        return None
    objects = {
        "issue_comment": "comment",
        "pull_request_review_comment": "comment",
        "pull_request_review": "review",
        "commit_comment": "comment",
    }
    object_name = objects.get(event_name)
    target = payload.get(object_name) if object_name else None
    target_id = target.get("id") if isinstance(target, dict) else None
    if target_id and action:
        canonical_action = "created" if action in {"created", "submitted"} else action
        return f"{event_name}:{canonical_action}:{repo}:{target_id}"
    if event_name == "workflow_run":
        run = payload.get("workflow_run") or {}
        run_id = run.get("id")
        if run_id and action:
            return f"workflow_run:{action}:{repo}:{run_id}"
    return None


def webhook_notification(
    event_name: str,
    delivery_id: str,
    payload: dict[str, Any],
) -> Notification | None:
    """Translate actionable webhook payloads into the transport-neutral queue input."""
    action = str(payload.get("action") or "")
    if (event_name, action) not in {
        ("issue_comment", "created"),
        ("pull_request_review_comment", "created"),
        ("pull_request_review", "submitted"),
        ("commit_comment", "created"),
        ("workflow_run", "completed"),
    }:
        return None
    repository = payload.get("repository") if isinstance(payload.get("repository"), dict) else {}
    repo = str(repository.get("full_name") or "")
    if not repo:
        return None
    subject = payload.get("pull_request") or payload.get("issue") or payload.get("workflow_run") or {}
    number = subject.get("number") if isinstance(subject, dict) else None
    source = (
        payload.get("comment") or payload.get("review") or payload.get("workflow_run") or {}
    )
    if not isinstance(source, dict):
        return None
    url = str(source.get("html_url") or subject.get("html_url") or repository.get("html_url") or "")
    if not url.startswith("https://github.com/"):
        return None
    body = str(source.get("body") or "")
    if event_name == "workflow_run":
        body = f"Workflow run {source.get('conclusion') or source.get('status') or action}.\n{body}"
    sender = payload.get("sender") if isinstance(payload.get("sender"), dict) else {}
    login = str(sender.get("login") or "GitHub")
    title = str(subject.get("title") or subject.get("name") or event_name)
    suffix = f" (#{number})" if number else ""
    return Notification(
        uid=None,
        message_id=f"<{delivery_id}@github.com>",
        subject=f"[{repo}] {title}{suffix}",
        from_addr=f"{login} <notifications@github.com>",
        body=f"{body}\n\n{url}",
        auth={"spf": True, "dkim": True, "dmarc": True},
    )


def persist_shadow_delivery(
    db: str | Path,
    *,
    delivery_id: str,
    event_name: str,
    raw_payload: bytes,
    hook_id: str | None = None,
    retention_days: int = 30,
) -> ShadowReceipt:
    payload = json.loads(raw_payload)
    action = str(payload.get("action") or "") or None
    repository = payload.get("repository") or {}
    repo = str(repository.get("full_name") or "") or None
    event_key = canonical_webhook_event_key(event_name, payload)
    status = "observed" if event_key else "unsupported"
    payload_hash = hashlib.sha256(raw_payload).hexdigest()
    now = utc_now()
    con = sqlite3.connect(Path(db).expanduser(), timeout=30)
    try:
        con.execute(
            "DELETE FROM webhook_shadow_receipts WHERE julianday(created_at) < julianday('now', ?)",
            (f"-{retention_days} days",),
        )
        try:
            con.execute(
                "INSERT INTO webhook_shadow_receipts(delivery_id,hook_id,event_name,action,event_key,repository,payload_hash,status,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (delivery_id, hook_id, event_name, action, event_key, repo, payload_hash, status, now),
            )
        except sqlite3.IntegrityError:
            con.rollback()
            con.execute(
                "UPDATE webhook_shadow_receipts SET duplicate_count=duplicate_count+1 WHERE delivery_id=?",
                (delivery_id,),
            )
            status = "duplicate"
        if hook_id:
            hook = payload.get("hook") if isinstance(payload, dict) else None
            organization = payload.get("organization") if isinstance(payload, dict) else None
            target_type = "organization" if isinstance(organization, dict) else "repository"
            target = (
                organization.get("login") if isinstance(organization, dict)
                else repository.get("full_name") if isinstance(repository, dict)
                else None
            ) or "unknown"
            if event_name == "ping" and isinstance(hook, dict):
                config = hook.get("config") if isinstance(hook.get("config"), dict) else {}
                events = hook.get("events") if isinstance(hook.get("events"), list) else []
                insecure_ssl = config.get("insecure_ssl")
                insecure_ssl_value = None if insecure_ssl is None else int(str(insecure_ssl) == "1")
                con.execute(
                    "INSERT INTO webhook_hooks("
                    "hook_id,target,target_type,name,active,events_json,content_type,insecure_ssl,delivery_url,"
                    "github_api_url,ping_url,deliveries_url,github_created_at,github_updated_at,last_ping_at,updated_at"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(hook_id) DO UPDATE SET "
                    "target=excluded.target,target_type=excluded.target_type,name=excluded.name,active=excluded.active,"
                    "events_json=excluded.events_json,content_type=excluded.content_type,insecure_ssl=excluded.insecure_ssl,"
                    "delivery_url=excluded.delivery_url,github_api_url=excluded.github_api_url,ping_url=excluded.ping_url,"
                    "deliveries_url=excluded.deliveries_url,github_created_at=excluded.github_created_at,"
                    "github_updated_at=excluded.github_updated_at,last_ping_at=excluded.last_ping_at,updated_at=excluded.updated_at",
                    (
                        hook_id, target, target_type, str(hook.get("name") or "") or None,
                        int(bool(hook.get("active", True))), json.dumps(events),
                        str(config.get("content_type") or "") or None, insecure_ssl_value,
                        str(config.get("url") or "") or None, str(hook.get("url") or "") or None,
                        str(hook.get("ping_url") or "") or None, str(hook.get("deliveries_url") or "") or None,
                        str(hook.get("created_at") or "") or None, str(hook.get("updated_at") or "") or None,
                        now, now,
                    ),
                )
            else:
                con.execute(
                    "INSERT INTO webhook_hooks("
                    "hook_id,target,target_type,active,events_json,last_event_at,last_delivery_id,last_event_name,"
                    "last_action,last_repository,last_result,updated_at"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(hook_id) DO UPDATE SET "
                    "last_event_at=excluded.last_event_at,last_delivery_id=excluded.last_delivery_id,"
                    "last_event_name=excluded.last_event_name,last_action=excluded.last_action,"
                    "last_repository=excluded.last_repository,last_result=excluded.last_result,updated_at=excluded.updated_at",
                    (
                        hook_id, target, target_type, 1, "[]", now, delivery_id, event_name,
                        action, repo, status, now,
                    ),
                )
        con.commit()
    finally:
        con.close()
    return ShadowReceipt(delivery_id, event_name, action, event_key, repo, status)
