from __future__ import annotations

import json
import subprocess
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable


REQUIRED_WEBHOOK_EVENTS = frozenset({
    "commit_comment",
    "issue_comment",
    "issues",
    "pull_request",
    "pull_request_review",
    "pull_request_review_comment",
    "workflow_run",
})


class WebhookAuditError(RuntimeError):
    pass


@dataclass(frozen=True)
class WebhookAuditReport:
    owner: str
    endpoint: str
    hook_id: int | None = None
    ok: bool = True
    alerts: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "owner": self.owner,
            "endpoint": self.endpoint,
            "hook_id": self.hook_id,
            "alerts": self.alerts,
            "metrics": self.metrics,
        }

    def text(self) -> str:
        status = "OK" if self.ok else "ALERT"
        parts = [
            f"{status} GitHub organization webhook owner={self.owner} "
            f"hook_id={self.hook_id or '-'} endpoint={self.endpoint}"
        ]
        parts.extend(f"- {alert}" for alert in self.alerts)
        if self.ok:
            parts.append(
                f"- latest_delivery_age={self.metrics.get('latest_delivery_age_seconds')}s "
                f"consecutive_failures={self.metrics.get('consecutive_failures', 0)}"
            )
        return "\n".join(parts)


def gh_api(path: str, *, gh_bin: str = "gh") -> Any:
    try:
        proc = subprocess.run(
            [gh_bin, "api", path],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired as exc:
        raise WebhookAuditError(
            f"GitHub API request timed out for {path} after {exc.timeout}s"
        ) from exc
    except FileNotFoundError as exc:
        raise WebhookAuditError(f"GitHub CLI executable not found: {gh_bin}") from exc
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip() or "unknown GitHub API error"
        raise WebhookAuditError(f"GitHub API request failed for {path}: {detail}")
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise WebhookAuditError(f"GitHub API returned invalid JSON for {path}") from exc


def audit_organization_webhook(
    owner: str,
    endpoint: str,
    *,
    max_delivery_age_seconds: int,
    failure_threshold: int,
    api: Callable[[str], Any],
    now: datetime | None = None,
) -> WebhookAuditReport:
    owner = owner.strip()
    endpoint = endpoint.strip()
    if not owner or not endpoint:
        raise ValueError("owner and endpoint are required")
    if max_delivery_age_seconds < 1:
        raise ValueError("max_delivery_age_seconds must be positive")
    if failure_threshold < 1:
        raise ValueError("failure_threshold must be positive")

    owner_path = urllib.parse.quote(owner, safe="")
    hooks = api(f"/orgs/{owner_path}/hooks?per_page=100")
    if not isinstance(hooks, list):
        raise WebhookAuditError("GitHub hook inventory returned an unexpected response")

    matches = [
        hook for hook in hooks
        if isinstance(hook, dict)
        and isinstance(hook.get("config"), dict)
        and str(hook["config"].get("url") or "") == endpoint
    ]
    if len(matches) != 1:
        reason = "not found" if not matches else "matched more than one hook"
        return WebhookAuditReport(
            owner=owner,
            endpoint=endpoint,
            ok=False,
            alerts=[f"webhook endpoint {reason} in the organization hook inventory"],
            metrics={"matching_hooks": len(matches)},
        )

    hook = matches[0]
    hook_id = int(hook["id"])
    config = hook.get("config") or {}
    events = {str(event) for event in hook.get("events") or []}
    missing_events = sorted(REQUIRED_WEBHOOK_EVENTS - events)
    tls_verify = config.get("insecure_ssl") == "0"
    alerts: list[str] = []
    if not bool(hook.get("active")):
        alerts.append("webhook is inactive")
    if str(config.get("content_type") or "").lower() != "json":
        alerts.append("webhook content type is not json")
    if not tls_verify:
        if config.get("insecure_ssl") is None:
            alerts.append("webhook TLS verification setting is missing")
        else:
            alerts.append("webhook TLS verification is disabled")
    if missing_events:
        alerts.append("missing required event subscriptions: " + ", ".join(missing_events))

    deliveries = api(f"/orgs/{owner_path}/hooks/{hook_id}/deliveries?per_page=100")
    if not isinstance(deliveries, list):
        raise WebhookAuditError("GitHub hook deliveries returned an unexpected response")
    deliveries = sorted(
        (delivery for delivery in deliveries if isinstance(delivery, dict)),
        key=lambda delivery: str(delivery.get("delivered_at") or ""),
        reverse=True,
    )

    latest_age: int | None = None
    consecutive_failures = 0
    if not deliveries:
        alerts.append("webhook has no retained GitHub deliveries")
    else:
        delivered_at = _parse_github_datetime(deliveries[0].get("delivered_at"))
        if delivered_at is None:
            alerts.append("latest webhook delivery has no valid delivered_at timestamp")
        else:
            current = now or datetime.now(timezone.utc)
            if current.tzinfo is None:
                current = current.replace(tzinfo=timezone.utc)
            latest_age = max(0, int((current - delivered_at).total_seconds()))
            if latest_age > max_delivery_age_seconds:
                alerts.append(
                    f"latest webhook delivery age {latest_age}s exceeds "
                    f"{max_delivery_age_seconds}s"
                )
        for delivery in deliveries:
            if _delivery_succeeded(delivery):
                break
            consecutive_failures += 1
        if consecutive_failures >= failure_threshold:
            alerts.append(
                f"webhook has {consecutive_failures} consecutive failed deliveries "
                f"(threshold {failure_threshold})"
            )

    metrics = {
        "active": bool(hook.get("active")),
        "content_type": config.get("content_type"),
        "tls_verify": tls_verify,
        "events": sorted(events),
        "missing_events": missing_events,
        "deliveries_checked": len(deliveries),
        "latest_delivery_age_seconds": latest_age,
        "consecutive_failures": consecutive_failures,
    }
    return WebhookAuditReport(
        owner=owner,
        endpoint=endpoint,
        hook_id=hook_id,
        ok=not alerts,
        alerts=alerts,
        metrics=metrics,
    )


def _parse_github_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _delivery_succeeded(delivery: dict[str, Any]) -> bool:
    try:
        status_code = int(delivery.get("status_code"))
    except (TypeError, ValueError):
        return False
    return 200 <= status_code < 300
