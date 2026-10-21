from __future__ import annotations

import re

from .models import (
    EventProvenance,
    GitHubContext,
    NormalizedEventEnvelope,
    Notification,
    TriggerActor,
)
from .parser import github_event_flags


LIFECYCLE_ACTION_RE = re.compile(r"\b(?P<action>closed|reopened) this\b")


def canonical_event_identity(
    *,
    repository: str | None,
    thread: int | None,
    event_type: str | None,
    event_action: str | None,
    object_id: int | str | None = None,
    target_login: str | None = None,
) -> str | None:
    """Return a transport-neutral identity only when semantics are stable.

    Thread-scoped lifecycle, assignment and review-request events intentionally
    use the public issue/PR number. Both webhook payloads and notification mail
    expose it, unlike GitHub's transport-specific database/timeline ids.
    """
    repo = (repository or "").lower()
    if not repo or not event_type or not event_action:
        return None
    action = "created" if event_action == "submitted" else event_action
    if event_type in {
        "issue_comment",
        "pull_request_review_comment",
        "pull_request_review",
        "commit_comment",
    }:
        if object_id:
            return f"{event_type}:{action}:{repo}:{object_id}"
        return None
    if event_type in {"issues", "pull_request"} and action == "assigned":
        if thread and target_login:
            return f"{event_type}:assigned:{repo}:{thread}:{target_login.lower()}"
        return None
    if event_type == "pull_request" and action == "review_requested":
        if thread and target_login:
            return (
                f"pull_request:review_requested:{repo}:{thread}:"
                f"{target_login.lower()}"
            )
        return None
    if event_type == "pull_request" and action == "merged" and thread:
        return f"pull_request:merged:{repo}:{thread}"
    if event_type == "issues" and action in {"closed", "reopened"} and thread:
        return f"issues:{action}:{repo}:{thread}"
    if event_type == "workflow_run" and object_id:
        return f"workflow_run:{action}:{repo}:{object_id}"
    return None


def _subject_type(ctx: GitHubContext) -> str:
    if ctx.target_kind == "workflow_run":
        return "workflow_run"
    if ctx.target_kind in {"commit", "commit_comment"}:
        return "commit"
    if ctx.is_pull_request:
        return "pull_request"
    if ctx.issue_number:
        return "issue"
    return "unknown"


def _target_login(
    notification: Notification,
    event_action: str | None,
    bot_logins: set[str],
) -> str | None:
    structured = str(notification.metadata.get("github_target_login") or "")
    if structured:
        return structured.lower().lstrip("@")
    if event_action not in {"assigned", "review_requested"}:
        return None
    text = f"{notification.subject}\n{notification.body}".lower()
    configured = sorted(login.lower().lstrip("@") for login in bot_logins)
    for login in configured:
        if f"@{login}" in text or re.search(rf"\b{re.escape(login)}\b", text):
            return login
    return configured[0] if len(configured) == 1 else None


def _descriptor(
    notification: Notification,
    ctx: GitHubContext,
    bridge_action: str,
    bot_logins: set[str],
) -> tuple[str | None, str | None, int | str | None, str | None]:
    event_type = str(notification.metadata.get("github_event") or "") or None
    event_action = str(notification.metadata.get("github_action") or "") or None
    object_id: int | str | None = None
    flags = github_event_flags(notification.subject, notification.body, bot_logins)

    if ctx.comment_id:
        event_type, event_action, object_id = "issue_comment", "created", ctx.comment_id
    elif ctx.review_comment_id:
        event_type, event_action, object_id = (
            "pull_request_review_comment",
            "created",
            ctx.review_comment_id,
        )
    elif ctx.review_id:
        event_type, event_action, object_id = (
            "pull_request_review",
            "created",
            ctx.review_id,
        )
    elif ctx.commit_comment_id:
        event_type, event_action, object_id = (
            "commit_comment",
            "created",
            ctx.commit_comment_id,
        )
    elif ctx.workflow_run_id:
        event_type, event_action, object_id = (
            "workflow_run",
            bridge_action,
            ctx.workflow_run_id,
        )
    elif bridge_action == "sync_after_merge":
        event_type, event_action = "pull_request", "merged"
    elif flags["assigned"] and bridge_action == "open_issue":
        event_type = "pull_request" if ctx.is_pull_request else "issues"
        event_action = "assigned"
    elif flags["review_requested"] and bridge_action == "submit_review":
        event_type, event_action = "pull_request", "review_requested"
    elif bridge_action == "archive_notification":
        lifecycle = LIFECYCLE_ACTION_RE.search(
            f"{notification.subject}\n{notification.body}".lower()
        )
        if lifecycle and ctx.issue_number:
            event_type = "pull_request" if ctx.is_pull_request else "issues"
            event_action = lifecycle.group("action")

    target_login = _target_login(notification, event_action, bot_logins)
    return event_type, event_action, object_id, target_login


def normalize_event_envelope(
    notification: Notification,
    ctx: GitHubContext,
    *,
    bridge_action: str,
    work_intent: str,
    source: str,
    source_key: str,
    trigger_actor: TriggerActor | None,
    bot_logins: set[str],
) -> NormalizedEventEnvelope:
    event_type, event_action, object_id, target_login = _descriptor(
        notification, ctx, bridge_action, bot_logins
    )
    identity = canonical_event_identity(
        repository=ctx.repo,
        thread=ctx.issue_number,
        event_type=event_type,
        event_action=event_action,
        object_id=object_id,
        target_login=target_login,
    ) or f"{source}:{source_key}"
    occurred_at = str(notification.metadata.get("github_event_timestamp") or "") or None
    return NormalizedEventEnvelope(
        repository=ctx.repo,
        thread=ctx.issue_number,
        subject_type=_subject_type(ctx),
        event_type=event_type,
        event_action=("created" if event_action == "submitted" else event_action),
        actor=trigger_actor.login if trigger_actor else None,
        occurred_at=occurred_at,
        canonical_identity=identity,
        bridge_action=bridge_action,
        work_intent=work_intent,
        provenance=EventProvenance(
            transport=source,
            receipt_id=source_key,
            received_at=notification.source_received_at or notification.received_at,
        ),
    )
