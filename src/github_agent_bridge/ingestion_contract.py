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
) -> str | None:
    """Return a transport-neutral identity only for one immutable occurrence."""
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
    if event_type == "pull_request" and action == "merged" and thread:
        return f"pull_request:merged:{repo}:{thread}"
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


def _descriptor(
    notification: Notification,
    ctx: GitHubContext,
    bridge_action: str,
    bot_logins: set[str],
) -> tuple[str | None, str | None, int | str | None]:
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

    return event_type, event_action, object_id


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
    event_type, event_action, object_id = _descriptor(
        notification, ctx, bridge_action, bot_logins
    )
    identity = canonical_event_identity(
        repository=ctx.repo,
        thread=ctx.issue_number,
        event_type=event_type,
        event_action=event_action,
        object_id=object_id,
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
