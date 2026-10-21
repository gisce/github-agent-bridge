from __future__ import annotations

import re

from .models import (
    EventProvenance,
    GitHubContext,
    NormalizedEventEnvelope,
    Notification,
    ResolvedGitHubEvent,
    TriggerActor,
)
from .parser import github_event_flags


LIFECYCLE_ACTION_RE = re.compile(r"\b(?P<action>closed|reopened) this\b")
OCCURRENCE_ACTIONS = {
    ("issues", "assigned"),
    ("issues", "unassigned"),
    ("issues", "closed"),
    ("issues", "reopened"),
    ("pull_request", "assigned"),
    ("pull_request", "unassigned"),
    ("pull_request", "review_requested"),
    ("pull_request", "review_request_removed"),
}


def canonical_event_identity(
    *,
    repository: str | None,
    thread: int | None,
    event_type: str | None,
    event_action: str | None,
    object_id: int | str | None = None,
    occurred_at: str | None = None,
    actor: str | None = None,
    target: str | None = None,
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
    if (event_type, action) in OCCURRENCE_ACTIONS and thread:
        normalized_actor = (actor or "").lower()
        normalized_target = (target or "").lower()
        if not occurred_at or not normalized_actor:
            return None
        if action in {"assigned", "unassigned", "review_requested", "review_request_removed"} and not normalized_target:
            return None
        return (
            f"{event_type}:{action}:{repo}:{thread}:{occurred_at}:"
            f"{normalized_actor}:{normalized_target or '-'}"
        )
    return None


def bridge_action_for_resolved_event(
    event: ResolvedGitHubEvent,
    ctx: GitHubContext,
    bot_logins: set[str],
) -> str | None:
    normalized_bots = {login.lower().lstrip("@") for login in bot_logins}
    target_login = (event.target_login or "").lower()
    if event.event == "assigned":
        return (
            "open_issue"
            if target_login in normalized_bots
            else "archive_notification"
        )
    if event.event == "review_requested" and ctx.is_pull_request:
        return (
            "submit_review"
            if target_login in normalized_bots
            else "archive_notification"
        )
    if event.event == "merged" and ctx.is_pull_request:
        return "sync_after_merge"
    if event.event in {
        "closed",
        "reopened",
        "unassigned",
        "review_request_removed",
    }:
        return "archive_notification"
    return None


def _resolved_descriptor(
    event: ResolvedGitHubEvent,
    ctx: GitHubContext,
) -> tuple[str | None, str | None, int | str | None]:
    if event.event == "merged":
        return "pull_request", "merged", None
    if event.event == "review_requested" or event.event == "review_request_removed":
        return "pull_request", event.event, None
    if event.event in {"assigned", "unassigned", "closed", "reopened"}:
        return (
            "pull_request" if ctx.is_pull_request else "issues",
            event.event,
            None,
        )
    return None, None, None


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
    resolved_event: ResolvedGitHubEvent | None,
) -> tuple[str | None, str | None, int | str | None]:
    if resolved_event is not None:
        resolved = _resolved_descriptor(resolved_event, ctx)
        if resolved[0] is not None:
            return resolved
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
    resolved_event: ResolvedGitHubEvent | None = None,
) -> NormalizedEventEnvelope:
    event_type, event_action, object_id = _descriptor(
        notification, ctx, bridge_action, bot_logins, resolved_event
    )
    occurred_at = (
        resolved_event.created_at
        if resolved_event is not None
        else str(notification.metadata.get("github_event_timestamp") or "") or None
    )
    target = (
        resolved_event.target_login
        if resolved_event is not None
        else str(notification.metadata.get("github_target_login") or "") or None
    )
    identity = canonical_event_identity(
        repository=ctx.repo,
        thread=ctx.issue_number,
        event_type=event_type,
        event_action=event_action,
        object_id=object_id,
        occurred_at=occurred_at,
        actor=trigger_actor.login if trigger_actor else None,
        target=target,
    ) or f"{source}:{source_key}"
    return NormalizedEventEnvelope(
        repository=ctx.repo,
        thread=ctx.issue_number,
        subject_type=_subject_type(ctx),
        event_type=event_type,
        event_action=("created" if event_action == "submitted" else event_action),
        actor=trigger_actor.login if trigger_actor else None,
        target=target,
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
