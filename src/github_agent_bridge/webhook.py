from __future__ import annotations

import hashlib
import hmac
import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .actors import default_gh_bin, resolve_github_event_occurrence
from .actionability import feedback_authorization_from_webhook
from .models import Notification, ResolvedGitHubEvent, utc_now
from .ingestion_contract import canonical_event_identity
from .parser import classify_github_action
from .persistence import Database, WebhookRepository


@dataclass(frozen=True)
class ShadowReceipt:
    delivery_id: str
    event_name: str
    action: str | None
    event_key: str | None
    repository: str | None
    status: str
    enqueue_status: str | None
    job_id: int | None


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


def bot_is_pull_request_reviewer(
    payload: dict[str, Any],
    bot_logins: set[str],
    *,
    gh_bin: str | None = None,
    timeout: int = 5,
) -> bool | None:
    """Return whether a configured bot has an active PR review request.

    The signed synchronize payload proves current outstanding review requests.
    Completed reviews disappear from that list, so the issue timeline remains
    the source of truth for the most recent request/removal transition. ``None``
    means the paginated lookup could not be completed and callers must fail
    closed instead of dispatching an unauthorised review.
    """
    configured = {
        str(login).lower().lstrip("@")
        for login in bot_logins
        if str(login).strip()
    }
    if not configured:
        return False
    pull_request = payload.get("pull_request")
    if not isinstance(pull_request, dict):
        return False
    requested_reviewers = pull_request.get("requested_reviewers")
    if isinstance(requested_reviewers, list) and any(
        str(reviewer.get("login") or "").lower() in configured
        for reviewer in requested_reviewers
        if isinstance(reviewer, dict)
    ):
        return True
    repository = payload.get("repository")
    repo = (
        str(repository.get("full_name") or "").lower()
        if isinstance(repository, dict)
        else ""
    )
    number = pull_request.get("number")
    if not repo or not isinstance(number, int) or number <= 0:
        return False
    endpoint = f"repos/{repo}/issues/{number}/events?per_page=100"
    try:
        proc = subprocess.run(
            [
                gh_bin or default_gh_bin(),
                "api",
                "--paginate",
                "--slurp",
                endpoint,
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    try:
        pages = json.loads(proc.stdout or "[]")
    except json.JSONDecodeError:
        return None
    if not isinstance(pages, list) or any(
        not isinstance(page, list) for page in pages
    ):
        return None
    latest: dict[str, tuple[int, bool]] = {}
    for page in pages:
        for event in page:
            if not isinstance(event, dict):
                continue
            action = str(event.get("event") or "").lower()
            if action not in {"review_requested", "review_request_removed"}:
                continue
            reviewer = event.get("requested_reviewer")
            login = (
                str(reviewer.get("login") or "").lower()
                if isinstance(reviewer, dict)
                else ""
            )
            if login not in configured:
                continue
            event_id = event.get("id")
            if not isinstance(event_id, int) or event_id <= 0:
                return None
            current = latest.get(login)
            if current is None or event_id > current[0]:
                latest[login] = (event_id, action == "review_requested")
    return any(active for _, active in latest.values())


def _pull_request_head_sha(payload: dict[str, Any]) -> str | None:
    pull_request = payload.get("pull_request")
    head = pull_request.get("head") if isinstance(pull_request, dict) else None
    value = str(
        payload.get("after")
        or (head.get("sha") if isinstance(head, dict) else "")
        or ""
    ).lower()
    return value if re.fullmatch(r"[0-9a-f]{40,64}", value) else None


def canonical_webhook_event_key(
    event_name: str,
    payload: dict[str, Any],
    *,
    source_key: str | None = None,
    resolved_event: ResolvedGitHubEvent | None = None,
) -> str | None:
    """Return an immutable event key, or a webhook receipt fallback when known.

    State-change payloads use a timeline event ID only after unique API
    resolution. Reusing thread state as identity would drop later assignments
    or review requests, so unresolved actions remain receipt-scoped.
    """
    fallback = f"webhook:{source_key}" if source_key else None
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
        return canonical_event_identity(
            repository=repo,
            thread=None,
            event_type=event_name,
            event_action=action,
            object_id=target_id,
        )
    if event_name in {"issues", "pull_request"} and action == "assigned":
        target_name = "issue" if event_name == "issues" else "pull_request"
        assignment_target = payload.get(target_name) or {}
        number = assignment_target.get("number") if isinstance(assignment_target, dict) else None
        assignee = payload.get("assignee") or {}
        assignee_login = str(assignee.get("login") or "").lower() if isinstance(assignee, dict) else ""
        return canonical_event_identity(
            repository=repo,
            thread=number,
            event_type=event_name,
            event_action=action,
            occurrence_id=(
                resolved_event.event_id if resolved_event is not None else None
            ),
            occurred_at=_webhook_event_timestamp(event_name, action, assignment_target),
            actor=(
                resolved_event.actor.login
                if resolved_event is not None and resolved_event.actor is not None
                else None
            ),
            target=(
                resolved_event.target_login
                if resolved_event is not None
                else assignee_login
            ),
        ) or (fallback if number and assignee_login else None)
    if event_name == "pull_request" and action == "review_requested":
        pull_request = payload.get("pull_request") or {}
        number = pull_request.get("number") if isinstance(pull_request, dict) else None
        reviewer = payload.get("requested_reviewer") or {}
        requested_login = str(reviewer.get("login") or "").lower() if isinstance(reviewer, dict) else ""
        return canonical_event_identity(
            repository=repo,
            thread=number,
            event_type=event_name,
            event_action=action,
            occurrence_id=(
                resolved_event.event_id if resolved_event is not None else None
            ),
            occurred_at=_webhook_event_timestamp(event_name, action, pull_request),
            actor=(
                resolved_event.actor.login
                if resolved_event is not None and resolved_event.actor is not None
                else None
            ),
            target=(
                resolved_event.target_login
                if resolved_event is not None
                else requested_login
            ),
        ) or (fallback if number and requested_login else None)
    if event_name == "pull_request" and action == "synchronize":
        pull_request = payload.get("pull_request") or {}
        number = pull_request.get("number") if isinstance(pull_request, dict) else None
        head_sha = _pull_request_head_sha(payload)
        return canonical_event_identity(
            repository=repo,
            thread=number,
            event_type=event_name,
            event_action=action,
            object_id=head_sha,
        )
    if event_name == "pull_request" and action == "closed":
        pull_request = payload.get("pull_request") or {}
        merged = pull_request.get("merged") if isinstance(pull_request, dict) else False
        number = pull_request.get("number") if isinstance(pull_request, dict) else None
        if merged:
            return canonical_event_identity(
                repository=repo,
                thread=number,
                event_type="pull_request",
                event_action="merged",
            )
    if event_name == "issues" and action in {"closed", "reopened"}:
        issue = payload.get("issue") or {}
        number = issue.get("number") if isinstance(issue, dict) else None
        return canonical_event_identity(
            repository=repo,
            thread=number,
            event_type=event_name,
            event_action=action,
            occurrence_id=(
                resolved_event.event_id if resolved_event is not None else None
            ),
            occurred_at=_webhook_event_timestamp(event_name, action, issue),
            actor=(
                resolved_event.actor.login
                if resolved_event is not None and resolved_event.actor is not None
                else None
            ),
        ) or (fallback if number else None)
    if event_name == "workflow_run":
        run = payload.get("workflow_run") or {}
        run_id = run.get("id")
        if run_id and action:
            canonical_action = (
                "workflow_run_failed"
                if action == "completed" and str(run.get("conclusion") or "").lower() == "failure"
                else action
            )
            return canonical_event_identity(
                repository=repo,
                thread=None,
                event_type="workflow_run",
                event_action=canonical_action,
                object_id=run_id,
            )
    return None


def _webhook_event_timestamp(
    event_name: str,
    action: str,
    source: dict[str, Any],
) -> str | None:
    if event_name == "pull_request" and action == "closed":
        value = source.get("merged_at")
    elif event_name == "issues" and action == "closed":
        value = source.get("closed_at")
    elif action in {"assigned", "review_requested", "reopened"}:
        value = source.get("updated_at")
    elif event_name == "pull_request_review":
        value = source.get("submitted_at")
    else:
        value = source.get("created_at") or source.get("updated_at")
    return str(value) if value else None


def resolve_github_event_for_webhook(
    event_name: str,
    payload: dict[str, Any],
    *,
    gh_bin: str | None = None,
    timeout: int = 5,
) -> ResolvedGitHubEvent | None:
    """Resolve a webhook state transition only when one timeline event matches."""
    action = str(payload.get("action") or "").lower()
    if (event_name, action) not in {
        ("issues", "assigned"),
        ("issues", "closed"),
        ("issues", "reopened"),
        ("pull_request", "assigned"),
        ("pull_request", "review_requested"),
    }:
        return None
    repository = payload.get("repository")
    repo = (
        str(repository.get("full_name") or "").lower()
        if isinstance(repository, dict)
        else ""
    )
    target_name = "issue" if event_name == "issues" else "pull_request"
    subject = payload.get(target_name)
    if not isinstance(subject, dict):
        return None
    number = subject.get("number")
    sender = payload.get("sender")
    actor_login = (
        str(sender.get("login") or "")
        if isinstance(sender, dict)
        else ""
    )
    target = None
    if action == "assigned":
        target = payload.get("assignee")
    elif action == "review_requested":
        target = payload.get("requested_reviewer")
    target_login = (
        str(target.get("login") or "")
        if isinstance(target, dict)
        else None
    )
    return resolve_github_event_occurrence(
        repository=repo,
        thread=number if isinstance(number, int) else None,
        event=action,
        occurred_at=_webhook_event_timestamp(event_name, action, subject),
        actor_login=actor_login,
        target_login=target_login,
        gh_bin=gh_bin,
        timeout=timeout,
    )


COMMENT_EVENT_NAMES = {
    "issue_comment",
    "pull_request_review_comment",
    "pull_request_review",
    "commit_comment",
}


def webhook_notification(
    event_name: str,
    delivery_id: str,
    payload: dict[str, Any],
    *,
    bot_logins: set[str] | None = None,
    received_at: str | None = None,
    rereview_authorized: bool = False,
) -> Notification | None:
    """Translate actionable webhook payloads into the transport-neutral queue input."""
    received_at = received_at or utc_now()
    action = str(payload.get("action") or "")
    if (event_name, action) not in {
        ("issue_comment", "created"),
        ("issues", "assigned"),
        ("issues", "closed"),
        ("issues", "reopened"),
        ("pull_request_review_comment", "created"),
        ("pull_request_review", "submitted"),
        ("pull_request", "review_requested"),
        ("pull_request", "synchronize"),
        ("pull_request", "assigned"),
        ("pull_request", "closed"),
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
        payload.get("comment")
        or payload.get("review")
        or payload.get("workflow_run")
        or (payload.get("pull_request") if event_name == "pull_request" else None)
        or subject
        or {}
    )
    if not isinstance(source, dict):
        return None
    configured_logins = {login.lower().lstrip("@") for login in (bot_logins or set())}
    if event_name == "pull_request" and action == "closed":
        if not bool(source.get("merged")):
            return None
    if action == "assigned":
        assignee = payload.get("assignee") if isinstance(payload.get("assignee"), dict) else {}
        assignee_login = str(assignee.get("login") or "").lower()
        if assignee_login not in configured_logins:
            return None
    if event_name == "pull_request" and action == "review_requested":
        reviewer = payload.get("requested_reviewer") if isinstance(payload.get("requested_reviewer"), dict) else {}
        requested_login = str(reviewer.get("login") or "").lower()
        if requested_login not in configured_logins:
            return None
    if event_name == "pull_request" and action == "synchronize":
        if not rereview_authorized or _pull_request_head_sha(payload) is None:
            return None
    if event_name == "workflow_run":
        conclusion = str(source.get("conclusion") or "").lower()
        if conclusion != "failure":
            return None
    if event_name == "pull_request_review":
        review_state = str(source.get("state") or "").lower()
        if review_state not in {"changes_requested", "commented"}:
            return None
    feedback_actionability = feedback_authorization_from_webhook(
        event_name,
        payload,
        source,
        configured_logins,
    )
    if feedback_actionability == "ignored":
        return None
    url = str(source.get("html_url") or subject.get("html_url") or repository.get("html_url") or "")
    pull_request = subject.get("pull_request") if isinstance(subject, dict) else None
    if event_name == "issue_comment" and isinstance(pull_request, dict) and number:
        # GitHub reports PR issue comments with an /issues/ HTML URL. Preserve
        # the comment identity but normalize the target so downstream code can
        # distinguish PR jobs (which have a head SHA) from plain issues.
        comment_id = source.get("id")
        url = f"https://github.com/{repo}/pull/{number}"
        if comment_id:
            url += f"#issuecomment-{comment_id}"
    if not url.startswith("https://github.com/"):
        return None
    if event_name == "workflow_run":
        body = "Workflow run failed (conclusion: failure)."
    elif event_name == "pull_request" and action == "closed":
        base = source.get("base") if isinstance(source.get("base"), dict) else {}
        body = f"Merged #{number} into {str(base.get('ref') or 'the base branch')}."
    elif event_name == "issues" and action in {"closed", "reopened"}:
        body = "Closed this issue." if action == "closed" else "Reopened this issue."
    elif action == "assigned":
        assignee = payload.get("assignee") if isinstance(payload.get("assignee"), dict) else {}
        target_label = "pull request" if event_name == "pull_request" else "issue"
        body = f"Assigned @{str(assignee.get('login') or '')} to this {target_label}."
    elif event_name == "pull_request" and action == "synchronize":
        body = "The pull request head changed. Review requested."
    elif event_name == "pull_request":
        body = "Review requested."
    else:
        body = str(source.get("body") or "")
    sender = payload.get("sender") if isinstance(payload.get("sender"), dict) else {}
    login = str(sender.get("login") or "GitHub")
    title = str(subject.get("title") or subject.get("name") or event_name)
    suffix = f" (#{number})" if number else ""
    message_id = f"<{delivery_id}@github.com>"
    if event_name == "pull_request" and action == "closed":
        message_id = f"<{repo}/pull/{number}/merged@github.com>"
    notification = Notification(
        uid=None,
        message_id=message_id,
        subject=f"[{repo}] {title}{suffix}",
        from_addr=f"{login} <notifications@github.com>",
        body=f"{body}\n\n{url}",
        received_at=received_at,
        source_received_at=received_at,
        auth={"spf": True, "dkim": True, "dmarc": True},
        metadata={
            "github_event": event_name,
            "github_action": action,
            "feedback_actionability": feedback_actionability,
            **(
                {
                    "github_target_login": str(
                        (payload.get("assignee") or {}).get("login") or ""
                    ).lower()
                }
                if action == "assigned"
                else {
                    "github_target_login": str(
                        (payload.get("requested_reviewer") or {}).get("login") or ""
                    ).lower()
                }
                if event_name == "pull_request" and action == "review_requested"
                else {}
            ),
            **(
                {"github_event_timestamp": event_timestamp}
                if (
                    event_timestamp := _webhook_event_timestamp(
                        event_name, action, source
                    )
                )
                else {}
            ),
            **(
                {"review_state": str(source.get("state") or "").lower()}
                if event_name == "pull_request_review"
                else {}
            ),
            **(
                {"github_head_sha": _pull_request_head_sha(payload)}
                if event_name == "pull_request" and action == "synchronize"
                else {}
            ),
        },
    )
    if event_name != "workflow_run" and classify_github_action(
        notification.subject,
        notification.body,
        bot_logins,
        message_id=notification.message_id,
    ) == "archive_notification" and feedback_actionability in {
        "ignored",
        "not_applicable",
    } and not (event_name == "issues" and action in {"closed", "reopened"}):
        return None
    return notification


def webhook_hook_target(payload: dict[str, Any]) -> tuple[str, str]:
    hook = payload.get("hook") if isinstance(payload, dict) else None
    repository = payload.get("repository") if isinstance(payload, dict) else None
    organization = payload.get("organization") if isinstance(payload, dict) else None
    repository_name = str(repository.get("full_name") or "") if isinstance(repository, dict) else ""
    organization_name = str(organization.get("login") or "") if isinstance(organization, dict) else ""
    hook_url = str(hook.get("url") or "") if isinstance(hook, dict) else ""

    if "/repos/" in hook_url and repository_name:
        return repository_name, "repository"
    if "/orgs/" in hook_url and organization_name:
        return organization_name, "organization"
    if repository_name:
        return repository_name, "repository"
    if organization_name:
        return organization_name, "organization"
    return "unknown", "repository"


def persist_shadow_delivery(
    db: str | Path,
    *,
    delivery_id: str,
    event_name: str,
    raw_payload: bytes,
    hook_id: str | None = None,
    retention_days: int = 30,
    enqueue_status: str | None = None,
    job_id: int | None = None,
    resolved_event: ResolvedGitHubEvent | None = None,
) -> ShadowReceipt:
    payload = json.loads(raw_payload)
    action = str(payload.get("action") or "") or None
    repository = payload.get("repository") or {}
    repo = str(repository.get("full_name") or "") or None
    event_key = canonical_webhook_event_key(
        event_name,
        payload,
        source_key=delivery_id,
        resolved_event=resolved_event,
    )
    status = "observed" if event_key else "unsupported"
    payload_hash = hashlib.sha256(raw_payload).hexdigest()
    now = utc_now()
    stored = WebhookRepository(Database(db)).persist_delivery(
        delivery_id=delivery_id,
        hook_id=hook_id,
        event_name=event_name,
        action=action,
        event_key=event_key,
        repository=repo,
        payload_hash=payload_hash,
        payload_json=raw_payload.decode("utf-8"),
        payload=payload,
        status=status,
        enqueue_status=enqueue_status,
        job_id=job_id,
        retention_days=retention_days,
        created_at=now,
        hook_target=webhook_hook_target(payload) if hook_id else None,
    )
    return ShadowReceipt(
        stored.delivery_id,
        stored.event_name,
        stored.action,
        stored.event_key,
        stored.repository,
        stored.status,
        stored.enqueue_status,
        stored.job_id,
    )
