from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import Notification, utc_now
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
    if event_name in {"issues", "pull_request"} and action == "assigned":
        target_name = "issue" if event_name == "issues" else "pull_request"
        assignment_target = payload.get(target_name) or {}
        assignment_target_id = (
            assignment_target.get("id") if isinstance(assignment_target, dict) else None
        )
        assignee = payload.get("assignee") or {}
        assignee_login = str(assignee.get("login") or "").lower() if isinstance(assignee, dict) else ""
        if assignment_target_id and assignee_login:
            return f"{event_name}:assigned:{repo}:{assignment_target_id}:{assignee_login}"
    if event_name == "pull_request" and action == "review_requested":
        pull_request = payload.get("pull_request") or {}
        pr_id = pull_request.get("id") if isinstance(pull_request, dict) else None
        reviewer = payload.get("requested_reviewer") or {}
        requested_login = str(reviewer.get("login") or "").lower() if isinstance(reviewer, dict) else ""
        if pr_id and requested_login:
            return f"pull_request:review_requested:{repo}:{pr_id}:{requested_login}"
    if event_name == "pull_request" and action == "closed":
        pull_request = payload.get("pull_request") or {}
        merged = pull_request.get("merged") if isinstance(pull_request, dict) else False
        number = pull_request.get("number") if isinstance(pull_request, dict) else None
        if merged and number:
            return f"pull_request:merged:{repo}:{number}"
    if event_name == "workflow_run":
        run = payload.get("workflow_run") or {}
        run_id = run.get("id")
        if run_id and action:
            canonical_action = (
                "workflow_run_failed"
                if action == "completed" and str(run.get("conclusion") or "").lower() == "failure"
                else action
            )
            return f"workflow_run:{canonical_action}:{repo}:{run_id}"
    return None


COMMENT_EVENT_NAMES = {
    "issue_comment",
    "pull_request_review_comment",
    "pull_request_review",
    "commit_comment",
}


def _first_mentioned_login(body: str) -> str | None:
    match = re.search(r"@([A-Za-z0-9-]+)", body or "")
    return match.group(1).lower() if match else None


def _feedback_actionability(
    event_name: str,
    payload: dict[str, Any],
    source: dict[str, Any],
    configured_logins: set[str],
) -> str:
    """Authorize feedback from structured GitHub fields, never delivery reason."""
    if event_name not in COMMENT_EVENT_NAMES:
        return "not_applicable"
    if not configured_logins:
        return "ignored"
    first_mention = _first_mentioned_login(str(source.get("body") or ""))
    if first_mention in configured_logins:
        return "mentioned"
    pull_request = payload.get("pull_request") if isinstance(payload.get("pull_request"), dict) else None
    issue = payload.get("issue") if isinstance(payload.get("issue"), dict) else None
    subject = pull_request or issue or {}
    if not isinstance(subject, dict):
        return "ignored"
    pr_issue_comment = bool(
        event_name == "issue_comment"
        and issue
        and isinstance(issue.get("pull_request"), dict)
    )
    author: dict[str, Any] = {}
    if pull_request:
        author = pull_request.get("user") if isinstance(pull_request.get("user"), dict) else {}
    elif pr_issue_comment and issue:
        # GitHub represents PR conversation comments as issue_comment payloads.
        # In that shape issue.user is the PR author.
        author = issue.get("user") if isinstance(issue.get("user"), dict) else {}
    author_login = str(author.get("login") or "").lower()
    if author_login in configured_logins:
        return "pr_authored_by_bot"
    assignees = subject.get("assignees") if isinstance(subject.get("assignees"), list) else []
    assignee = subject.get("assignee") if isinstance(subject.get("assignee"), dict) else None
    if assignee:
        assignees = [*assignees, assignee]
    if any(
        isinstance(item, dict)
        and str(item.get("login") or "").lower() in configured_logins
        for item in assignees
    ):
        return "assigned"
    if pr_issue_comment and not author_login:
        # Partial payloads must reach the executor, which resolves PR authorship
        # live before allowing work.
        return "defer_to_executor"
    return "ignored"


def webhook_notification(
    event_name: str,
    delivery_id: str,
    payload: dict[str, Any],
    *,
    bot_logins: set[str] | None = None,
    received_at: str | None = None,
) -> Notification | None:
    """Translate actionable webhook payloads into the transport-neutral queue input."""
    received_at = received_at or utc_now()
    action = str(payload.get("action") or "")
    if (event_name, action) not in {
        ("issue_comment", "created"),
        ("issues", "assigned"),
        ("pull_request_review_comment", "created"),
        ("pull_request_review", "submitted"),
        ("pull_request", "review_requested"),
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
        or (subject if action == "assigned" else None)
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
    if event_name == "workflow_run":
        conclusion = str(source.get("conclusion") or "").lower()
        if conclusion != "failure":
            return None
    if event_name == "pull_request_review":
        review_state = str(source.get("state") or "").lower()
        if review_state not in {"changes_requested", "commented"}:
            return None
    feedback_actionability = _feedback_actionability(
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
    elif action == "assigned":
        assignee = payload.get("assignee") if isinstance(payload.get("assignee"), dict) else {}
        target_label = "pull request" if event_name == "pull_request" else "issue"
        body = f"Assigned @{str(assignee.get('login') or '')} to this {target_label}."
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
                {"review_state": str(source.get("state") or "").lower()}
                if event_name == "pull_request_review"
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
    }:
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
) -> ShadowReceipt:
    payload = json.loads(raw_payload)
    action = str(payload.get("action") or "") or None
    repository = payload.get("repository") or {}
    repo = str(repository.get("full_name") or "") or None
    event_key = canonical_webhook_event_key(event_name, payload)
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
