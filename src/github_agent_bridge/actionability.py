from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from typing import Any

from .actors import actor_endpoint, default_gh_bin
from .models import GitHubContext


FEEDBACK_TARGET_KINDS = {
    "issue_comment",
    "review_comment",
    "review",
    "commit_comment",
}


@dataclass(frozen=True)
class FeedbackAuthorizationFacts:
    body: str | None
    assignees: tuple[str, ...] = ()
    subject_author: str | None = None
    is_pull_request: bool = False
    complete: bool = True


def first_mentioned_login(body: str | None) -> str | None:
    match = re.search(r"@([A-Za-z0-9-]+)", body or "")
    return match.group(1).lower() if match else None


def evaluate_feedback_authorization(
    facts: FeedbackAuthorizationFacts,
    bot_logins: set[str],
) -> str:
    """Return the positive signal that authorizes feedback dispatch.

    Delivery metadata and issue authorship are deliberately absent: neither is
    proof that a comment was addressed to the bot. Incomplete facts may only be
    revalidated by the executor and never authorize write intent at ingestion.
    """
    configured = {login.lower().lstrip("@") for login in bot_logins}
    if not configured:
        return "ignored"
    if first_mentioned_login(facts.body) in configured:
        return "mentioned"
    if configured.intersection(login.lower() for login in facts.assignees):
        return "assigned"
    author = (facts.subject_author or "").lower()
    if facts.is_pull_request and author in configured:
        return "pr_authored_by_bot"
    return "ignored" if facts.complete else "defer_to_executor"


def feedback_authorization_from_webhook(
    event_name: str,
    payload: dict[str, Any],
    source: dict[str, Any],
    bot_logins: set[str],
) -> str:
    if event_name not in {
        "issue_comment",
        "pull_request_review_comment",
        "pull_request_review",
        "commit_comment",
    }:
        return "not_applicable"
    pull_request = (
        payload.get("pull_request")
        if isinstance(payload.get("pull_request"), dict)
        else None
    )
    issue = payload.get("issue") if isinstance(payload.get("issue"), dict) else None
    pr_issue_comment = bool(
        event_name == "issue_comment"
        and issue
        and isinstance(issue.get("pull_request"), dict)
    )
    subject = pull_request or issue or {}
    author = subject.get("user") if isinstance(subject.get("user"), dict) else {}
    assignees = (
        subject.get("assignees")
        if isinstance(subject.get("assignees"), list)
        else []
    )
    assignee = subject.get("assignee")
    if isinstance(assignee, dict):
        assignees = [*assignees, assignee]
    author_login = str(author.get("login") or "").lower()
    return evaluate_feedback_authorization(
        FeedbackAuthorizationFacts(
            body=str(source.get("body") or ""),
            assignees=tuple(
                str(item.get("login") or "").lower()
                for item in assignees
                if isinstance(item, dict) and item.get("login")
            ),
            subject_author=author_login or None,
            is_pull_request=bool(pull_request or pr_issue_comment),
            complete=not (pr_issue_comment and not author_login),
        ),
        bot_logins,
    )


def _github_json(endpoint: str, *, gh_bin: str, timeout: int) -> dict[str, Any] | None:
    try:
        proc = subprocess.run(
            [gh_bin, "api", endpoint],
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
        payload = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def resolve_feedback_authorization(
    ctx: GitHubContext,
    bot_logins: set[str],
    *,
    notification_body: str | None = None,
    gh_bin: str | None = None,
    timeout: int = 5,
) -> str:
    """Resolve transport-neutral feedback facts for an email receipt.

    A first explicit mention is already a positive signal. Other cases require
    live GitHub facts; the email footer and ``X-GitHub-Reason`` are never used.
    """
    configured = {login.lower().lstrip("@") for login in bot_logins}
    if first_mentioned_login(notification_body) in configured:
        return "mentioned"
    if ctx.target_kind not in FEEDBACK_TARGET_KINDS or not ctx.repo:
        return "not_applicable"

    binary = gh_bin or default_gh_bin()
    source_endpoint = actor_endpoint(ctx)
    source = (
        _github_json(source_endpoint, gh_bin=binary, timeout=timeout)
        if source_endpoint
        else None
    )
    thread = (
        _github_json(
            f"repos/{ctx.repo}/issues/{ctx.issue_number}",
            gh_bin=binary,
            timeout=timeout,
        )
        if ctx.issue_number
        else None
    )
    subject_author = (
        thread.get("user") if isinstance(thread, dict) else None
    )
    assignees = (
        thread.get("assignees")
        if isinstance(thread, dict) and isinstance(thread.get("assignees"), list)
        else []
    )
    return evaluate_feedback_authorization(
        FeedbackAuthorizationFacts(
            body=(str(source.get("body") or "") if source is not None else None),
            assignees=tuple(
                str(item.get("login") or "").lower()
                for item in assignees
                if isinstance(item, dict) and item.get("login")
            ),
            subject_author=(
                str(subject_author.get("login") or "").lower()
                if isinstance(subject_author, dict)
                else None
            ),
            is_pull_request=bool(
                isinstance(thread, dict)
                and isinstance(thread.get("pull_request"), dict)
            ),
            complete=source is not None and (thread is not None or not ctx.issue_number),
        ),
        bot_logins,
    )
