from __future__ import annotations

import json
from types import SimpleNamespace

from github_agent_bridge.actionability import (
    FeedbackAuthorizationFacts,
    evaluate_feedback_authorization,
    feedback_authorization_from_webhook,
    resolve_feedback_authorization,
)
from github_agent_bridge.models import GitHubContext


def test_issue_authorship_and_delivery_reason_do_not_authorize_feedback():
    assert evaluate_feedback_authorization(
        FeedbackAuthorizationFacts(
            body="Migration safety gap. You are receiving this because you were assigned.",
            subject_author="giscebot",
            is_pull_request=False,
        ),
        {"giscebot"},
    ) == "ignored"


def test_pull_request_authorship_is_a_positive_signal():
    assert evaluate_feedback_authorization(
        FeedbackAuthorizationFacts(
            body="Please address the migration safety gap.",
            subject_author="giscebot",
            is_pull_request=True,
        ),
        {"giscebot"},
    ) == "pr_authored_by_bot"


def test_first_mention_must_address_the_configured_bot():
    assert evaluate_feedback_authorization(
        FeedbackAuthorizationFacts(body="@marc ask @giscebot to check this"),
        {"giscebot"},
    ) == "ignored"
    assert evaluate_feedback_authorization(
        FeedbackAuthorizationFacts(body="@giscebot check this with @marc"),
        {"giscebot"},
    ) == "mentioned"


def test_partial_webhook_payload_defers_without_authorizing():
    assert feedback_authorization_from_webhook(
        "issue_comment",
        {
            "issue": {
                "number": 1,
                "pull_request": {"url": "https://api.github.com/pulls/1"},
            }
        },
        {"body": "Please fix this."},
        {"giscebot"},
    ) == "defer_to_executor"


def test_email_guard_uses_verified_comment_and_thread_facts(monkeypatch):
    responses = {
        "repos/gisce/example/issues/comments/6045089117": {
            "body": "I found a migration safety gap.",
        },
        "repos/gisce/example/issues/260": {
            "user": {"login": "giscebot"},
            "assignees": [],
        },
    }

    def fake_run(args, **kwargs):
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps(responses[args[-1]]),
            stderr="",
        )

    monkeypatch.setattr("github_agent_bridge.actionability.subprocess.run", fake_run)
    ctx = GitHubContext(
        urls=[
            "https://github.com/gisce/example/issues/260#issuecomment-6045089117"
        ],
        repo="gisce/example",
        issue_number=260,
        comment_id=6045089117,
        target_kind="issue_comment",
    )

    assert resolve_feedback_authorization(
        ctx,
        {"giscebot"},
        notification_body=(
            "I found a migration safety gap.\n"
            "You are receiving this because you were assigned."
        ),
    ) == "ignored"


def test_email_only_mention_does_not_authorize_feedback(monkeypatch):
    responses = {
        "repos/gisce/example/issues/comments/6045089117": {
            "body": "I found a migration safety gap.",
        },
        "repos/gisce/example/issues/260": {
            "user": {"login": "ecarreras"},
            "assignees": [],
        },
    }

    def fake_run(args, **kwargs):
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps(responses[args[-1]]),
            stderr="",
        )

    monkeypatch.setattr("github_agent_bridge.actionability.subprocess.run", fake_run)
    ctx = GitHubContext(
        urls=[
            "https://github.com/gisce/example/issues/260#issuecomment-6045089117"
        ],
        repo="gisce/example",
        issue_number=260,
        comment_id=6045089117,
        target_kind="issue_comment",
    )

    assert resolve_feedback_authorization(
        ctx,
        {"giscebot"},
        notification_body=(
            "@giscebot fix this.\n"
            "You are receiving this because you were assigned."
        ),
    ) == "ignored"


def test_incomplete_source_lookup_defers_even_when_thread_is_bot_assigned(
    monkeypatch,
):
    def fake_run(args, **kwargs):
        if args[-1] == "repos/gisce/example/issues/comments/6045089117":
            return SimpleNamespace(returncode=1, stdout="", stderr="unavailable")
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                {
                    "user": {"login": "ecarreras"},
                    "assignees": [{"login": "giscebot"}],
                }
            ),
            stderr="",
        )

    monkeypatch.setattr("github_agent_bridge.actionability.subprocess.run", fake_run)
    ctx = GitHubContext(
        urls=[
            "https://github.com/gisce/example/issues/260#issuecomment-6045089117"
        ],
        repo="gisce/example",
        issue_number=260,
        comment_id=6045089117,
        target_kind="issue_comment",
    )

    assert resolve_feedback_authorization(ctx, {"giscebot"}) == "defer_to_executor"
