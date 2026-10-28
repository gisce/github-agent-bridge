import subprocess
from datetime import datetime, timezone

import pytest

from github_agent_bridge.cli import build_parser, main
from github_agent_bridge.webhook_audit import (
    REQUIRED_WEBHOOK_EVENTS,
    audit_organization_webhook,
)


NOW = datetime(2026, 10, 28, 20, 0, tzinfo=timezone.utc)
ENDPOINT = "https://bridge.example/api/webhooks/github"


def hook(**overrides):
    value = {
        "id": 123,
        "active": True,
        "events": sorted(REQUIRED_WEBHOOK_EVENTS),
        "config": {
            "url": ENDPOINT,
            "content_type": "json",
            "insecure_ssl": "0",
        },
    }
    value.update(overrides)
    return value


def api_for(hooks, deliveries):
    def api(path):
        if path == "/orgs/gisce/hooks?per_page=100":
            return hooks
        if path == "/orgs/gisce/hooks/123/deliveries?per_page=100":
            return deliveries
        raise AssertionError(path)

    return api


def test_audit_webhook_accepts_secure_active_hook_with_recent_success():
    report = audit_organization_webhook(
        "gisce",
        ENDPOINT,
        max_delivery_age_seconds=3600,
        failure_threshold=3,
        api=api_for(
            [hook()],
            [{"delivered_at": "2026-10-28T19:55:00Z", "status_code": 204}],
        ),
        now=NOW,
    )

    assert report.ok is True
    assert report.hook_id == 123
    assert report.metrics["latest_delivery_age_seconds"] == 300
    assert report.metrics["consecutive_failures"] == 0


def test_audit_webhook_reports_configuration_drift():
    report = audit_organization_webhook(
        "gisce",
        ENDPOINT,
        max_delivery_age_seconds=3600,
        failure_threshold=3,
        api=api_for(
            [hook(
                active=False,
                events=["issue_comment"],
                config={
                    "url": ENDPOINT,
                    "content_type": "form",
                    "insecure_ssl": "1",
                },
            )],
            [{"delivered_at": "2026-10-28T19:55:00Z", "status_code": 200}],
        ),
        now=NOW,
    )

    assert report.ok is False
    assert "webhook is inactive" in report.alerts
    assert "webhook content type is not json" in report.alerts
    assert "webhook TLS verification is disabled" in report.alerts
    assert report.metrics["missing_events"] == sorted(
        REQUIRED_WEBHOOK_EVENTS - {"issue_comment"}
    )


def test_audit_webhook_fails_closed_when_tls_setting_is_missing():
    report = audit_organization_webhook(
        "gisce",
        ENDPOINT,
        max_delivery_age_seconds=3600,
        failure_threshold=3,
        api=api_for(
            [hook(config={"url": ENDPOINT, "content_type": "json"})],
            [{"delivered_at": "2026-10-28T19:55:00Z", "status_code": 204}],
        ),
        now=NOW,
    )

    assert report.ok is False
    assert report.metrics["tls_verify"] is False
    assert "webhook TLS verification setting is missing" in report.alerts


def test_audit_webhook_reports_stale_and_repeated_failed_deliveries():
    report = audit_organization_webhook(
        "gisce",
        ENDPOINT,
        max_delivery_age_seconds=3600,
        failure_threshold=3,
        api=api_for(
            [hook()],
            [
                {"delivered_at": "2026-10-28T18:00:00Z", "status_code": 500},
                {"delivered_at": "2026-10-28T17:59:00Z", "status_code": 502},
                {"delivered_at": "2026-10-28T17:58:00Z", "status_code": 0},
                {"delivered_at": "2026-10-28T17:57:00Z", "status_code": 200},
            ],
        ),
        now=NOW,
    )

    assert report.ok is False
    assert report.metrics["latest_delivery_age_seconds"] == 7200
    assert report.metrics["consecutive_failures"] == 3
    assert any("latest webhook delivery age" in alert for alert in report.alerts)
    assert any("3 consecutive failed deliveries" in alert for alert in report.alerts)


def test_audit_webhook_requires_one_exact_endpoint_match():
    report = audit_organization_webhook(
        "gisce",
        ENDPOINT,
        max_delivery_age_seconds=3600,
        failure_threshold=3,
        api=api_for([], []),
        now=NOW,
    )

    assert report.ok is False
    assert report.metrics["matching_hooks"] == 0
    assert "not found" in report.alerts[0]


def test_audit_webhook_cli_arguments():
    args = build_parser().parse_args([
        "audit-webhook",
        "--owner",
        "gisce",
        "--endpoint",
        ENDPOINT,
        "--json",
    ])

    assert args.owner == "gisce"
    assert args.endpoint == ENDPOINT
    assert args.max_delivery_age_seconds == 21600
    assert args.failure_threshold == 3
    assert args.json is True


@pytest.mark.parametrize(
    ("error", "expected_error"),
    [
        (subprocess.TimeoutExpired(["gh", "api"], 30), "request timed out"),
        (FileNotFoundError(), "GitHub CLI executable not found"),
    ],
)
def test_audit_webhook_cli_reports_gh_execution_errors(
    monkeypatch,
    capsys,
    error,
    expected_error,
):
    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr("github_agent_bridge.webhook_audit.subprocess.run", fail)

    result = main([
        "audit-webhook",
        "--owner",
        "gisce",
        "--endpoint",
        ENDPOINT,
    ])

    assert result == 2
    assert expected_error in capsys.readouterr().err
