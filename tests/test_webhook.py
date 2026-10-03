from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from github_agent_bridge import backend
from github_agent_bridge.backend import DashboardConfig, _encode_session, _sign, create_app


SECRET = "test-secret"


def signed_headers(payload: bytes, *, delivery: str = "delivery-1", event: str = "issue_comment", secret: str = SECRET) -> dict[str, str]:
    digest = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    return {
        "content-type": "application/json",
        "x-github-delivery": delivery,
        "x-github-event": event,
        "x-hub-signature-256": f"sha256={digest}",
    }


def hook_headers(payload: bytes, *, delivery: str, event: str, hook_id: str) -> dict[str, str]:
    return {**signed_headers(payload, delivery=delivery, event=event), "x-github-hook-id": hook_id}


def issue_comment_payload(*, action: str = "created") -> bytes:
    return json.dumps({
        "action": action,
        "repository": {"full_name": "gisce/github-agent-bridge"},
        "comment": {"id": 5948901951},
    }).encode()


def actionable_issue_comment_payload(*, comment_id: int = 5948901951) -> bytes:
    return json.dumps({
        "action": "created",
        "repository": {"full_name": "gisce/github-agent-bridge"},
        "issue": {
            "number": 191,
            "title": "Evaluate GitHub App webhooks",
            "html_url": "https://github.com/gisce/github-agent-bridge/issues/191",
        },
        "comment": {
            "id": comment_id,
            "body": "@giscebot implement this",
            "html_url": f"https://github.com/gisce/github-agent-bridge/issues/191#issuecomment-{comment_id}",
        },
        "sender": {"login": "ecarreras"},
    }).encode()


def canary_policy(tmp_path):
    path = tmp_path / "policy.json"
    path.write_text(json.dumps({
        "trustedOrgs": ["gisce"],
        "enabledRepos": ["gisce/github-agent-bridge"],
        "botLogins": ["giscebot"],
        "actions": {"trustedAuto": ["reply_comment"]},
    }))
    return path


def test_webhook_shadow_verifies_and_persists_without_creating_job(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    payload = issue_comment_payload()
    client = TestClient(create_app(DashboardConfig(db=db, require_auth=False, webhook_secrets=(SECRET,))))

    response = client.post("/api/webhooks/github", content=payload, headers=signed_headers(payload))

    assert response.status_code == 200
    assert response.json() == {
        "mode": "shadow",
        "status": "observed",
        "event_key": "issue_comment:created:gisce/github-agent-bridge:5948901951",
    }
    with sqlite3.connect(db) as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM github_events").fetchone()[0] == 0
        assert con.execute("SELECT payload_hash FROM webhook_shadow_receipts").fetchone()[0] == hashlib.sha256(payload).hexdigest()


def test_webhook_shadow_rejects_invalid_signature_without_persisting(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    payload = issue_comment_payload()
    headers = signed_headers(payload)
    headers["x-hub-signature-256"] = "sha256=invalid"
    client = TestClient(create_app(DashboardConfig(db=db, require_auth=False, webhook_secrets=(SECRET,))))

    response = client.post("/api/webhooks/github", content=payload, headers=headers)

    assert response.status_code == 401
    assert not db.exists()


def test_webhook_shadow_deduplicates_delivery_and_tracks_edited_separately(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    payload = issue_comment_payload(action="edited")
    client = TestClient(create_app(DashboardConfig(db=db, require_auth=False, webhook_secrets=(SECRET,))))

    first = client.post("/api/webhooks/github", content=payload, headers=signed_headers(payload))
    duplicate = client.post("/api/webhooks/github", content=payload, headers=signed_headers(payload))

    assert first.json()["event_key"] == "issue_comment:edited:gisce/github-agent-bridge:5948901951"
    assert duplicate.json()["status"] == "duplicate"
    status = client.get("/api/webhooks/github/status").json()
    assert status["receipts"] == {"observed": 1}
    assert status["duplicate_deliveries"] == 1
    assert status["cross_source_matches"] == 0


def test_webhook_shadow_accepts_previous_rotation_secret(tmp_path):
    payload = issue_comment_payload()
    client = TestClient(create_app(DashboardConfig(
        db=tmp_path / "bridge.sqlite3",
        require_auth=False,
        webhook_secrets=("new-secret", SECRET),
    )))

    assert client.post("/api/webhooks/github", content=payload, headers=signed_headers(payload)).status_code == 200


def test_webhook_canary_enqueues_enabled_actionable_repository_once(tmp_path):
    payload = actionable_issue_comment_payload()
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3",
        require_auth=False,
        webhook_secrets=(SECRET,),
        webhook_mode="canary",
        webhook_policy=canary_policy(tmp_path),
    )
    client = TestClient(create_app(config))

    first = client.post("/api/webhooks/github", content=payload, headers=signed_headers(payload))
    retry = client.post("/api/webhooks/github", content=payload, headers=signed_headers(payload))

    assert first.json()["mode"] == "canary"
    assert first.json()["enqueue_status"] == "enqueued"
    assert first.json()["job_id"]
    assert retry.json()["enqueue_status"] == "duplicate"
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1
        assert con.execute(
            "SELECT source,event_key,status FROM ingest_receipts"
        ).fetchone() == (
            "webhook",
            "issue_comment:created:gisce/github-agent-bridge:5948901951",
            "accepted",
        )


def test_webhook_canary_ignores_repo_outside_enabled_repos(tmp_path):
    policy = canary_policy(tmp_path)
    policy.write_text(json.dumps({"trustedOrgs": ["gisce"], "enabledRepos": ["gisce/other"]}))
    payload = actionable_issue_comment_payload()
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3", require_auth=False,
        webhook_secrets=(SECRET,), webhook_mode="canary", webhook_policy=policy,
    )

    response = TestClient(create_app(config)).post(
        "/api/webhooks/github", content=payload, headers=signed_headers(payload),
    )

    assert response.json()["enqueue_status"] == "outside_canary"
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0


def test_webhook_shadow_selects_secret_by_repository_owner(tmp_path):
    payload = issue_comment_payload()
    client = TestClient(create_app(DashboardConfig(
        db=tmp_path / "bridge.sqlite3",
        require_auth=False,
        webhook_secrets_by_owner={"gisce": ("gisce-secret",), "example": ("example-secret",)},
    )))

    assert client.post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, secret="gisce-secret"),
    ).status_code == 200
    assert client.post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="wrong-owner-secret", secret="example-secret"),
    ).status_code == 401


def test_webhook_shadow_rejects_unconfigured_repository_owner(tmp_path):
    payload = json.dumps({
        "action": "created",
        "repository": {"full_name": "unknown/repository"},
        "comment": {"id": 1},
    }).encode()
    client = TestClient(create_app(DashboardConfig(
        db=tmp_path / "bridge.sqlite3",
        require_auth=False,
        webhook_secrets_by_owner={"gisce": (SECRET,)},
    )))

    response = client.post("/api/webhooks/github", content=payload, headers=signed_headers(payload))

    assert response.status_code == 403
    assert not (tmp_path / "bridge.sqlite3").exists()


def test_webhook_status_requires_dashboard_admin(tmp_path):
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3",
        secret_key="dashboard-secret",
        allowed_users={"alice"},
        admin_users={"operator"},
        webhook_secrets=(SECRET,),
    )
    client = TestClient(create_app(config))

    paths = (
        "/api/webhooks/github/status",
        "/api/webhooks/github/summary",
        "/api/webhooks/github/timeseries",
        "/api/webhooks/github/hooks",
        "/api/webhooks/github/deliveries",
        "/api/webhooks/github/exceptions",
    )
    assert {client.get(path).status_code for path in paths} == {401}
    assert client.get("/api/webhooks/github/hooks/42").status_code == 401
    client.cookies.set("gab_dashboard_session", _sign(config, _encode_session({"login": "alice"})))
    assert {client.get(path).status_code for path in paths} == {403}
    assert client.get("/api/webhooks/github/hooks/42").status_code == 403
    client.cookies.set("gab_dashboard_session", _sign(config, _encode_session({"login": "operator"}, is_admin=True)))
    assert {client.get(path).status_code for path in paths} == {200}
    assert client.get("/api/webhooks/github/hooks/42").status_code == 404
    assert client.get("/api/status").json()["webhook_configured"] is True


def test_dashboard_status_hides_webhook_tab_when_not_configured(tmp_path):
    client = TestClient(create_app(DashboardConfig(db=tmp_path / "bridge.sqlite3", require_auth=False)))

    assert client.get("/api/status").json()["webhook_configured"] is False


def test_webhook_monitoring_endpoints_keep_summary_light_and_return_real_data(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    client = TestClient(create_app(DashboardConfig(db=db, require_auth=False, webhook_secrets=(SECRET,))))
    ping = json.dumps({
        "zen": "Keep it logically awesome.",
        "hook": {
            "id": 42, "name": "web", "active": True,
            "events": ["issue_comment", "pull_request_review"],
            "config": {
                "content_type": "json", "insecure_ssl": "0",
                "secret": "********", "url": "https://gab.gisce.net/api/webhooks/github",
            },
            "created_at": "2026-10-02T11:07:17Z",
            "updated_at": "2026-10-02T11:07:17Z",
            "url": "https://api.github.com/orgs/gisce/hooks/42",
            "ping_url": "https://api.github.com/orgs/gisce/hooks/42/pings",
            "deliveries_url": "https://api.github.com/orgs/gisce/hooks/42/deliveries",
        },
        "organization": {"login": "gisce"},
    }).encode()
    delivery = issue_comment_payload()

    assert client.post("/api/webhooks/github", content=ping, headers=hook_headers(ping, delivery="ping-1", event="ping", hook_id="42")).status_code == 200
    assert client.post("/api/webhooks/github", content=delivery, headers=hook_headers(delivery, delivery="delivery-1", event="issue_comment", hook_id="42")).status_code == 200
    summary = client.get("/api/webhooks/github/summary").json()
    assert summary == {
        "mode": "shadow", "configured": True,
        "receipts": {"observed": 1, "unsupported": 1},
        "duplicate_deliveries": 0, "cross_source_matches": 0,
        "totals": {"hooks": 1, "deliveries": 2},
        "coverage": {
            "both": 0, "imap_only": 0, "webhook_only": 1,
            "imap_eligible": 0, "ratio": None, "mean_match_delay_ms": None,
        },
    }
    assert client.get("/api/webhooks/github/status").json() == summary

    hooks_response = client.get("/api/webhooks/github/hooks").json()
    assert hooks_response["next_cursor"] is None
    hooks = hooks_response["hooks"]
    assert len(hooks) == 1
    assert hooks[0]["id"] == "42"
    assert hooks[0]["target"] == "gisce"
    assert hooks[0]["target_type"] == "organization"
    assert hooks[0]["name"] == "web"
    assert hooks[0]["active"] is True
    assert hooks[0]["events"] == ["issue_comment", "pull_request_review"]
    assert hooks[0]["content_type"] == "json"
    assert hooks[0]["ssl_verify"] is True
    assert hooks[0]["delivery_url"] == "https://gab.gisce.net/api/webhooks/github"
    assert hooks[0]["admin_url"] == "https://github.com/organizations/gisce/settings/hooks/42"
    assert hooks[0]["last_ping_at"]
    assert hooks[0]["last_event_at"]
    assert hooks[0]["last_delivery_id"] == "delivery-1"
    assert hooks[0]["last_event_name"] == "issue_comment"
    assert hooks[0]["last_action"] == "created"
    assert hooks[0]["last_repository"] == "gisce/github-agent-bridge"
    assert hooks[0]["last_result"] == "observed"
    assert hooks[0]["status"] == "receiving"

    detail = client.get("/api/webhooks/github/hooks/42").json()
    assert detail["hook"] == hooks[0]
    assert detail["stats"] == {"deliveries": 2, "duplicates": 0, "unsupported": 1}
    assert {item["delivery_id"] for item in detail["recent_deliveries"]} == {"ping-1", "delivery-1"}
    assert "secret" not in json.dumps(detail)

    first_page = client.get("/api/webhooks/github/deliveries", params={"limit": 1}).json()
    assert len(first_page["deliveries"]) == 1
    assert first_page["next_cursor"]
    second_page = client.get(
        "/api/webhooks/github/deliveries",
        params={"limit": 1, "cursor": first_page["next_cursor"]},
    ).json()
    deliveries = first_page["deliveries"] + second_page["deliveries"]
    assert {item["delivery_id"] for item in deliveries} == {"ping-1", "delivery-1"}
    assert next(item for item in deliveries if item["delivery_id"] == "delivery-1")["hook_id"] == "42"
    assert next(item for item in deliveries if item["delivery_id"] == "delivery-1")["hook"] == {
        "id": "42", "target": "gisce", "target_type": "organization",
        "admin_url": "https://github.com/organizations/gisce/settings/hooks/42",
    }
    assert second_page["next_cursor"] is None

    filtered = client.get("/api/webhooks/github/deliveries", params={
        "hook_id": "42", "event_name": "issue_comment", "repository": "gisce/github-agent-bridge",
        "result": "observed",
    }).json()
    assert [item["delivery_id"] for item in filtered["deliveries"]] == ["delivery-1"]

    now = datetime.now(UTC)
    timeseries = client.get("/api/webhooks/github/timeseries", params={
        "from": (now - timedelta(days=1)).isoformat(),
        "to": (now + timedelta(days=1)).isoformat(),
        "bucket": "hour",
    }).json()
    assert timeseries["bucket"] == "hour"
    assert timeseries["points"][0]["observed"] == 1
    assert timeseries["points"][0]["unsupported"] == 1
    exceptions = client.get("/api/webhooks/github/exceptions").json()["exceptions"]
    assert {(item["kind"], item["reference"]) for item in exceptions} == {
        ("unmatchable", "ping-1"), ("webhook_only", "delivery-1"),
    }


def test_webhook_monitoring_rejects_unbounded_ranges_and_invalid_cursors(tmp_path):
    client = TestClient(create_app(DashboardConfig(
        db=tmp_path / "bridge.sqlite3", require_auth=False, webhook_secrets=(SECRET,),
    )))

    response = client.get("/api/webhooks/github/timeseries", params={
        "from": "2020-01-01T00:00:00Z", "to": "2022-01-01T00:00:00Z", "bucket": "day",
    })
    assert response.status_code == 400
    assert response.json()["detail"] == "time_range_too_large"
    assert client.get("/api/webhooks/github/deliveries", params={"cursor": "invalid"}).status_code == 400
    assert client.get("/api/webhooks/github/hooks", params={"cursor": "invalid"}).status_code == 400


def test_webhook_hook_inventory_uses_stable_cursor_pagination(tmp_path):
    client = TestClient(create_app(DashboardConfig(
        db=tmp_path / "bridge.sqlite3", require_auth=False, webhook_secrets=(SECRET,),
    )))
    for hook_id in ("41", "42", "43"):
        payload = json.dumps({
            "hook": {"id": int(hook_id), "active": True, "events": ["issue_comment"]},
            "organization": {"login": "gisce"},
        }).encode()
        assert client.post(
            "/api/webhooks/github", content=payload,
            headers=hook_headers(payload, delivery=f"ping-{hook_id}", event="ping", hook_id=hook_id),
        ).status_code == 200

    first = client.get("/api/webhooks/github/hooks", params={"limit": 2}).json()
    second = client.get("/api/webhooks/github/hooks", params={
        "limit": 2, "cursor": first["next_cursor"],
    }).json()

    assert len(first["hooks"]) == 2
    assert first["next_cursor"]
    assert len(second["hooks"]) == 1
    assert second["next_cursor"] is None
    assert {hook["id"] for hook in first["hooks"] + second["hooks"]} == {"41", "42", "43"}


def test_webhook_monitoring_initializes_schema_once_per_app(tmp_path, monkeypatch):
    initializations = 0
    queue_class = backend.JobQueue

    def counting_queue(path):
        nonlocal initializations
        initializations += 1
        return queue_class(path)

    monkeypatch.setattr(backend, "JobQueue", counting_queue)
    payload = issue_comment_payload()
    client = TestClient(create_app(DashboardConfig(
        db=tmp_path / "bridge.sqlite3", require_auth=False, webhook_secrets=(SECRET,),
    )))

    assert client.post("/api/webhooks/github", content=payload, headers=signed_headers(payload)).status_code == 200
    for path in (
        "/api/webhooks/github/summary",
        "/api/webhooks/github/timeseries",
        "/api/webhooks/github/hooks",
        "/api/webhooks/github/deliveries",
    ):
        assert client.get(path).status_code == 200
    assert initializations == 1


def test_webhook_receipt_retention_removes_expired_delivery_details(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    client = TestClient(create_app(DashboardConfig(
        db=db, require_auth=False, webhook_secrets=(SECRET,), webhook_retention_days=7,
    )))
    payload = issue_comment_payload()
    client.post("/api/webhooks/github", content=payload, headers=signed_headers(payload, delivery="old"))
    with sqlite3.connect(db) as con:
        con.execute("UPDATE webhook_shadow_receipts SET created_at='2020-01-01T00:00:00+00:00'")
    client.post("/api/webhooks/github", content=payload, headers=signed_headers(payload, delivery="new"))

    deliveries = client.get("/api/webhooks/github/deliveries").json()["deliveries"]
    assert [item["delivery_id"] for item in deliveries] == ["new"]


def test_existing_webhook_receipt_schema_is_migrated_for_hook_inventory(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    with sqlite3.connect(db) as con:
        con.execute(
            "CREATE TABLE webhook_shadow_receipts (delivery_id TEXT PRIMARY KEY,event_name TEXT NOT NULL,"
            "action TEXT,event_key TEXT,repository TEXT,payload_hash TEXT NOT NULL,status TEXT NOT NULL,"
            "duplicate_count INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL)"
        )
    payload = issue_comment_payload()
    client = TestClient(create_app(DashboardConfig(db=db, require_auth=False, webhook_secrets=(SECRET,))))

    response = client.post(
        "/api/webhooks/github", content=payload,
        headers=hook_headers(payload, delivery="migrated", event="issue_comment", hook_id="84"),
    )

    assert response.status_code == 200
    assert client.get("/api/webhooks/github/hooks").json()["hooks"][0]["id"] == "84"
    with sqlite3.connect(db) as con:
        indexes = {row[1] for row in con.execute("PRAGMA index_list(webhook_shadow_receipts)")}
        hook_columns = {row[1] for row in con.execute("PRAGMA table_info(webhook_hooks)")}
    assert "idx_webhook_shadow_delivery_page" in indexes
    assert {"delivery_url", "last_delivery_id", "last_result"} <= hook_columns


def test_submitted_review_uses_same_canonical_key_as_email_ingestion(tmp_path):
    payload = json.dumps({
        "action": "submitted",
        "repository": {"full_name": "gisce/github-agent-bridge"},
        "review": {"id": 1234},
    }).encode()
    client = TestClient(create_app(DashboardConfig(
        db=tmp_path / "bridge.sqlite3",
        require_auth=False,
        webhook_secrets=(SECRET,),
    )))

    response = client.post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, event="pull_request_review"),
    )

    assert response.json()["event_key"] == "pull_request_review:created:gisce/github-agent-bridge:1234"
