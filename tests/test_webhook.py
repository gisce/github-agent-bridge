from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from github_agent_bridge import backend
from github_agent_bridge.actors import TriggerActor
from github_agent_bridge.backend import DashboardConfig, _encode_session, _sign, create_app, create_webhook_app
from github_agent_bridge.models import Notification
from github_agent_bridge.policy import Policy
from github_agent_bridge.queue import JobQueue


SECRET = "test-secret"


def test_dedicated_webhook_ingress_exposes_only_health_and_delivery(tmp_path):
    client = TestClient(create_webhook_app(DashboardConfig(
        db=tmp_path / "bridge.sqlite3", webhook_secrets=(SECRET,),
    )))
    payload = issue_comment_payload()

    assert client.get("/api/health").json() == {
        "ok": True, "service": "github-agent-bridge-webhook-ingress",
    }
    assert client.post(
        "/api/webhooks/github", content=payload, headers=signed_headers(payload),
    ).json()["status"] == "observed"
    assert client.get("/").status_code == 404
    assert client.get("/api/webhooks/github/summary").status_code == 404


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


def non_actionable_issue_comment_payload(*, comment_id: int = 5948901951) -> bytes:
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
            "body": "Looks good to me.",
            "html_url": f"https://github.com/gisce/github-agent-bridge/issues/191#issuecomment-{comment_id}",
        },
        "sender": {"login": "ecarreras"},
    }).encode()


def pull_request_review_payload(
    *,
    state: str,
    review_id: int = 4815162342,
    sender: str = "pilipilisbot",
) -> bytes:
    return json.dumps({
        "action": "submitted",
        "repository": {"full_name": "gisce/github-agent-bridge"},
        "pull_request": {
            "number": 233,
            "title": "feat: enable guarded webhook canary ingestion",
            "html_url": "https://github.com/gisce/github-agent-bridge/pull/233",
        },
        "review": {
            "id": review_id,
            "state": state,
            "body": "Please address this.",
            "html_url": f"https://github.com/gisce/github-agent-bridge/pull/233#pullrequestreview-{review_id}",
        },
        "sender": {"login": sender},
    }).encode()


def pull_request_review_requested_payload(
    *,
    pr_id: int = 1829195123,
    requested_reviewer: str = "giscebot",
    sender: str = "ecarreras",
    title: str = "feat: enable guarded webhook canary ingestion",
) -> bytes:
    return json.dumps({
        "action": "review_requested",
        "repository": {"full_name": "gisce/github-agent-bridge"},
        "pull_request": {
            "id": pr_id,
            "number": 233,
            "title": title,
            "html_url": "https://github.com/gisce/github-agent-bridge/pull/233",
        },
        "requested_reviewer": {"login": requested_reviewer},
        "sender": {"login": sender},
    }).encode()


def assignment_payload(
    *,
    event_name: str = "issues",
    target_id: int = 1829195190,
    number: int = 190,
    assignee: str = "giscebot",
    sender: str = "ecarreras",
) -> bytes:
    target_name = "pull_request" if event_name == "pull_request" else "issue"
    target_path = "pull" if event_name == "pull_request" else "issues"
    return json.dumps({
        "action": "assigned",
        "repository": {"full_name": "gisce/github-agent-bridge"},
        target_name: {
            "id": target_id,
            "number": number,
            "title": "Introduce versioned SQLite migrations",
            "html_url": f"https://github.com/gisce/github-agent-bridge/{target_path}/{number}",
        },
        "assignee": {"login": assignee},
        "sender": {"login": sender},
    }).encode()


def commit_comment_payload(*, comment_id: int = 778899) -> bytes:
    return json.dumps({
        "action": "created",
        "repository": {
            "full_name": "gisce/github-agent-bridge",
            "html_url": "https://github.com/gisce/github-agent-bridge",
        },
        "comment": {
            "id": comment_id,
            "body": "@giscebot investigate this",
            "html_url": (
                "https://github.com/gisce/github-agent-bridge/commit/abcdef123456"
                f"#commitcomment-{comment_id}"
            ),
        },
        "sender": {"login": "ecarreras"},
    }).encode()


def workflow_run_payload(*, conclusion: str) -> bytes:
    return json.dumps({
        "action": "completed",
        "repository": {"full_name": "gisce/github-agent-bridge"},
        "workflow_run": {
            "id": 33123456789,
            "name": "pytest",
            "conclusion": conclusion,
            "html_url": "https://github.com/gisce/github-agent-bridge/actions/runs/33123456789",
        },
        "sender": {"login": "github-actions"},
    }).encode()


def canary_policy(tmp_path):
    path = tmp_path / "policy.json"
    path.write_text(json.dumps({
        "trustedOrgs": ["gisce"],
        "webhookCanaryRepos": ["gisce/github-agent-bridge"],
        "botLogins": ["giscebot"],
        "actions": {"trustedAuto": ["reply_comment", "workflow_run_failed"]},
    }))
    return path


def assignment_policy(tmp_path):
    path = tmp_path / "policy.json"
    path.write_text(json.dumps({
        "trustedOrgs": ["gisce"],
        "webhookCanaryRepos": ["gisce/github-agent-bridge"],
        "botLogins": ["giscebot"],
        "actions": {"trustedAuto": ["open_issue"]},
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


def test_webhook_shadow_records_pull_request_review_requested_as_supported(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    payload = pull_request_review_requested_payload()
    client = TestClient(create_app(DashboardConfig(db=db, require_auth=False, webhook_secrets=(SECRET,))))

    response = client.post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="review-requested", event="pull_request"),
    )

    assert response.status_code == 200
    assert response.json()["status"] == "observed"
    assert response.json()["event_key"] == (
        "pull_request:review_requested:gisce/github-agent-bridge:1829195123:giscebot"
    )


def test_webhook_shadow_records_assignment_as_supported(tmp_path):
    payload = assignment_payload()
    client = TestClient(create_app(DashboardConfig(
        db=tmp_path / "bridge.sqlite3",
        require_auth=False,
        webhook_secrets=(SECRET,),
    )))

    response = client.post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="issue-assigned", event="issues"),
    )

    assert response.status_code == 200
    assert response.json()["status"] == "observed"
    assert response.json()["event_key"] == (
        "issues:assigned:gisce/github-agent-bridge:1829195190:giscebot"
    )


def test_webhook_canary_enqueues_issue_assigned_to_configured_bot(tmp_path):
    payload = assignment_payload()
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3",
        require_auth=False,
        webhook_secrets=(SECRET,),
        webhook_mode="canary",
        webhook_policy=assignment_policy(tmp_path),
    )

    response = TestClient(create_app(config)).post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="issue-assigned", event="issues"),
    )

    assert response.json()["enqueue_status"] == "enqueued"
    with sqlite3.connect(config.db) as con:
        assert con.execute(
            "SELECT action,work_intent,work_key,trigger_actor FROM jobs"
        ).fetchone() == (
            "open_issue",
            "work_allowed",
            "gisce/github-agent-bridge#190",
            "ecarreras",
        )


def test_webhook_canary_enqueues_pull_request_assigned_to_configured_bot(tmp_path):
    payload = assignment_payload(event_name="pull_request", number=258)
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3",
        require_auth=False,
        webhook_secrets=(SECRET,),
        webhook_mode="canary",
        webhook_policy=assignment_policy(tmp_path),
    )

    response = TestClient(create_app(config)).post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="pr-assigned", event="pull_request"),
    )

    assert response.json()["event_key"] == (
        "pull_request:assigned:gisce/github-agent-bridge:1829195190:giscebot"
    )
    assert response.json()["enqueue_status"] == "enqueued"
    with sqlite3.connect(config.db) as con:
        assert con.execute(
            "SELECT action,work_intent,work_key,trigger_actor FROM jobs"
        ).fetchone() == (
            "open_issue",
            "review_only",
            "gisce/github-agent-bridge#258",
            "ecarreras",
        )


def test_webhook_canary_ignores_assignment_to_other_user(tmp_path):
    payload = assignment_payload(assignee="someone-else")
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3",
        require_auth=False,
        webhook_secrets=(SECRET,),
        webhook_mode="canary",
        webhook_policy=assignment_policy(tmp_path),
    )

    response = TestClient(create_app(config)).post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="issue-assigned-other", event="issues"),
    )

    assert response.json()["status"] == "observed"
    assert response.json()["enqueue_status"] == "ignored"
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0


def test_webhook_canary_ignores_assignment_without_configured_bot_logins(tmp_path):
    policy_path = assignment_policy(tmp_path)
    policy_path.write_text(json.dumps({
        "trustedOrgs": ["gisce"],
        "webhookCanaryRepos": ["gisce/github-agent-bridge"],
        "botLogins": [],
        "actions": {"trustedAuto": ["open_issue"]},
    }))
    payload = assignment_payload()
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3",
        require_auth=False,
        webhook_secrets=(SECRET,),
        webhook_mode="canary",
        webhook_policy=policy_path,
    )

    response = TestClient(create_app(config)).post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="issue-assigned-no-bots", event="issues"),
    )

    assert response.json()["status"] == "observed"
    assert response.json()["enqueue_status"] == "ignored"
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0


def test_assignment_webhook_and_email_coalesce_into_one_active_job(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "github_agent_bridge.queue.trigger_actor_details_for_enqueue",
        lambda notification, ctx: TriggerActor(login="ecarreras"),
    )
    payload = assignment_payload()
    policy_path = assignment_policy(tmp_path)
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3",
        require_auth=False,
        webhook_secrets=(SECRET,),
        webhook_mode="canary",
        webhook_policy=policy_path,
    )
    webhook = TestClient(create_app(config)).post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="issue-assigned", event="issues"),
    )
    queue = JobQueue(config.db)
    claimed = queue.claim_next("worker")
    assert claimed is not None
    assert claimed.id == webhook.json()["job_id"]

    email_job, email_status = queue.enqueue(
        Notification(
            uid=99,
            message_id="<gisce/github-agent-bridge/issue/190/issue_event/32660065630@github.com>",
            subject="Re: [gisce/github-agent-bridge] Introduce versioned SQLite migrations (Issue #190)",
            from_addr="ecarreras <notifications@github.com>",
            body=(
                "ecarreras assigned @giscebot to this issue.\n\n"
                "https://github.com/gisce/github-agent-bridge/issues/190#event-32660065630"
            ),
            auth={"spf": True, "dkim": True, "dmarc": True},
        ),
        Policy.from_file(policy_path),
    )

    assert webhook.json()["enqueue_status"] == "enqueued"
    assert email_status == "coalesced"
    assert email_job is not None
    assert email_job.id == webhook.json()["job_id"]
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1
        assert con.execute("SELECT status,coalesced_count FROM jobs").fetchone() == (
            "running",
            1,
        )


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
    detail = client.get("/api/webhooks/github/deliveries/delivery-1").json()
    assert detail["delivery"]["delivery_id"] == "delivery-1"
    assert detail["delivery"]["enqueue_status"] == "enqueued"
    assert detail["delivery"]["job_id"] == first.json()["job_id"]
    assert detail["payload"] == json.loads(payload)
    assert detail["payload_hash"] == hashlib.sha256(payload).hexdigest()
    assert detail["job"] == {
        "id": first.json()["job_id"],
        "work_key": "gisce/github-agent-bridge#191",
        "status": "pending",
        "action": "reply_comment",
        "decision": "auto_trusted",
        "work_intent": "work_allowed",
        "updated_at": detail["job"]["updated_at"],
    }
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1
        assert con.execute(
            "SELECT source,event_key,status FROM ingest_receipts"
        ).fetchone() == (
            "webhook",
            "issue_comment:created:gisce/github-agent-bridge:5948901951",
            "accepted",
        )
        assert con.execute(
            "SELECT enqueue_status,job_id FROM webhook_shadow_receipts"
        ).fetchone() == ("enqueued", first.json()["job_id"])


def test_webhook_canary_does_not_claim_non_actionable_comment_before_email(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "github_agent_bridge.queue.trigger_actor_details_for_enqueue",
        lambda notification, ctx: None,
    )
    comment_id = 5948901951
    payload = non_actionable_issue_comment_payload(comment_id=comment_id)
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3",
        require_auth=False,
        webhook_secrets=(SECRET,),
        webhook_mode="canary",
        webhook_policy=canary_policy(tmp_path),
    )
    client = TestClient(create_app(config))

    webhook = client.post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload),
    )
    email_job, email_status = JobQueue(config.db).enqueue(
        Notification(
            uid=99,
            message_id="<email-delivery@github.com>",
            subject="Re: [gisce/github-agent-bridge] Evaluate GitHub App webhooks (#191)",
            from_addr="ecarreras <notifications@github.com>",
            body=(
                "You are receiving this because you were assigned.\n\n"
                f"https://github.com/gisce/github-agent-bridge/issues/191#issuecomment-{comment_id}"
            ),
            auth={"spf": True, "dkim": True, "dmarc": True},
        ),
        Policy.from_file(config.webhook_policy),
    )

    assert webhook.json()["enqueue_status"] == "ignored"
    assert email_status == "enqueued"
    assert email_job is not None
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1
        assert con.execute(
            "SELECT first_source FROM github_events WHERE event_key=?",
            (f"issue_comment:created:gisce/github-agent-bridge:{comment_id}",),
        ).fetchone()[0] == "email"


def test_webhook_canary_ignores_repo_outside_webhook_canary_repos(tmp_path):
    policy = canary_policy(tmp_path)
    policy.write_text(json.dumps({
        "trustedOrgs": ["gisce"],
        "webhookCanaryRepos": ["gisce/other"],
    }))
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
        assert con.execute(
            "SELECT enqueue_status FROM webhook_shadow_receipts"
        ).fetchone()[0] == "outside_canary"


def test_webhook_canary_ignores_events_created_by_configured_bot(tmp_path):
    payload = pull_request_review_payload(state="commented", sender="giscebot")
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3", require_auth=False,
        webhook_secrets=(SECRET,), webhook_mode="canary",
        webhook_policy=canary_policy(tmp_path),
    )

    response = TestClient(create_app(config)).post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="review-by-bot", event="pull_request_review"),
    )

    assert response.json()["enqueue_status"] == "ignored_bot"
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0
        assert con.execute(
            "SELECT enqueue_status,job_id FROM webhook_shadow_receipts"
        ).fetchone() == ("ignored_bot", None)


def test_webhook_canary_enqueues_pull_request_review_requested_for_configured_bot(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "github_agent_bridge.queue.trigger_actor_details_for_enqueue",
        lambda notification, ctx: None,
    )
    policy = canary_policy(tmp_path)
    policy.write_text(json.dumps({
        "trustedOrgs": ["gisce"],
        "webhookCanaryRepos": ["gisce/github-agent-bridge"],
        "botLogins": ["giscebot"],
        "actions": {"trustedAuto": ["submit_review"]},
    }))
    payload = pull_request_review_requested_payload(
        title="fix(deps): bump source-map-js from 1.2.1 to 1.2.2 in /dashboard",
    )
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3", require_auth=False,
        webhook_secrets=(SECRET,), webhook_mode="canary",
        webhook_policy=policy,
    )

    response = TestClient(create_app(config)).post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="review-requested", event="pull_request"),
    )

    assert response.json()["enqueue_status"] == "enqueued"
    assert response.json()["job_id"]
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT action,work_intent,work_key FROM jobs").fetchone() == (
            "submit_review",
            "review_only",
            "gisce/github-agent-bridge#233",
        )
        metadata = json.loads(con.execute("SELECT metadata_json FROM jobs").fetchone()[0])
        assert metadata["intent_guardrail"] == "submit_review_read_only"


def test_webhook_canary_ignores_pull_request_review_requested_for_other_reviewer(tmp_path):
    policy = canary_policy(tmp_path)
    policy.write_text(json.dumps({
        "trustedOrgs": ["gisce"],
        "webhookCanaryRepos": ["gisce/github-agent-bridge"],
        "botLogins": ["giscebot"],
        "actions": {"trustedAuto": ["submit_review"]},
    }))
    payload = pull_request_review_requested_payload(requested_reviewer="someone-else")
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3", require_auth=False,
        webhook_secrets=(SECRET,), webhook_mode="canary",
        webhook_policy=policy,
    )

    response = TestClient(create_app(config)).post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="review-requested-other", event="pull_request"),
    )

    assert response.json()["status"] == "observed"
    assert response.json()["enqueue_status"] == "ignored"
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0


def test_webhook_canary_ignores_pull_request_review_requested_when_bot_logins_empty(tmp_path):
    policy = canary_policy(tmp_path)
    policy.write_text(json.dumps({
        "trustedOrgs": ["gisce"],
        "webhookCanaryRepos": ["gisce/github-agent-bridge"],
        "botLogins": [],
        "actions": {"trustedAuto": ["submit_review"]},
    }))
    payload = pull_request_review_requested_payload(requested_reviewer="someone-else")
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3", require_auth=False,
        webhook_secrets=(SECRET,), webhook_mode="canary",
        webhook_policy=policy,
    )

    response = TestClient(create_app(config)).post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="review-requested-no-bots", event="pull_request"),
    )

    assert response.json()["status"] == "observed"
    assert response.json()["enqueue_status"] == "ignored"
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0


def test_webhook_canary_preserves_review_request_sender_as_trigger_actor(tmp_path, monkeypatch):
    def fail_context_lookup(notification, ctx):
        raise AssertionError("signed webhook sender should not be replaced by PR author lookup")

    monkeypatch.setattr(
        "github_agent_bridge.queue.trigger_actor_details_for_enqueue",
        fail_context_lookup,
    )
    policy = canary_policy(tmp_path)
    policy.write_text(json.dumps({
        "trustedOrgs": ["gisce"],
        "webhookCanaryRepos": ["gisce/github-agent-bridge"],
        "botLogins": ["giscebot"],
        "actions": {"trustedAuto": ["submit_review"]},
    }))
    payload = pull_request_review_requested_payload(sender="review-requester")
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3", require_auth=False,
        webhook_secrets=(SECRET,), webhook_mode="canary",
        webhook_policy=policy,
    )

    response = TestClient(create_app(config)).post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="review-requested-actor", event="pull_request"),
    )

    assert response.json()["enqueue_status"] == "enqueued"
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT trigger_actor FROM jobs").fetchone()[0] == "review-requester"


def test_webhook_canary_enqueues_failed_workflow_run(tmp_path):
    payload = workflow_run_payload(conclusion="failure")
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3", require_auth=False,
        webhook_secrets=(SECRET,), webhook_mode="canary",
        webhook_policy=canary_policy(tmp_path),
    )

    response = TestClient(create_app(config)).post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="workflow-failure", event="workflow_run"),
    )

    assert response.json()["enqueue_status"] == "enqueued"
    assert response.json()["event_key"] == (
        "workflow_run:workflow_run_failed:gisce/github-agent-bridge:33123456789"
    )
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT action,work_key FROM jobs").fetchone() == (
            "workflow_run_failed",
            "gisce/github-agent-bridge/actions/runs/33123456789",
        )


def test_webhook_canary_ignores_successful_workflow_run(tmp_path):
    payload = workflow_run_payload(conclusion="success")
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3", require_auth=False,
        webhook_secrets=(SECRET,), webhook_mode="canary",
        webhook_policy=canary_policy(tmp_path),
    )

    response = TestClient(create_app(config)).post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="workflow-success", event="workflow_run"),
    )

    assert response.json()["enqueue_status"] == "ignored"
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0


def test_webhook_primary_requires_policy(tmp_path):
    try:
        DashboardConfig(
            db=tmp_path / "bridge.sqlite3",
            webhook_mode="primary",
            webhook_primary_ack=True,
        )
    except ValueError as exc:
        assert "WEBHOOK_POLICY" in str(exc)
    else:
        raise AssertionError("primary mode must require a policy")


def test_webhook_primary_requires_explicit_acknowledgement(tmp_path):
    try:
        DashboardConfig(
            db=tmp_path / "bridge.sqlite3",
            webhook_mode="primary",
            webhook_policy=canary_policy(tmp_path),
            webhook_primary_ack=False,
        )
    except ValueError as exc:
        assert "PRIMARY_ACK" in str(exc)
    else:
        raise AssertionError("primary mode must require an explicit acknowledgement")


def test_webhook_primary_enqueues_trusted_repo_without_canary_allowlist(tmp_path):
    policy = canary_policy(tmp_path)
    policy.write_text(json.dumps({
        "trustedOrgs": ["gisce"],
        "enabledRepos": ["gisce/github-agent-bridge"],
        "webhookCanaryRepos": [],
        "botLogins": ["giscebot"],
        "actions": {"trustedAuto": ["reply_comment"]},
    }))
    payload = actionable_issue_comment_payload()
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3",
        require_auth=False,
        webhook_secrets=(SECRET,),
        webhook_mode="primary",
        webhook_policy=policy,
        webhook_primary_ack=True,
    )

    response = TestClient(create_app(config)).post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload),
    )

    assert response.json()["mode"] == "primary"
    assert response.json()["enqueue_status"] == "enqueued"
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT first_source FROM github_events").fetchone()[0] == "webhook"


def test_webhook_commit_comment_keeps_cross_source_identity(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "github_agent_bridge.queue.trigger_actor_details_for_enqueue",
        lambda notification, ctx: None,
    )
    comment_id = 778899
    payload = commit_comment_payload(comment_id=comment_id)
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3", require_auth=False,
        webhook_secrets=(SECRET,), webhook_mode="canary",
        webhook_policy=canary_policy(tmp_path),
    )
    client = TestClient(create_app(config))

    webhook = client.post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="commit-webhook", event="commit_comment"),
    )
    email_job, email_status = JobQueue(config.db).enqueue(
        Notification(
            uid=100,
            message_id="<commit-email@github.com>",
            subject="Re: commit comment",
            from_addr="ecarreras <notifications@github.com>",
            body=(
                "@giscebot investigate this\n\n"
                "https://github.com/gisce/github-agent-bridge/commit/abcdef123456"
                f"#commitcomment-{comment_id}"
            ),
            auth={"spf": True, "dkim": True, "dmarc": True},
        ),
        Policy.from_file(config.webhook_policy),
    )

    expected = f"commit_comment:created:gisce/github-agent-bridge:{comment_id}"
    assert webhook.json()["event_key"] == expected
    assert webhook.json()["enqueue_status"] == "enqueued"
    assert email_status == "duplicate"
    assert email_job is not None
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1
        assert con.execute(
            "SELECT event_key FROM ingest_receipts WHERE source='webhook'"
        ).fetchone()[0] == expected


def test_webhook_canary_ignores_approved_pull_request_reviews(tmp_path):
    payload = pull_request_review_payload(state="approved")
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3", require_auth=False,
        webhook_secrets=(SECRET,), webhook_mode="canary",
        webhook_policy=canary_policy(tmp_path),
    )

    response = TestClient(create_app(config)).post(
        "/api/webhooks/github",
        content=payload,
        headers=signed_headers(payload, delivery="review-approved", event="pull_request_review"),
    )

    assert response.json()["enqueue_status"] == "ignored"
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0


def test_webhook_canary_enqueues_actionable_pull_request_review_states(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "github_agent_bridge.queue.trigger_actor_details_for_enqueue",
        lambda notification, ctx: None,
    )
    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3", require_auth=False,
        webhook_secrets=(SECRET,), webhook_mode="canary",
        webhook_policy=canary_policy(tmp_path),
    )
    client = TestClient(create_app(config))

    for delivery, state in (("review-commented", "commented"), ("review-changes", "changes_requested")):
        payload = pull_request_review_payload(state=state)
        response = client.post(
            "/api/webhooks/github",
            content=payload,
            headers=signed_headers(payload, delivery=delivery, event="pull_request_review"),
        )

        assert response.json()["enqueue_status"] in {"enqueued", "duplicate"}
    with sqlite3.connect(config.db) as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1


def test_dashboard_config_expands_systemd_home_specifier_for_webhook_policy(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))

    config = DashboardConfig(
        db=tmp_path / "bridge.sqlite3",
        require_auth=False,
        webhook_mode="canary",
        webhook_policy="%h/.config/github-agent-bridge/policy.json",
    )

    assert config.webhook_policy == tmp_path / ".config/github-agent-bridge/policy.json"


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
    assert client.post("/api/webhooks/github/hooks/42/ping").status_code == 401
    assert client.get("/api/webhooks/github/deliveries/delivery-1").status_code == 401
    client.cookies.set("gab_dashboard_session", _sign(config, _encode_session({"login": "alice"})))
    assert {client.get(path).status_code for path in paths} == {403}
    assert client.get("/api/webhooks/github/hooks/42").status_code == 403
    assert client.post("/api/webhooks/github/hooks/42/ping").status_code == 403
    assert client.get("/api/webhooks/github/deliveries/delivery-1").status_code == 403
    client.cookies.set("gab_dashboard_session", _sign(config, _encode_session({"login": "operator"}, is_admin=True)))
    assert {client.get(path).status_code for path in paths} == {200}
    assert client.get("/api/webhooks/github/hooks/42").status_code == 404
    assert client.post("/api/webhooks/github/hooks/42/ping").status_code == 404
    assert client.get("/api/webhooks/github/deliveries/delivery-1").status_code == 404
    assert client.get("/api/status").json()["webhook_configured"] is True


def test_dashboard_status_hides_webhook_tab_when_not_configured(tmp_path):
    client = TestClient(create_app(DashboardConfig(db=tmp_path / "bridge.sqlite3", require_auth=False)))

    assert client.get("/api/status").json()["webhook_configured"] is False


def test_webhook_monitoring_endpoints_keep_summary_light_and_return_real_data(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    client = TestClient(create_app(DashboardConfig(
        db=db,
        require_auth=False,
        webhook_secrets=(SECRET,),
        webhook_coverage_grace_seconds=0,
    )))
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
    assert summary["mode"] == "shadow"
    assert summary["configured"] is True
    assert summary["receipts"] == {"observed": 1, "unsupported": 1}
    assert summary["duplicate_deliveries"] == 0
    assert summary["cross_source_matches"] == 0
    assert summary["enqueue"] == {}
    assert summary["totals"] == {"hooks": 1, "deliveries": 2}
    assert {
        key: summary["coverage"][key]
        for key in (
            "both", "imap_only", "webhook_only", "imap_eligible", "ratio",
            "mean_match_delay_ms", "grace_seconds",
        )
    } == {
        "both": 0, "imap_only": 0, "webhook_only": 1,
        "imap_eligible": 0, "ratio": None, "mean_match_delay_ms": None,
        "grace_seconds": 0,
    }
    assert summary["coverage"]["window_start"]
    assert summary["coverage"]["window_end"]
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
    assert detail["recent_actions"] == []

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

    delivery_detail = client.get("/api/webhooks/github/deliveries/delivery-1").json()
    assert delivery_detail["delivery"] == next(
        item for item in deliveries if item["delivery_id"] == "delivery-1"
    )
    assert delivery_detail["payload"] == json.loads(delivery)
    assert delivery_detail["payload_hash"] == hashlib.sha256(delivery).hexdigest()
    assert delivery_detail["job"] is None

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


def test_webhook_repository_hook_ping_keeps_repository_target_when_organization_is_present(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    client = TestClient(create_app(DashboardConfig(
        db=db,
        require_auth=False,
        webhook_secrets=(SECRET,),
    )))
    ping = json.dumps({
        "zen": "Keep it logically awesome.",
        "hook": {
            "id": 42, "name": "web", "active": True,
            "events": ["*"],
            "config": {
                "content_type": "json", "insecure_ssl": "0",
                "secret": "********", "url": "https://gab.gisce.net/api/webhooks/github",
            },
            "created_at": "2026-10-05T15:29:44Z",
            "updated_at": "2026-10-05T15:29:44Z",
            "url": "https://api.github.com/repos/gisce/github-agent-bridge/hooks/42",
            "ping_url": "https://api.github.com/repos/gisce/github-agent-bridge/hooks/42/pings",
            "deliveries_url": "https://api.github.com/repos/gisce/github-agent-bridge/hooks/42/deliveries",
        },
        "repository": {"full_name": "gisce/github-agent-bridge"},
        "organization": {"login": "gisce"},
    }).encode()

    response = client.post(
        "/api/webhooks/github",
        content=ping,
        headers=hook_headers(ping, delivery="repo-ping-1", event="ping", hook_id="42"),
    )

    assert response.status_code == 200
    hook = client.get("/api/webhooks/github/hooks").json()["hooks"][0]
    assert hook["target"] == "gisce/github-agent-bridge"
    assert hook["target_type"] == "repository"
    assert hook["admin_url"] == "https://github.com/gisce/github-agent-bridge/settings/hooks/42"


def test_webhook_coverage_uses_comparable_window_keys_and_grace(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    JobQueue(db)
    now = datetime.now(UTC)
    within_window = (now - timedelta(minutes=20)).isoformat()
    rollout_start = (now - timedelta(hours=1)).isoformat()
    too_recent = (now - timedelta(minutes=1)).isoformat()
    too_old = (now - timedelta(days=40)).isoformat()
    rows = [
        ("matched", "issue_comment:created:gisce/repo:1", rollout_start),
        ("webhook-only", "issue_comment:created:gisce/repo:2", within_window),
        ("successful-workflow", "workflow_run:completed:gisce/repo:3", within_window),
    ]
    with sqlite3.connect(db) as con:
        con.executemany(
            "INSERT INTO webhook_shadow_receipts("
            "delivery_id,event_name,action,event_key,repository,payload_hash,status,created_at"
            ") VALUES(?,?,?,?,?,?,?,?)",
            [
                (
                    delivery_id,
                    "workflow_run" if delivery_id == "successful-workflow" else "issue_comment",
                    "completed" if delivery_id == "successful-workflow" else "created",
                    event_key, "gisce/repo", delivery_id, "observed", created_at,
                )
                for delivery_id, event_key, created_at in rows
            ],
        )
        con.execute(
            "INSERT INTO webhook_shadow_receipts("
            "delivery_id,event_name,action,event_key,repository,payload_hash,status,created_at"
            ") VALUES(?,?,?,?,?,?,?,?)",
            (
                "review-request", "pull_request", "review_requested",
                "pull_request:review_requested:gisce/repo:42:giscebot",
                "gisce/repo", "review-request", "observed", within_window,
            ),
        )
        con.executemany(
            "INSERT INTO ingest_receipts("
            "source,source_key,payload_hash,event_key,status,created_at,updated_at"
            ") VALUES('email',?,?,?,?,?,?)",
            [
                ("email-matched", "hash-1", "issue_comment:created:gisce/repo:1", "accepted", within_window, within_window),
                ("email-only", "hash-2", "issue_comment:created:gisce/repo:4", "accepted", within_window, within_window),
                ("email-recent", "hash-3", "issue_comment:created:gisce/repo:5", "accepted", too_recent, too_recent),
                ("email-old", "hash-4", "issue_comment:created:gisce/repo:6", "accepted", too_old, too_old),
                ("email-fallback", "hash-5", "email:<fallback@github.com>", "accepted", within_window, within_window),
                (
                    "email-review-request", "hash-6", "email:<review-request@github.com>",
                    "accepted", within_window, within_window,
                ),
            ],
        )
    client = TestClient(create_app(DashboardConfig(
        db=db,
        require_auth=False,
        webhook_secrets=(SECRET,),
        webhook_coverage_grace_seconds=600,
    )))

    summary = client.get("/api/webhooks/github/summary").json()
    exceptions = client.get("/api/webhooks/github/exceptions").json()

    assert {
        key: summary["coverage"][key]
        for key in (
            "both", "imap_only", "webhook_only", "imap_eligible", "ratio",
            "grace_seconds",
        )
    } == {
        "both": 1, "imap_only": 1, "webhook_only": 1,
        "imap_eligible": 2, "ratio": 0.5,
        "grace_seconds": 600,
    }
    assert {(item["kind"], item["event_key"]) for item in exceptions["exceptions"]} == {
        ("imap_only", "issue_comment:created:gisce/repo:4"),
        ("webhook_only", "issue_comment:created:gisce/repo:2"),
    }


def test_admin_can_request_hook_ping_and_action_is_audited(tmp_path, monkeypatch):
    db = tmp_path / "bridge.sqlite3"
    client = TestClient(create_app(DashboardConfig(db=db, require_auth=False, webhook_secrets=(SECRET,))))
    ping = json.dumps({
        "hook": {
            "id": 42, "active": True, "events": ["issue_comment"],
            "ping_url": "https://api.github.com/orgs/gisce/hooks/42/pings",
        },
        "organization": {"login": "gisce"},
    }).encode()
    assert client.post(
        "/api/webhooks/github", content=ping,
        headers=hook_headers(ping, delivery="ping-config", event="ping", hook_id="42"),
    ).status_code == 200
    calls = []

    def fake_ping(endpoint, *, gh_bin):
        calls.append((endpoint, gh_bin))
        return backend.subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(backend, "_request_webhook_ping", fake_ping)
    response = client.post("/api/webhooks/github/hooks/42/ping")

    assert response.status_code == 200
    assert response.json()["status"] == "succeeded"
    assert calls == [("/orgs/gisce/hooks/42/pings", "gh")]
    action = client.get("/api/webhooks/github/hooks/42").json()["recent_actions"][0]
    assert action["action"] == "ping"
    assert action["actor"] == "test"
    assert action["status"] == "succeeded"
    assert action["completed_at"]


def test_hook_ping_rejects_untrusted_stored_api_url(tmp_path, monkeypatch):
    db = tmp_path / "bridge.sqlite3"
    client = TestClient(create_app(DashboardConfig(db=db, require_auth=False, webhook_secrets=(SECRET,))))
    ping = json.dumps({
        "hook": {
            "id": 42, "active": True,
            "ping_url": "https://attacker.example/orgs/gisce/hooks/42/pings",
        },
        "organization": {"login": "gisce"},
    }).encode()
    assert client.post(
        "/api/webhooks/github", content=ping,
        headers=hook_headers(ping, delivery="ping-config", event="ping", hook_id="42"),
    ).status_code == 200
    monkeypatch.setattr(backend, "_request_webhook_ping", lambda *args, **kwargs: None)

    response = client.post("/api/webhooks/github/hooks/42/ping")

    assert response.status_code == 409
    assert response.json()["detail"] == "webhook_ping_url_invalid"


def test_failed_hook_ping_is_audited_without_exposing_cli_error(tmp_path, monkeypatch):
    db = tmp_path / "bridge.sqlite3"
    client = TestClient(create_app(DashboardConfig(db=db, require_auth=False, webhook_secrets=(SECRET,))))
    ping = json.dumps({
        "hook": {
            "id": 42, "active": True,
            "ping_url": "https://api.github.com/orgs/gisce/hooks/42/pings",
        },
        "organization": {"login": "gisce"},
    }).encode()
    client.post(
        "/api/webhooks/github", content=ping,
        headers=hook_headers(ping, delivery="ping-config", event="ping", hook_id="42"),
    )
    monkeypatch.setattr(
        backend, "_request_webhook_ping",
        lambda *args, **kwargs: backend.subprocess.CompletedProcess([], 1, "", "permission denied: sensitive detail"),
    )

    response = client.post("/api/webhooks/github/hooks/42/ping")

    assert response.status_code == 502
    assert response.json()["detail"] == "github_webhook_ping_failed"
    assert "sensitive detail" not in response.text
    action = client.get("/api/webhooks/github/hooks/42").json()["recent_actions"][0]
    assert action["status"] == "failed"
    assert action["detail"] == "permission denied: sensitive detail"

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


def test_existing_webhook_hook_targets_are_backfilled_from_github_api_url(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    JobQueue(db)
    with sqlite3.connect(db) as con:
        con.execute(
            "INSERT INTO webhook_hooks(hook_id,target,target_type,events_json,github_api_url,updated_at) "
            "VALUES(?,?,?,?,?,?)",
            (
                "42", "gisce", "organization", "[]",
                "https://api.github.com/repos/gisce/github-agent-bridge/hooks/42",
                "2026-10-05T15:29:45+00:00",
            ),
        )
        con.execute(
            "INSERT INTO webhook_hooks(hook_id,target,target_type,events_json,github_api_url,updated_at) "
            "VALUES(?,?,?,?,?,?)",
            (
                "43", "gisce", "organization", "[]",
                "https://api.github.com/orgs/gisce/hooks/43",
                "2026-10-05T15:29:45+00:00",
            ),
        )
        con.execute("DELETE FROM schema_migrations WHERE version=1")

    client = TestClient(create_app(DashboardConfig(db=db, require_auth=False, webhook_secrets=(SECRET,))))
    hooks = {hook["id"]: hook for hook in client.get("/api/webhooks/github/hooks").json()["hooks"]}

    assert hooks["42"]["target"] == "gisce/github-agent-bridge"
    assert hooks["42"]["target_type"] == "repository"
    assert hooks["42"]["admin_url"] == "https://github.com/gisce/github-agent-bridge/settings/hooks/42"
    assert hooks["43"]["target"] == "gisce"
    assert hooks["43"]["target_type"] == "organization"
    assert hooks["43"]["admin_url"] == "https://github.com/organizations/gisce/settings/hooks/43"


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
