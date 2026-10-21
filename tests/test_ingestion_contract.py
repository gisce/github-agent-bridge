from __future__ import annotations

import json
from pathlib import Path

import pytest

from github_agent_bridge.ingestion_contract import normalize_event_envelope
from github_agent_bridge.models import Notification, TriggerActor
from github_agent_bridge.parser import (
    classify_github_action,
    classify_work_intent,
    extract_github_context,
)
from github_agent_bridge.policy import Policy
from github_agent_bridge.queue import JobQueue
from github_agent_bridge.webhook import (
    canonical_webhook_event_key,
    webhook_notification,
)


FIXTURES = json.loads(
    (Path(__file__).parent / "fixtures" / "ingestion_parity.json").read_text()
)
BOT_LOGINS = {"giscebot"}


def _pair(case):
    webhook_data = case["webhook"]
    webhook = webhook_notification(
        webhook_data["event_name"],
        webhook_data["delivery_id"],
        webhook_data["payload"],
        bot_logins=BOT_LOGINS,
    )
    assert webhook is not None
    email = Notification(
        **case["email"],
        auth={"spf": True, "dkim": True, "dmarc": True},
    )
    return webhook, email


@pytest.mark.parametrize("case", FIXTURES, ids=lambda case: case["name"])
def test_redacted_transport_fixtures_share_the_event_contract(case):
    webhook, email = _pair(case)
    webhook_identity = case["canonical_identity"] or (
        f"webhook:{case['webhook']['delivery_id']}"
    )
    assert canonical_webhook_event_key(
        case["webhook"]["event_name"],
        case["webhook"]["payload"],
        source_key=case["webhook"]["delivery_id"],
    ) == webhook_identity
    envelopes = []
    for source, source_key, notification in (
        ("webhook", case["webhook"]["delivery_id"], webhook),
        ("email", email.message_id, email),
    ):
        ctx = extract_github_context(notification.body)
        action = classify_github_action(
            notification.subject,
            notification.body,
            BOT_LOGINS,
            message_id=notification.message_id,
        )
        intent = classify_work_intent(
            notification.subject, notification.body, BOT_LOGINS
        )
        if action in {"archive_notification", "submit_review", "sync_after_merge"}:
            intent = "review_only"
        envelope = normalize_event_envelope(
            notification,
            ctx,
            bridge_action=action,
            work_intent=intent,
            source=source,
            source_key=source_key,
            trigger_actor=TriggerActor(login="ecarreras"),
            bot_logins=BOT_LOGINS,
        )
        envelopes.append(envelope)

        assert envelope.bridge_action == case["bridge_action"]
        assert envelope.work_intent == case["work_intent"]
        assert envelope.event_type == case["event_type"]
        assert envelope.event_action == case["event_action"]
        assert envelope.subject_type == case["subject_type"]
        assert envelope.thread == case["thread"]
        assert envelope.canonical_identity == (
            case["canonical_identity"] or f"{source}:{source_key}"
        )
        assert envelope.repository == "gisce/example"
        assert envelope.actor == "ecarreras"
        assert envelope.provenance.transport == source
        assert envelope.provenance.receipt_id == source_key

    webhook_envelope, email_envelope = envelopes
    assert (
        webhook_envelope.repository,
        webhook_envelope.thread,
        webhook_envelope.subject_type,
        webhook_envelope.event_type,
        webhook_envelope.event_action,
        webhook_envelope.actor,
        webhook_envelope.bridge_action,
        webhook_envelope.work_intent,
    ) == (
        email_envelope.repository,
        email_envelope.thread,
        email_envelope.subject_type,
        email_envelope.event_type,
        email_envelope.event_action,
        email_envelope.actor,
        email_envelope.bridge_action,
        email_envelope.work_intent,
    )
    if case["canonical_identity"]:
        assert (
            webhook_envelope.canonical_identity
            == email_envelope.canonical_identity
        )
    else:
        assert webhook_envelope.canonical_identity != email_envelope.canonical_identity


@pytest.mark.parametrize(
    "case_name",
    [case["name"] for case in FIXTURES if case["canonical_identity"]],
)
@pytest.mark.parametrize("first_source", ["webhook", "email"])
def test_transport_neutral_events_deduplicate_in_both_arrival_orders(
    tmp_path, monkeypatch, case_name, first_source
):
    case = next(item for item in FIXTURES if item["name"] == case_name)
    webhook, email = _pair(case)
    monkeypatch.setattr(
        "github_agent_bridge.queue.trigger_actor_details_for_enqueue",
        lambda notification, ctx: TriggerActor(login="ecarreras"),
    )
    queue = JobQueue(tmp_path / "queue.sqlite3")
    policy = Policy(trusted_orgs={"gisce"}, bot_logins=BOT_LOGINS)
    notifications = {
        "webhook": (
            webhook,
            {
                "source": "webhook",
                "source_key": case["webhook"]["delivery_id"],
            },
        ),
        "email": (email, {}),
    }
    second_source = "email" if first_source == "webhook" else "webhook"

    first, first_state = queue.ingest(
        notifications[first_source][0],
        policy,
        **notifications[first_source][1],
    )
    second, second_state = queue.ingest(
        notifications[second_source][0],
        policy,
        **notifications[second_source][1],
    )

    assert first_state == "enqueued"
    assert second_state == "duplicate"
    assert first is not None and second is not None
    assert first.id == second.id
    assert first.action == case["bridge_action"]
    assert first.work_intent == case["work_intent"]
    assert first.work_key == f"gisce/example#{case['thread']}"
    assert first.metadata["event_envelope"]["canonical_identity"] == case[
        "canonical_identity"
    ]
    with queue.connect() as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM ingest_receipts").fetchone()[0] == 2
        assert con.execute("SELECT COUNT(*) FROM github_events").fetchone()[0] == 1


@pytest.mark.parametrize(
    "case_name", ["issue_assignment", "review_requested", "issue_closed"]
)
def test_repeated_state_event_after_completion_creates_a_new_job(
    tmp_path, case_name
):
    case = next(item for item in FIXTURES if item["name"] == case_name)
    first_notification, _ = _pair(case)
    second_delivery = f"{case['webhook']['delivery_id']}-again"
    second_notification = webhook_notification(
        case["webhook"]["event_name"],
        second_delivery,
        case["webhook"]["payload"],
        bot_logins=BOT_LOGINS,
    )
    assert second_notification is not None
    queue = JobQueue(tmp_path / "queue.sqlite3")
    policy = Policy(trusted_orgs={"gisce"}, bot_logins=BOT_LOGINS)

    first, first_state = queue.ingest(
        first_notification,
        policy,
        source="webhook",
        source_key=case["webhook"]["delivery_id"],
    )
    assert first_state == "enqueued"
    assert first is not None
    if first.status == "pending":
        claimed = queue.claim_next("worker")
        assert claimed is not None and claimed.id == first.id
        queue.finish(first.id, "done", "completed")
    else:
        assert first.status == "done"

    second, second_state = queue.ingest(
        second_notification,
        policy,
        source="webhook",
        source_key=second_delivery,
    )

    assert second_state == "enqueued"
    assert second is not None and second.id != first.id
    assert first.metadata["event_envelope"]["canonical_identity"] == (
        f"webhook:{case['webhook']['delivery_id']}"
    )
    assert second.metadata["event_envelope"]["canonical_identity"] == (
        f"webhook:{second_delivery}"
    )
    with queue.connect() as con:
        assert con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 2
        assert con.execute("SELECT COUNT(*) FROM github_events").fetchone()[0] == 2
