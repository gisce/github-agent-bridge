import sqlite3

import pytest

from github_agent_bridge import feedback
from github_agent_bridge.models import GitHubContext, Notification
from github_agent_bridge.queue import JobQueue


def notification(body: str | None = None):
    return Notification(
        uid=1,
        message_id="<1@github.com>",
        subject="Re: [gisce/erp] PR",
        from_addr="notifications@github.com",
        body=body or "Aquest comentari critica el comportament de l'agent. https://github.com/gisce/erp/pull/1#issuecomment-10",
    )


def context():
    return GitHubContext(["https://github.com/gisce/erp/pull/1#issuecomment-10"], "gisce/erp", 1, comment_id=10)


def test_capture_feedback_stores_candidate_event_without_synthesizing_rule(tmp_path):
    db = tmp_path / "q.sqlite3"
    JobQueue(db)

    assert feedback.capture_feedback(db, notification(), context(), "reply_comment", "auto_trusted", "review_only")

    with sqlite3.connect(db) as con:
        event = con.execute("SELECT scope, classification, confidence, memorable FROM feedback_events").fetchone()
        rule_count = con.execute("SELECT count(*) FROM feedback_rules").fetchone()[0]

    assert event == ("repo:gisce/erp", "unreviewed", 0.0, 0)
    assert rule_count == 0


def test_capture_feedback_deduplicates_events(tmp_path):
    db = tmp_path / "q.sqlite3"
    JobQueue(db)
    n = notification()

    feedback.capture_feedback(db, n, context(), "reply_comment", "auto_trusted", "review_only")
    feedback.capture_feedback(db, n, context(), "reply_comment", "auto_trusted", "review_only")

    assert len(feedback.list_events(db, "repo:gisce/erp")) == 1
    assert feedback.list_rules(db, "repo:gisce/erp") == []


def test_capture_feedback_ignores_non_actionable_decisions(tmp_path):
    db = tmp_path / "q.sqlite3"
    JobQueue(db)

    assert feedback.capture_feedback(db, notification(), context(), "archive_notification", "auto", "work_allowed") is False
    assert feedback.list_events(db) == []


def test_add_rule_creates_curated_agent_rule(tmp_path):
    db = tmp_path / "q.sqlite3"
    JobQueue(db)
    feedback.capture_feedback(db, notification(), context(), "reply_comment", "auto_trusted", "review_only")
    event_id = feedback.list_events(db, "repo:gisce/erp")[0]["id"]

    rule = feedback.add_rule(
        db,
        scope="repo:gisce/erp",
        rule_type="style_preference",
        rule="Avoid generic acknowledgements; answer with concrete repo-specific evidence.",
        confidence=0.8,
        source_events=[event_id],
    )

    assert rule["scope"] == "repo:gisce/erp"
    assert rule["type"] == "style_preference"
    assert rule["confidence"] == 0.8
    assert rule["source_events"] == [event_id]
    assert feedback.list_rules(db, "repo:gisce/erp", min_confidence=0.75) == [rule]
    assert feedback.list_rules(db, "repo:gisce/erp", min_confidence=0.95) == []


def test_add_rule_rejects_invalid_confidence(tmp_path):
    db = tmp_path / "q.sqlite3"
    JobQueue(db)

    with pytest.raises(ValueError, match="confidence"):
        feedback.add_rule(db, "repo:gisce/erp", "style_preference", "Rule", 1.7)


def test_openclaw_json_payload_text_is_extracted():
    raw = '{"result":{"payloads":[{"text":"{\\\"is_feedback\\\":false,\\\"scope\\\":\\\"global\\\",\\\"type\\\":\\\"domain_context\\\",\\\"rule\\\":\\\"\\\",\\\"confidence\\\":0,\\\"reason\\\":\\\"shape test\\\"}"}]}}'

    assert feedback._extract_json_object(feedback._openclaw_text_from_json(raw))["reason"] == "shape test"


def test_learning_prompt_uses_packaged_prompt_resource():
    event = {"id": "e1", "scope": "repo:gisce/erp", "comment": "Read AGENTS.md first"}

    prompt = feedback.build_learning_prompt(event)

    assert "# Feedback classifier prompt" in prompt
    assert "Event JSON:" in prompt
    assert "Read AGENTS.md first" in prompt
    assert "You are classifying GitHub agent feedback" in feedback.FEEDBACK_CLASSIFIER_PROMPT
    assert "feature requests, work orders, or product" in feedback.FEEDBACK_CLASSIFIER_PROMPT


def test_learning_prompt_accepts_policy_override_template():
    event = {"id": "e1", "scope": "repo:gisce/erp", "comment": "Read AGENTS.md first"}

    prompt = feedback.build_learning_prompt(event, "CUSTOM CLASSIFIER {event_json}\n")

    assert prompt.startswith("CUSTOM CLASSIFIER ")
    assert "Read AGENTS.md first" in prompt
    assert "# Feedback classifier prompt" not in prompt


def test_learn_from_events_auto_approves_high_confidence_feedback(tmp_path, monkeypatch):
    db = tmp_path / "q.sqlite3"
    JobQueue(db)
    feedback.capture_feedback(db, notification(), context(), "reply_comment", "auto_trusted", "review_only")

    def fake_classify(event, **kwargs):
        return {
            "event_id": event["id"],
            "is_feedback": True,
            "scope": event["scope"],
            "type": "operating_rule",
            "rule": "Read the repository guide before changing project architecture.",
            "confidence": 0.91,
            "reason": "The comment criticizes a process failure that can recur.",
        }

    monkeypatch.setattr(feedback, "classify_event_with_llm", fake_classify)

    result = feedback.learn_from_events(db, limit=5, auto_approve_confidence=0.8)

    assert result["processed"] == 1
    assert result["approved"] == 1
    rules = feedback.list_rules(db, "repo:gisce/erp", min_confidence=0.8)
    assert len(rules) == 1
    assert rules[0]["rule"] == "Read the repository guide before changing project architecture."


def test_learn_from_events_rejects_task_specific_comments(tmp_path, monkeypatch):
    db = tmp_path / "q.sqlite3"
    JobQueue(db)
    feedback.capture_feedback(db, notification(), context(), "reply_comment", "auto_trusted", "review_only")

    def fake_classify(event, **kwargs):
        return {
            "event_id": event["id"],
            "is_feedback": False,
            "scope": event["scope"],
            "type": "domain_context",
            "rule": "",
            "confidence": 0.2,
            "reason": "Only about this PR.",
        }

    monkeypatch.setattr(feedback, "classify_event_with_llm", fake_classify)

    result = feedback.learn_from_events(db, limit=5, auto_approve_confidence=0.8)

    assert result["processed"] == 1
    assert result["rejected"] == 1
    assert feedback.list_rules(db) == []
    assert feedback.list_proposals(db, status="rejected")[0]["reason"] == "Only about this PR."


def test_learn_from_events_reports_error_when_failure_cannot_be_stored(tmp_path, monkeypatch):
    db = tmp_path / "q.sqlite3"
    JobQueue(db)
    feedback.capture_feedback(db, notification(), context(), "reply_comment", "auto_trusted", "review_only")

    def fail_classification(event, **kwargs):
        raise RuntimeError("ENOSPC: no space left on device")

    def fail_store(*args, **kwargs):
        raise sqlite3.OperationalError("database or disk is full")

    monkeypatch.setattr(feedback, "classify_event_with_llm", fail_classification)
    monkeypatch.setattr(feedback, "store_proposal", fail_store)

    result = feedback.learn_from_events(db, limit=5)

    assert result["processed"] == 1
    assert result["errors"] == 1
    assert result["proposals"][0]["status"] == "error"
    assert "ENOSPC" in result["proposals"][0]["error"]
    assert "database or disk is full" in result["proposals"][0]["error"]
