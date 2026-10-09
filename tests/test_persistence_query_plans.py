from __future__ import annotations

import sqlite3

from github_agent_bridge.persistence import (
    CommitStatusRepository,
    Database,
    FeedbackRepository,
    JobRepository,
    McpTokenRepository,
    RuntimeRepository,
    StateRepository,
    WebhookRepository,
    WebPushRepository,
)
from github_agent_bridge.queue import JobQueue


class TracingDatabase(Database):
    def __init__(self, path):
        super().__init__(path)
        self.statements: list[str] = []

    def read_only(self) -> sqlite3.Connection:
        con = super().read_only()
        con.set_trace_callback(self.statements.append)
        return con

    def read_write(self) -> sqlite3.Connection:
        con = super().read_write()
        con.set_trace_callback(self.statements.append)
        return con


def _captured(database: TracingDatabase, fragment: str) -> str:
    expected = fragment.lower()
    for statement in database.statements:
        normalized = " ".join(statement.split()).lower()
        if expected in normalized:
            return statement
    raise AssertionError(f"query containing {fragment!r} was not executed")


def _plan(path, statement: str) -> list[str]:
    with Database(path).read_only() as con:
        return [
            str(row["detail"])
            for row in con.execute(f"EXPLAIN QUERY PLAN {statement}").fetchall()
        ]


def test_runnable_job_claim_uses_status_and_work_key_indexes(tmp_path):
    path = tmp_path / "bridge.sqlite3"
    JobQueue(path)
    database = TracingDatabase(path)
    repository = JobRepository(
        database,
        RuntimeRepository(database),
        StateRepository(database),
        CommitStatusRepository(database),
    )

    assert repository.claim_next("plan-worker") is None

    details = _plan(database.path, _captured(database, "select * from jobs j"))
    assert any("idx_jobs_status_created" in detail for detail in details)
    assert any("idx_jobs_work_status" in detail for detail in details)
    assert all("SCAN j" not in detail for detail in details)


def test_webhook_pagination_and_coverage_use_hot_path_indexes(tmp_path):
    path = tmp_path / "bridge.sqlite3"
    JobQueue(path)
    database = TracingDatabase(path)
    repository = WebhookRepository(database)

    assert repository.list_deliveries(
        50,
        cursor=("2026-10-09T12:00:00Z", "delivery-50"),
    ) == []
    repository.summary("2026-10-08T12:00:00Z", "2026-10-09T12:00:00Z")

    pagination = _plan(
        database.path,
        _captured(database, "from webhook_shadow_receipts r left join"),
    )
    coverage = _plan(database.path, _captured(database, "with email_events as"))
    assert any("idx_webhook_shadow_delivery_page" in detail for detail in pagination)
    assert all("USE TEMP B-TREE FOR ORDER BY" not in detail for detail in pagination)
    assert any("idx_ingest_receipts_coverage" in detail for detail in coverage)
    assert any("idx_webhook_shadow_coverage" in detail for detail in coverage)


def test_feedback_mcp_and_push_lookups_use_domain_indexes(tmp_path):
    path = tmp_path / "bridge.sqlite3"
    JobQueue(path)
    database = TracingDatabase(path)

    FeedbackRepository(database).list_rules(
        scopes=["global", "repo:gisce/github-agent-bridge"],
        min_confidence=0.5,
    )
    McpTokenRepository(database).active_credentials("2026-10-09T12:00:00Z")
    push = WebPushRepository(database)
    push.active_for_user("ecarreras")
    push.active_for_recipients(["ecarreras", "marc"])

    feedback = _plan(database.path, _captured(database, "from feedback_rules"))
    mcp = _plan(database.path, _captured(database, "from mcp_tokens"))
    push_user = _plan(
        database.path,
        _captured(database, "where user_login='ecarreras' and disabled_at is null"),
    )
    push_recipients = _plan(
        database.path,
        _captured(database, "lower(user_login) in ('ecarreras','marc')"),
    )
    assert any("idx_feedback_rules_scope_confidence" in detail for detail in feedback)
    assert any(
        "idx_feedback_rules_scope_nocase_confidence" in detail
        for detail in feedback
    )
    assert any("idx_mcp_tokens_active" in detail for detail in mcp)
    assert any("idx_web_push_subscriptions_user" in detail for detail in push_user)
    assert any(
        "idx_web_push_subscriptions_active_recipient" in detail
        for detail in push_recipients
    )
