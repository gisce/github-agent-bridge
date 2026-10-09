from __future__ import annotations

import sqlite3


VERSION = 3
NAME = "persistence hot-path query indexes"


def upgrade(con: sqlite3.Connection) -> None:
    statements = (
        "CREATE INDEX IF NOT EXISTS idx_ingest_receipts_coverage "
        "ON ingest_receipts(source, julianday(created_at), event_key)",
        "CREATE INDEX IF NOT EXISTS idx_webhook_shadow_coverage "
        "ON webhook_shadow_receipts(julianday(created_at), event_key)",
        "CREATE INDEX IF NOT EXISTS idx_coalesced_notifications_job_id "
        "ON coalesced_notifications(job_id, id)",
        "CREATE INDEX IF NOT EXISTS idx_worklog_job_id ON worklog(job_id, id)",
        "CREATE INDEX IF NOT EXISTS idx_job_progress_job_id "
        "ON job_progress(job_id, id)",
        "CREATE INDEX IF NOT EXISTS idx_feedback_rules_scope_nocase_confidence "
        "ON feedback_rules(scope COLLATE NOCASE, confidence)",
        "CREATE INDEX IF NOT EXISTS idx_web_push_subscriptions_active_recipient "
        "ON web_push_subscriptions(lower(user_login), disabled_at, updated_at DESC)",
    )
    for statement in statements:
        con.execute(statement)
