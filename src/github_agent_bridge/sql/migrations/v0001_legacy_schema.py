from __future__ import annotations

import json
import sqlite3
from urllib.parse import urlparse

from ...session_correlation import session_id_for_job_attempt


VERSION = 1
NAME = "legacy schema compatibility and backfills"


ADDITIVE_COLUMNS = {
    "jobs": {"trigger_actor": "TEXT", "trigger_actor_avatar_url": "TEXT"},
    "coalesced_notifications": {
        "trigger_actor": "TEXT",
        "trigger_actor_avatar_url": "TEXT",
    },
    "mcp_tokens": {"user_login": "TEXT", "created_by": "TEXT"},
    "webhook_shadow_receipts": {
        "duplicate_count": "INTEGER NOT NULL DEFAULT 0",
        "hook_id": "TEXT",
        "payload_json": "TEXT",
        "enqueue_status": "TEXT",
        "job_id": "INTEGER REFERENCES jobs(id) ON DELETE SET NULL",
    },
    "webhook_hooks": {
        "name": "TEXT",
        "content_type": "TEXT",
        "insecure_ssl": "INTEGER",
        "delivery_url": "TEXT",
        "github_api_url": "TEXT",
        "ping_url": "TEXT",
        "deliveries_url": "TEXT",
        "github_created_at": "TEXT",
        "github_updated_at": "TEXT",
        "last_delivery_id": "TEXT",
        "last_event_name": "TEXT",
        "last_action": "TEXT",
        "last_repository": "TEXT",
        "last_result": "TEXT",
    },
}


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    return (
        con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        is not None
    )


def _columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f"PRAGMA table_info({table})")}


def _add_columns(con: sqlite3.Connection) -> None:
    for table, columns in ADDITIVE_COLUMNS.items():
        if not _table_exists(con, table):
            continue
        existing = _columns(con, table)
        for column, definition in columns.items():
            if column not in existing:
                con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _ensure_indexes(con: sqlite3.Connection) -> None:
    statements = {
        "webhook_shadow_receipts": (
            "CREATE INDEX IF NOT EXISTS idx_webhook_shadow_delivery_page ON webhook_shadow_receipts(created_at DESC, delivery_id DESC)",
            "CREATE INDEX IF NOT EXISTS idx_webhook_shadow_job_id ON webhook_shadow_receipts(job_id)",
        ),
        "webhook_hooks": (
            "CREATE INDEX IF NOT EXISTS idx_webhook_hooks_page ON webhook_hooks(updated_at DESC, hook_id DESC)",
        ),
        "mcp_tokens": (
            "CREATE INDEX IF NOT EXISTS idx_mcp_tokens_user ON mcp_tokens(user_login, revoked_at, created_at)",
        ),
        "quarantined_notifications": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_quarantined_notifications_message_id ON quarantined_notifications(message_id) WHERE message_id IS NOT NULL AND message_id != ''",
            "CREATE INDEX IF NOT EXISTS idx_quarantined_notifications_unresolved ON quarantined_notifications(resolved_at, created_at)",
        ),
    }
    for table, table_statements in statements.items():
        if not _table_exists(con, table):
            continue
        for statement in table_statements:
            con.execute(statement)


def _backfill_job_runs(con: sqlite3.Connection) -> None:
    if not _table_exists(con, "jobs") or not _table_exists(con, "job_runs"):
        return
    required = {"id", "attempts", "started_at", "finished_at", "locked_by", "metadata_json"}
    if not required <= _columns(con, "jobs"):
        return
    rows = con.execute(
        """SELECT id, attempts, started_at, finished_at, locked_by, metadata_json
        FROM jobs
        WHERE started_at IS NOT NULL
          AND finished_at IS NOT NULL
          AND julianday(finished_at) >= julianday(started_at)
          AND NOT EXISTS (SELECT 1 FROM job_runs WHERE job_runs.job_id=jobs.id)"""
    ).fetchall()
    for row in rows:
        try:
            metadata = json.loads(row[5] or "{}")
        except (json.JSONDecodeError, TypeError):
            metadata = {}
        attempt = max(1, int(row[1] or 0))
        session_id = str(
            metadata.get("openclaw_session_id")
            or session_id_for_job_attempt(int(row[0]), attempt)
        )
        con.execute(
            """INSERT OR IGNORE INTO job_runs(
                job_id,attempt,started_at,finished_at,result,worker_id,session_id,is_estimated
            ) VALUES(?,?,?,?,?,?,?,1)""",
            (row[0], attempt, row[2], row[3], "historical", row[4], session_id),
        )


def _hook_target(api_url: str) -> tuple[str, str] | None:
    parts = [part for part in urlparse(api_url).path.split("/") if part]
    if len(parts) >= 4 and parts[0] == "repos" and parts[3] == "hooks":
        return f"{parts[1]}/{parts[2]}", "repository"
    if len(parts) >= 3 and parts[0] == "orgs" and parts[2] == "hooks":
        return parts[1], "organization"
    return None


def _backfill_webhook_hook_targets(con: sqlite3.Connection) -> None:
    if not _table_exists(con, "webhook_hooks"):
        return
    if not {"hook_id", "target", "target_type", "github_api_url"} <= _columns(
        con, "webhook_hooks"
    ):
        return
    rows = con.execute(
        "SELECT hook_id,target,target_type,github_api_url FROM webhook_hooks WHERE github_api_url IS NOT NULL"
    ).fetchall()
    for hook_id, current_target, current_type, github_api_url in rows:
        target = _hook_target(str(github_api_url or ""))
        if target is None or (current_target, current_type) == target:
            continue
        con.execute(
            "UPDATE webhook_hooks SET target=?,target_type=? WHERE hook_id=?",
            (target[0], target[1], hook_id),
        )


def upgrade(con: sqlite3.Connection) -> None:
    _add_columns(con)
    _ensure_indexes(con)
    _backfill_job_runs(con)
    _backfill_webhook_hook_targets(con)
