from concurrent.futures import ThreadPoolExecutor
import sqlite3
import threading

import pytest

from github_agent_bridge.queue import JobQueue
from github_agent_bridge.sql.migrations import (
    Migration,
    MigrationRequiredError,
    apply_migrations,
)


def test_failed_migration_rolls_back_schema_and_history():
    con = sqlite3.connect(":memory:", isolation_level=None)
    con.execute("CREATE TABLE example (id INTEGER PRIMARY KEY)")

    def failing_upgrade(connection):
        connection.execute("ALTER TABLE example ADD COLUMN value TEXT")
        raise RuntimeError("backfill failed")

    migration = Migration(1, "failing migration", "test-checksum", failing_upgrade)

    with pytest.raises(RuntimeError, match="backfill failed"):
        apply_migrations(con, [migration])

    columns = {row[1] for row in con.execute("PRAGMA table_info(example)")}
    applied = con.execute("SELECT count(*) FROM schema_migrations").fetchone()[0]
    assert columns == {"id"}
    assert applied == 0


def test_migration_is_not_reapplied_after_backfill_is_recorded():
    con = sqlite3.connect(":memory:", isolation_level=None)
    con.execute("CREATE TABLE example (id INTEGER PRIMARY KEY, value TEXT)")
    con.execute("INSERT INTO example(id,value) VALUES(1,NULL)")
    calls = []

    def backfill(connection):
        calls.append("applied")
        connection.execute("UPDATE example SET value='backfilled' WHERE value IS NULL")

    migration = Migration(1, "backfill values", "test-checksum", backfill)

    assert apply_migrations(con, [migration]) == (1,)
    assert apply_migrations(con, [migration]) == ()
    assert calls == ["applied"]
    assert con.execute("SELECT value FROM example WHERE id=1").fetchone()[0] == "backfilled"


def test_concurrent_migration_callers_apply_each_step_once(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    with sqlite3.connect(db) as con:
        con.execute("CREATE TABLE example (id INTEGER PRIMARY KEY)")

    first_upgrade_started = threading.Event()
    contender_started_write = threading.Event()
    calls = []

    class ContendingConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            if sql == "BEGIN IMMEDIATE":
                contender_started_write.set()
            return super().execute(sql, parameters)

    def upgrade(connection):
        calls.append("applied")
        if len(calls) == 1:
            first_upgrade_started.set()
            assert contender_started_write.wait(timeout=5)
        connection.execute("ALTER TABLE example ADD COLUMN value TEXT")

    migration = Migration(1, "add value", "test-checksum", upgrade)

    def migrate(factory=sqlite3.Connection):
        with sqlite3.connect(db, isolation_level=None, timeout=5, factory=factory) as con:
            return apply_migrations(con, [migration])

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(migrate)
        assert first_upgrade_started.wait(timeout=5)
        contender = executor.submit(migrate, ContendingConnection)

        assert first.result(timeout=5) == (1,)
        assert contender.result(timeout=5) == ()

    assert calls == ["applied"]


def test_queue_refuses_newer_database_before_applying_schema_snapshot(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    queue = JobQueue(db)
    with queue.connect() as con:
        con.execute(
            "INSERT INTO schema_migrations(version,name,checksum,applied_at) VALUES(999,'future','future','2099-01-01T00:00:00Z')"
        )
        con.execute("DROP TABLE alerts")

    with pytest.raises(RuntimeError, match="newer than this package"):
        JobQueue(db)

    with sqlite3.connect(db) as con:
        alerts_exists = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='alerts'"
        ).fetchone()
    assert alerts_exists is None


def test_query_plan_index_migration_upgrades_existing_database(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    queue = JobQueue(db)
    expected = {
        "idx_ingest_receipts_coverage",
        "idx_webhook_shadow_coverage",
        "idx_coalesced_notifications_job_id",
        "idx_worklog_job_id",
        "idx_job_progress_job_id",
        "idx_feedback_rules_scope_nocase_confidence",
        "idx_web_push_subscriptions_active_recipient",
    }
    with queue.connect() as con:
        for index in expected:
            con.execute(f"DROP INDEX {index}")
        con.execute("DELETE FROM schema_migrations WHERE version=3")

    with pytest.raises(MigrationRequiredError, match="pending migrations 3"):
        JobQueue(db)

    JobQueue(db, migrate=True)
    with sqlite3.connect(db) as con:
        indexes = {
            str(row[0])
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
        }
    assert expected <= indexes


def test_slo_instrumentation_migration_adds_nullable_signals_without_backfill(
    tmp_path,
):
    db = tmp_path / "bridge.sqlite3"
    queue = JobQueue(db)
    with queue.connect() as con:
        con.execute(
            "INSERT INTO jobs("
            "work_key,status,action,decision,work_intent,subject,message_id,"
            "context_json,metadata_json,created_at,updated_at,finished_at"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "gisce/repo#1",
                "done",
                "reply_comment",
                "auto_trusted",
                "review_only",
                "legacy",
                "<legacy@github.com>",
                '{"urls": []}',
                "{}",
                "2026-10-01T10:00:00Z",
                "2026-10-01T10:01:00Z",
                "2026-10-01T10:01:00Z",
            ),
        )
        con.execute("DROP INDEX idx_jobs_finished_at")
        con.execute("ALTER TABLE jobs DROP COLUMN outcome_reason")
        con.execute("ALTER TABLE jobs DROP COLUMN terminal_outcome")
        con.execute("ALTER TABLE jobs DROP COLUMN source_received_at")
        con.execute("DELETE FROM schema_migrations WHERE version=4")

    with pytest.raises(MigrationRequiredError, match="pending migrations 4"):
        JobQueue(db)

    JobQueue(db, migrate=True)

    with sqlite3.connect(db) as con:
        columns = {row[1] for row in con.execute("PRAGMA table_info(jobs)")}
        historical = con.execute(
            "SELECT source_received_at,terminal_outcome,outcome_reason "
            "FROM jobs WHERE message_id='<legacy@github.com>'"
        ).fetchone()
        index_exists = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' "
            "AND name='idx_jobs_finished_at'"
        ).fetchone()

    assert {"source_received_at", "terminal_outcome", "outcome_reason"} <= columns
    assert historical == (None, None, None)
    assert index_exists == (1,)
