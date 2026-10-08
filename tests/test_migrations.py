from concurrent.futures import ThreadPoolExecutor
import sqlite3
import threading

import pytest

from github_agent_bridge.queue import JobQueue
from github_agent_bridge.sql.migrations import Migration, apply_migrations


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
