import logging
import sqlite3

import pytest

from github_agent_bridge.persistence import Database, TransactionMode


def test_read_write_connection_applies_operational_policy(tmp_path):
    database = Database(tmp_path / "nested" / "bridge.sqlite3", timeout_seconds=1.25)

    with database.read_write() as con:
        con.execute("CREATE TABLE example (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        con.execute("INSERT INTO example(value) VALUES(?)", ("stored",))
        row = con.execute("SELECT value FROM example").fetchone()
        policy = {
            "foreign_keys": con.execute("PRAGMA foreign_keys").fetchone()[0],
            "journal_mode": con.execute("PRAGMA journal_mode").fetchone()[0],
            "busy_timeout": con.execute("PRAGMA busy_timeout").fetchone()[0],
            "query_only": con.execute("PRAGMA query_only").fetchone()[0],
        }

    assert row["value"] == "stored"
    assert policy == {
        "foreign_keys": 1,
        "journal_mode": "wal",
        "busy_timeout": 1250,
        "query_only": 0,
    }
    assert database.path.parent.is_dir()


def test_read_only_connection_enforces_query_only_policy(tmp_path):
    database = Database(tmp_path / "bridge.sqlite3", timeout_seconds=0.75)
    with database.read_write() as con:
        con.execute("CREATE TABLE example (value TEXT NOT NULL)")
        con.execute("INSERT INTO example(value) VALUES('stored')")

    with database.read_only() as con:
        row = con.execute("SELECT value FROM example").fetchone()
        policy = {
            "foreign_keys": con.execute("PRAGMA foreign_keys").fetchone()[0],
            "busy_timeout": con.execute("PRAGMA busy_timeout").fetchone()[0],
            "query_only": con.execute("PRAGMA query_only").fetchone()[0],
        }
        with pytest.raises(sqlite3.OperationalError, match="readonly|read-only"):
            con.execute("INSERT INTO example(value) VALUES('forbidden')")

    assert row["value"] == "stored"
    assert policy == {"foreign_keys": 1, "busy_timeout": 750, "query_only": 1}


@pytest.mark.parametrize("mode", [TransactionMode.DEFERRED, TransactionMode.IMMEDIATE])
def test_transaction_commits_for_each_supported_mode(tmp_path, mode):
    database = Database(tmp_path / "bridge.sqlite3")
    with database.read_write() as con:
        con.execute("CREATE TABLE example (value TEXT NOT NULL)")

    with database.transaction(mode) as con:
        con.execute("INSERT INTO example(value) VALUES(?)", (mode.value,))

    with database.read_only() as con:
        assert con.execute("SELECT value FROM example").fetchone()["value"] == mode.value


def test_transaction_rolls_back_the_complete_unit_of_work(tmp_path):
    database = Database(tmp_path / "bridge.sqlite3")
    with database.read_write() as con:
        con.execute("CREATE TABLE example (value TEXT NOT NULL)")

    with pytest.raises(RuntimeError, match="abort unit of work"):
        with database.transaction(TransactionMode.IMMEDIATE) as con:
            con.execute("INSERT INTO example(value) VALUES('partial')")
            raise RuntimeError("abort unit of work")

    with database.read_only() as con:
        assert con.execute("SELECT count(*) FROM example").fetchone()[0] == 0


def test_named_read_records_slow_operation_without_sql_or_parameters(tmp_path, caplog):
    database = Database(
        tmp_path / "bridge.sqlite3",
        slow_operation_seconds=0,
    )
    with database.read_write() as con:
        con.execute("CREATE TABLE example (secret TEXT NOT NULL)")
        con.execute("INSERT INTO example(secret) VALUES(?)", ("sensitive-value",))

    with caplog.at_level(logging.WARNING):
        with database.read("tests.named_read") as con:
            assert con.execute("SELECT secret FROM example").fetchone()[0] == (
                "sensitive-value"
            )

    record = next(
        record for record in caplog.records if record.message == "sqlite_operation_slow"
    )
    assert record.sqlite_operation == "tests.named_read"
    assert record.sqlite_access == "read"
    assert record.sqlite_duration_ms >= 0
    assert "SELECT secret" not in caplog.text
    assert "sensitive-value" not in caplog.text


def test_named_transaction_records_sqlite_contention(tmp_path, caplog):
    database = Database(tmp_path / "bridge.sqlite3", timeout_seconds=0.01)
    with database.read_write() as con:
        con.execute("CREATE TABLE example (value TEXT NOT NULL)")
    blocker = database.read_write()
    blocker.execute("BEGIN IMMEDIATE")

    try:
        with caplog.at_level(logging.WARNING):
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                with database.transaction(
                    TransactionMode.IMMEDIATE,
                    operation="tests.contended_write",
                ):
                    pass
    finally:
        blocker.rollback()
        blocker.close()

    record = next(
        record for record in caplog.records if record.message == "sqlite_operation_busy"
    )
    assert record.sqlite_operation == "tests.contended_write"
    assert record.sqlite_access == "immediate"
    assert record.sqlite_duration_ms >= 0


def test_operation_names_reject_sql_and_free_form_values(tmp_path):
    database = Database(tmp_path / "bridge.sqlite3")

    with pytest.raises(ValueError, match="stable lowercase dotted name"):
        with database.read("SELECT secret FROM tokens"):
            pass
