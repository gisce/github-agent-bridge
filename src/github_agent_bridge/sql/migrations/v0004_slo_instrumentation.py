from __future__ import annotations

import sqlite3


VERSION = 4
NAME = "SLO source timing and terminal outcomes"


def _columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f"PRAGMA table_info({table})")}


def upgrade(con: sqlite3.Connection) -> None:
    columns = _columns(con, "jobs")
    additions = {
        "source_received_at": "TEXT",
        "terminal_outcome": (
            "TEXT CHECK(terminal_outcome IN "
            "('completed','no_op','blocked','cancelled','denied','dismissed'))"
        ),
        "outcome_reason": "TEXT",
    }
    for column, definition in additions.items():
        if column not in columns:
            con.execute(f"ALTER TABLE jobs ADD COLUMN {column} {definition}")
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_finished_at "
        "ON jobs(finished_at) WHERE finished_at IS NOT NULL"
    )
