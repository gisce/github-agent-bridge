from __future__ import annotations

import sqlite3


VERSION = 7
NAME = "ingest receipt decision trace"


def upgrade(con: sqlite3.Connection) -> None:
    columns = {str(row[1]) for row in con.execute("PRAGMA table_info(ingest_receipts)")}
    additions = {
        "repository": "TEXT",
        "work_key": "TEXT",
        "event_type": "TEXT",
        "event_action": "TEXT",
        "bridge_action": "TEXT",
        "decision": "TEXT",
        "work_intent": "TEXT",
    }
    for name, definition in additions.items():
        if name not in columns:
            con.execute(
                f"ALTER TABLE ingest_receipts ADD COLUMN {name} {definition}"
            )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_ingest_receipts_decision_trace "
        "ON ingest_receipts(event_key, source, bridge_action, work_intent, work_key)"
    )
