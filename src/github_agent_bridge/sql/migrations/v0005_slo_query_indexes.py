from __future__ import annotations

import sqlite3


VERSION = 5
NAME = "SLO ingestion cohort query indexes"


def upgrade(con: sqlite3.Connection) -> None:
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_coalesced_notifications_created_at "
        "ON coalesced_notifications(created_at)"
    )
