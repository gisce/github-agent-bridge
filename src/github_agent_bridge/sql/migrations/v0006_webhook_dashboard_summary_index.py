from __future__ import annotations

import sqlite3


VERSION = 6
NAME = "webhook dashboard summary covering index"


def upgrade(con: sqlite3.Connection) -> None:
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_webhook_shadow_summary "
        "ON webhook_shadow_receipts(enqueue_status, status, duplicate_count)"
    )
