from __future__ import annotations

import sqlite3


VERSION = 2
NAME = "durable GitHub commit status outbox"


def upgrade(con: sqlite3.Connection) -> None:
    con.execute(
        """CREATE TABLE IF NOT EXISTS job_commit_statuses (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          job_id INTEGER NOT NULL UNIQUE REFERENCES jobs(id) ON DELETE CASCADE,
          repo TEXT NOT NULL,
          sha TEXT,
          context_json TEXT NOT NULL,
          context_name TEXT NOT NULL,
          desired_state TEXT NOT NULL CHECK(desired_state IN ('pending','success','error')),
          description TEXT NOT NULL,
          revision INTEGER NOT NULL DEFAULT 1 CHECK(revision > 0),
          delivered_revision INTEGER NOT NULL DEFAULT 0 CHECK(delivered_revision >= 0),
          delivery_status TEXT NOT NULL DEFAULT 'pending' CHECK(delivery_status IN ('pending','processing','succeeded','failed')),
          attempts INTEGER NOT NULL DEFAULT 0,
          last_error TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )"""
    )
    con.execute(
        """CREATE INDEX IF NOT EXISTS idx_job_commit_statuses_delivery
        ON job_commit_statuses(delivery_status, updated_at)"""
    )
