from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from ..models import GitHubContext, utc_now
from .database import Database, TransactionMode


ACK_RETRY_LIMIT = 2


@dataclass(frozen=True)
class AcknowledgementClaim:
    """One durable GitHub acknowledgement reserved for delivery."""

    id: int
    job_id: int
    context: GitHubContext


def acknowledgement_target_key(ctx: GitHubContext) -> str:
    """Return a stable identity for one GitHub reaction target."""
    return json.dumps(
        {
            "repo": ctx.repo,
            "issue_number": ctx.issue_number,
            "comment_id": ctx.comment_id,
            "review_comment_id": ctx.review_comment_id,
            "review_id": ctx.review_id,
            "commit_comment_id": ctx.commit_comment_id,
        },
        sort_keys=True,
    )


class AcknowledgementRepository:
    """Persist and reserve GitHub acknowledgement delivery records."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def add_pending(
        self,
        con: sqlite3.Connection,
        job_id: int,
        ctx: GitHubContext,
        created_at: str,
    ) -> None:
        """Add an acknowledgement inside the caller's ingestion transaction."""
        con.execute(
            """INSERT OR IGNORE INTO job_acknowledgements(
            job_id,target_key,context_json,status,created_at,updated_at
            ) VALUES(?,?,?,'pending',?,?)""",
            (job_id, acknowledgement_target_key(ctx), ctx.to_json(), created_at, created_at),
        )

    def claim(self, job_id: int | None = None) -> AcknowledgementClaim | None:
        """Reserve one acknowledgement before any external side effect."""
        now = utc_now()
        with self.database.transaction(
            TransactionMode.IMMEDIATE,
            operation="acknowledgements.claim",
        ) as con:
            job_filter = "AND a.job_id=?" if job_id is not None else ""
            status_filter = (
                "a.status IN ('pending','failed')"
                if job_id is not None
                else "a.status='pending'"
            )
            args = (ACK_RETRY_LIMIT, job_id) if job_id is not None else (ACK_RETRY_LIMIT,)
            row = con.execute(
                f"""SELECT a.id,a.job_id,a.context_json
                FROM job_acknowledgements a
                JOIN jobs j ON j.id=a.job_id
                WHERE {status_filter} AND a.attempts < ?
                AND j.status IN ('pending','running') {job_filter}
                ORDER BY a.created_at,a.id LIMIT 1""",
                args,
            ).fetchone()
            if row is None:
                return None
            con.execute(
                "UPDATE job_acknowledgements SET status='processing',attempts=attempts+1,updated_at=? WHERE id=?",
                (now, row["id"]),
            )
            return AcknowledgementClaim(
                id=int(row["id"]),
                job_id=int(row["job_id"]),
                context=GitHubContext.from_json(row["context_json"]),
            )

    def recover_interrupted(self) -> int:
        """Release acknowledgements interrupted by an executor restart."""
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="acknowledgements.recover_interrupted",
        ) as con:
            cursor = con.execute(
                "UPDATE job_acknowledgements SET status='pending',updated_at=? WHERE status='processing'",
                (utc_now(),),
            )
            return cursor.rowcount

    def finish(self, acknowledgement_id: int, ok: bool, error: str | None = None) -> None:
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="acknowledgements.finish",
        ) as con:
            con.execute(
                "UPDATE job_acknowledgements SET status=?,last_error=?,updated_at=? WHERE id=?",
                (
                    "succeeded" if ok else "failed",
                    None if ok else (error or "reaction failed")[:1000],
                    utc_now(),
                    acknowledgement_id,
                ),
            )

    def all_succeeded(self, job_id: int) -> bool:
        with self.database.read("acknowledgements.all_succeeded") as con:
            row = con.execute(
                "SELECT COUNT(*) AS total,SUM(status='succeeded') AS succeeded FROM job_acknowledgements WHERE job_id=?",
                (job_id,),
            ).fetchone()
        return bool(row and row["total"] and row["total"] == row["succeeded"])
