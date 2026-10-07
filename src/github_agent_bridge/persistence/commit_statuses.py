from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from ..models import GitHubContext, utc_now
from .database import Database, TransactionMode


COMMIT_STATUS_CONTEXT = "github-agent-bridge/agent"
COMMIT_STATUS_RETRY_LIMIT = 3


@dataclass(frozen=True)
class CommitStatusClaim:
    id: int
    job_id: int
    repo: str
    sha: str | None
    github_context: GitHubContext
    context_name: str
    desired_state: str
    description: str
    revision: int


class CommitStatusRepository:
    """Durable desired-state outbox for GitHub commit statuses."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def add_for_job(
        self,
        con: sqlite3.Connection,
        job_id: int,
        ctx: GitHubContext,
        desired_state: str,
        description: str,
        created_at: str,
    ) -> bool:
        if not ctx.supports_commit_status or not ctx.repo:
            return False
        # A PR/issue comment body may mention an unrelated commit URL. Only
        # direct commit targets may seed the pinned SHA from parsed content;
        # PR jobs resolve their actual head once during delivery.
        sha = (
            ctx.commit_sha
            if ctx.target_kind in {"commit", "commit_comment"}
            else None
        )
        cursor = con.execute(
            """INSERT OR IGNORE INTO job_commit_statuses(
                job_id,repo,sha,context_json,context_name,desired_state,description,
                revision,delivered_revision,delivery_status,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,1,0,'pending',?,?)""",
            (
                job_id,
                ctx.repo,
                sha,
                ctx.to_json(),
                COMMIT_STATUS_CONTEXT,
                desired_state,
                description[:140],
                created_at,
                created_at,
            ),
        )
        return bool(cursor.rowcount)

    def set_desired(
        self,
        con: sqlite3.Connection,
        job_id: int,
        desired_state: str,
        description: str,
        updated_at: str,
    ) -> bool:
        cursor = con.execute(
            """UPDATE job_commit_statuses
            SET desired_state=?,description=?,revision=revision+1,
                delivery_status='pending',attempts=0,last_error=NULL,updated_at=?
            WHERE job_id=? AND (desired_state!=? OR description!=?)""",
            (
                desired_state,
                description[:140],
                updated_at,
                job_id,
                desired_state,
                description[:140],
            ),
        )
        return bool(cursor.rowcount)

    def claim(self, job_id: int | None = None) -> CommitStatusClaim | None:
        now = utc_now()
        with self.database.transaction(TransactionMode.IMMEDIATE) as con:
            job_filter = "AND job_id=?" if job_id is not None else ""
            args: tuple[object, ...] = (
                (COMMIT_STATUS_RETRY_LIMIT, job_id)
                if job_id is not None
                else (COMMIT_STATUS_RETRY_LIMIT,)
            )
            row = con.execute(
                f"""SELECT * FROM job_commit_statuses
                WHERE delivery_status IN ('pending','failed')
                  AND attempts < ?
                  AND delivered_revision < revision
                  {job_filter}
                ORDER BY created_at,job_id LIMIT 1""",
                args,
            ).fetchone()
            if row is None:
                return None
            con.execute(
                """UPDATE job_commit_statuses
                SET delivery_status='processing',attempts=attempts+1,updated_at=?
                WHERE id=?""",
                (now, row["id"]),
            )
            return CommitStatusClaim(
                id=int(row["id"]),
                job_id=int(row["job_id"]),
                repo=str(row["repo"]),
                sha=str(row["sha"]) if row["sha"] else None,
                github_context=GitHubContext.from_json(row["context_json"]),
                context_name=str(row["context_name"]),
                desired_state=str(row["desired_state"]),
                description=str(row["description"]),
                revision=int(row["revision"]),
            )

    def pin_sha(self, status_id: int, sha: str) -> str:
        clean_sha = sha.strip()
        if not clean_sha:
            raise ValueError("commit status SHA cannot be empty")
        with self.database.transaction(TransactionMode.IMMEDIATE) as con:
            con.execute(
                "UPDATE job_commit_statuses SET sha=COALESCE(sha,?),updated_at=? WHERE id=?",
                (clean_sha, utc_now(), status_id),
            )
            row = con.execute(
                "SELECT sha FROM job_commit_statuses WHERE id=?", (status_id,)
            ).fetchone()
            if row is None or not row["sha"]:
                raise ValueError(f"commit status {status_id} no longer exists")
            return str(row["sha"])

    def finish(
        self,
        status_id: int,
        revision: int,
        ok: bool,
        error: str | None = None,
    ) -> None:
        now = utc_now()
        with self.database.transaction(TransactionMode.IMMEDIATE) as con:
            row = con.execute(
                "SELECT revision FROM job_commit_statuses WHERE id=?", (status_id,)
            ).fetchone()
            if row is None:
                return
            newer_desired_state = int(row["revision"]) > revision
            if ok:
                con.execute(
                    """UPDATE job_commit_statuses
                    SET delivered_revision=MAX(delivered_revision,?),
                        delivery_status=?,last_error=NULL,updated_at=?
                    WHERE id=?""",
                    (
                        revision,
                        "pending" if newer_desired_state else "succeeded",
                        now,
                        status_id,
                    ),
                )
            elif newer_desired_state:
                con.execute(
                    """UPDATE job_commit_statuses
                    SET delivery_status='pending',last_error=NULL,updated_at=?
                    WHERE id=?""",
                    (now, status_id),
                )
            else:
                con.execute(
                    """UPDATE job_commit_statuses
                    SET delivery_status='failed',last_error=?,updated_at=?
                    WHERE id=?""",
                    ((error or "GitHub commit status delivery failed")[:1000], now, status_id),
                )

    def recover_interrupted(self) -> int:
        with self.database.transaction() as con:
            cursor = con.execute(
                """UPDATE job_commit_statuses
                SET delivery_status='pending',updated_at=?
                WHERE delivery_status='processing'""",
                (utc_now(),),
            )
            return cursor.rowcount
