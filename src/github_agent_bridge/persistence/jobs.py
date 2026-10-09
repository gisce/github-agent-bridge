from __future__ import annotations

import json
import sqlite3

from ..models import GitHubContext, Job, utc_now
from ..session_correlation import session_id_for_job, session_id_for_job_attempt
from .commit_statuses import CommitStatusRepository
from .database import Database, TransactionMode
from .runtime import RuntimeRepository
from .state import StateRepository

ACTIVE_JOB_STATUSES = ("pending", "running", "waiting_approval")


def active_job_counts(database: Database) -> dict[str, int]:
    """Read migration-blocking job counts without constructing a queue."""
    with database.read("jobs.active_job_counts") as con:
        jobs_exists = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='jobs'"
        ).fetchone()
        if jobs_exists is None:
            return {status: 0 for status in ACTIVE_JOB_STATUSES}
        placeholders = ",".join("?" for _ in ACTIVE_JOB_STATUSES)
        rows = con.execute(
            f"SELECT status,count(*) count FROM jobs "
            f"WHERE status IN ({placeholders}) GROUP BY status",
            ACTIVE_JOB_STATUSES,
        ).fetchall()
    counts = {status: 0 for status in ACTIVE_JOB_STATUSES}
    counts.update({str(row["status"]): int(row["count"]) for row in rows})
    return counts


def job_from_row(row: sqlite3.Row | None) -> Job | None:
    """Map one persistence row to the public queue job DTO."""
    if row is None:
        return None
    return Job(
        id=row["id"],
        work_key=row["work_key"],
        repo=row["repo"],
        thread=row["thread"],
        status=row["status"],
        action=row["action"],
        work_intent=row["work_intent"],
        subject=row["subject"],
        message_id=row["message_id"],
        uid=row["uid"],
        trigger_actor=row["trigger_actor"],
        trigger_actor_avatar_url=row["trigger_actor_avatar_url"],
        context=GitHubContext.from_json(row["context_json"]),
        attempts=row["attempts"],
        coalesced_count=row["coalesced_count"],
        last_error=row["last_error"],
        locked_by=row["locked_by"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        metadata=json.loads(row["metadata_json"] or "{}"),
    )


class JobRepository:
    """Persist job lifecycle transitions and map rows to typed jobs."""

    def __init__(
        self,
        database: Database,
        runtime: RuntimeRepository,
        state: StateRepository,
        commit_statuses: CommitStatusRepository,
    ) -> None:
        self.database = database
        self.runtime = runtime
        self.state = state
        self.commit_statuses = commit_statuses

    def claim_next(
        self,
        worker_id: str,
        work_intents: frozenset[str] | set[str] | None = None,
    ) -> Job | None:
        now = utc_now()
        with self.database.transaction(
            TransactionMode.IMMEDIATE,
            operation="jobs.claim_next",
        ) as con:
            # Serialize the pause check with claims so a completed pause blocks later claims.
            if self.state.executor_paused(connection=con):
                return None
            intent_filter = ""
            args: list[object] = []
            if work_intents is not None:
                if not work_intents:
                    return None
                intent_filter = (
                    f"AND j.work_intent IN ({','.join('?' for _ in work_intents)})"
                )
                args.extend(sorted(work_intents))
            row = con.execute(
                f"""SELECT * FROM jobs j WHERE j.status='pending'
                {intent_filter}
                AND NOT EXISTS (SELECT 1 FROM jobs r WHERE r.work_key=j.work_key AND r.status='running')
                ORDER BY j.created_at LIMIT 1""",
                args,
            ).fetchone()
            if row is None:
                return None
            metadata = json.loads(row["metadata_json"] or "{}")
            metadata.pop("runtime_process", None)
            fresh_session = bool(metadata.get("fresh_session_on_retry")) and int(
                row["attempts"]
            ) > 0
            if fresh_session or row["work_intent"] == "work_allowed":
                metadata["openclaw_session_id"] = session_id_for_job_attempt(
                    int(row["id"]), int(row["attempts"]) + 1
                )
            else:
                metadata.setdefault(
                    "openclaw_session_id", session_id_for_job(int(row["id"]))
                )
            attempt = int(row["attempts"]) + 1
            con.execute(
                "UPDATE jobs SET status='running', locked_by=?, attempts=attempts+1, started_at=?, finished_at=NULL, updated_at=?, metadata_json=? WHERE id=?",
                (
                    worker_id,
                    now,
                    now,
                    json.dumps(metadata, sort_keys=True),
                    row["id"],
                ),
            )
            self.runtime.record_job_run_started(
                con,
                int(row["id"]),
                attempt,
                now,
                worker_id,
                str(metadata["openclaw_session_id"]),
            )
            self.runtime.record_worklog(
                con,
                row["id"],
                row["work_key"],
                "running",
                f"claimed by {worker_id}",
                None,
            )
            self.runtime.record_session_event(
                con,
                row["id"],
                row["work_key"],
                metadata["openclaw_session_id"],
                "claimed",
                f"claimed by {worker_id}",
                None,
            )
            self.runtime.record_progress(
                con,
                row["id"],
                row["work_key"],
                "semantic",
                "claimed",
                f"claimed by {worker_id}",
                None,
            )
            self.commit_statuses.set_desired(
                con,
                int(row["id"]),
                "pending",
                f"Agent working (attempt {attempt})",
                now,
            )
            return job_from_row(
                con.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone()
            )

    def finish(
        self,
        job_id: int,
        status: str,
        summary: str,
        detail: str | None = None,
    ) -> None:
        now = utc_now()
        with self.database.transaction(
            TransactionMode.IMMEDIATE,
            operation="jobs.finish",
        ) as con:
            row = con.execute(
                "SELECT work_key FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
            metadata = self.runtime.job_metadata(con, job_id)
            cancellation = metadata.get("cancellation")
            cancelled = isinstance(cancellation, dict) and cancellation.get(
                "state"
            ) in {"requested", "cancelled"}
            if cancelled:
                status = "done"
                summary = str(cancellation.get("summary") or summary)
                detail = str(cancellation.get("detail") or detail or "")
                con.execute(
                    "UPDATE jobs SET status=?, last_error=?, locked_by=NULL, finished_at=COALESCE(finished_at, ?), updated_at=? WHERE id=?",
                    (status, detail if status == "blocked" else None, now, now, job_id),
                )
            else:
                con.execute(
                    "UPDATE jobs SET status=?, last_error=?, locked_by=NULL, finished_at=?, updated_at=? WHERE id=?",
                    (status, detail if status == "blocked" else None, now, now, job_id),
                )
            self.runtime.record_job_run_finished(
                con, job_id, "cancelled" if cancelled else status, now
            )
            work_key = row["work_key"] if row else None
            self.runtime.record_worklog(
                con, job_id, work_key, status, summary, detail
            )
            session_id = metadata.get("openclaw_session_id") or session_id_for_job(
                job_id
            )
            self.runtime.record_session_event(
                con, job_id, work_key, str(session_id), status, summary, detail
            )
            self.runtime.record_progress(
                con, job_id, work_key, "semantic", status, summary, detail
            )
            if cancelled:
                self.commit_statuses.set_desired(
                    con,
                    job_id,
                    "error",
                    "Agent cancelled; attention required",
                    now,
                )
            elif status == "done":
                description = (
                    "No agent action needed"
                    if "skipped" in summary or "not addressed" in summary
                    else "Agent finished; follow-up available"
                )
                self.commit_statuses.set_desired(
                    con, job_id, "success", description, now
                )
            elif status == "blocked":
                self.commit_statuses.set_desired(
                    con,
                    job_id,
                    "error",
                    "Agent blocked; attention required",
                    now,
                )

    def request_cancel_running(
        self,
        job_id: int,
        *,
        actor: str,
        reason: str | None = None,
    ) -> Job | None:
        now = utc_now()
        clean_actor = actor.strip().lstrip("@") or "unknown"
        clean_reason = (reason or "").strip()
        summary = f"job cancellation requested by @{clean_actor}"
        detail = f"reason={clean_reason}" if clean_reason else "reason not provided"
        with self.database.transaction(
            TransactionMode.IMMEDIATE,
            operation="jobs.request_cancel_running",
        ) as con:
            row = con.execute(
                "SELECT work_key, metadata_json FROM jobs WHERE id=? AND status='running'",
                (job_id,),
            ).fetchone()
            if row is None:
                return None
            metadata = json.loads(row["metadata_json"] or "{}")
            metadata["cancellation"] = {
                "state": "requested",
                "actor": clean_actor,
                "reason": clean_reason,
                "requested_at": now,
                "summary": summary,
                "detail": detail,
            }
            con.execute(
                "UPDATE jobs SET updated_at=?, metadata_json=? WHERE id=? AND status='running'",
                (now, json.dumps(metadata, sort_keys=True), job_id),
            )
            self.runtime.record_worklog(
                con,
                job_id,
                row["work_key"],
                "cancel_requested",
                summary,
                detail,
            )
            session_id = str(
                metadata.get("openclaw_session_id") or session_id_for_job(job_id)
            )
            self.runtime.record_session_event(
                con,
                job_id,
                row["work_key"],
                session_id,
                "cancel_requested",
                summary,
                detail,
            )
            self.runtime.record_progress(
                con,
                job_id,
                row["work_key"],
                "semantic",
                "cancel_requested",
                summary,
                detail,
            )
            return job_from_row(
                con.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            )

    def mark_cancelled(
        self,
        job_id: int,
        *,
        actor: str,
        reason: str | None = None,
        signal_detail: str | None = None,
        followup_url: str | None = None,
    ) -> Job | None:
        now = utc_now()
        clean_actor = actor.strip().lstrip("@") or "unknown"
        clean_reason = (reason or "").strip()
        summary = f"job cancelled by @{clean_actor}"
        detail_parts = []
        if clean_reason:
            detail_parts.append(f"reason={clean_reason}")
        if signal_detail:
            detail_parts.append(signal_detail)
        if followup_url:
            detail_parts.append(f"followup_url={followup_url}")
        detail = "; ".join(detail_parts) if detail_parts else "manual cancellation"
        with self.database.transaction(
            TransactionMode.IMMEDIATE,
            operation="jobs.mark_cancelled",
        ) as con:
            row = con.execute(
                "SELECT status, work_key, metadata_json FROM jobs WHERE id=?",
                (job_id,),
            ).fetchone()
            if row is None:
                return None
            metadata = json.loads(row["metadata_json"] or "{}")
            existing_cancellation = metadata.get("cancellation")
            metadata["cancellation"] = {
                "state": "cancelled",
                "actor": clean_actor,
                "reason": clean_reason,
                "cancelled_at": now,
                "signal_detail": signal_detail,
                "followup_url": followup_url,
                "summary": summary,
                "detail": detail,
            }
            if row["status"] != "running" and not (
                isinstance(existing_cancellation, dict)
                and existing_cancellation.get("state") == "requested"
            ):
                return None
            cursor = con.execute(
                """UPDATE jobs
                SET status='done', locked_by=NULL, last_error=NULL, finished_at=COALESCE(finished_at, ?), updated_at=?, metadata_json=?
                WHERE id=?""",
                (now, now, json.dumps(metadata, sort_keys=True), job_id),
            )
            if not cursor.rowcount:
                return None
            self.runtime.record_job_run_finished(con, job_id, "cancelled", now)
            self.runtime.record_worklog(
                con, job_id, row["work_key"], "cancelled", summary, detail
            )
            session_id = str(
                metadata.get("openclaw_session_id") or session_id_for_job(job_id)
            )
            self.runtime.record_session_event(
                con,
                job_id,
                row["work_key"],
                session_id,
                "cancelled",
                summary,
                detail,
            )
            self.runtime.record_progress(
                con,
                job_id,
                row["work_key"],
                "semantic",
                "cancelled",
                summary,
                detail,
            )
            self.commit_statuses.set_desired(
                con,
                job_id,
                "error",
                "Agent cancelled; attention required",
                now,
            )
            return job_from_row(
                con.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            )

    def requeue_running(
        self,
        job_id: int,
        summary: str,
        detail: str | None = None,
        *,
        fresh_session: bool = False,
    ) -> bool:
        now = utc_now()
        with self.database.transaction(
            TransactionMode.IMMEDIATE,
            operation="jobs.requeue_running",
        ) as con:
            row = con.execute(
                "SELECT work_key, metadata_json FROM jobs WHERE id=? AND status='running'",
                (job_id,),
            ).fetchone()
            if row is None:
                return False
            metadata = json.loads(row["metadata_json"] or "{}")
            if fresh_session:
                metadata["fresh_session_on_retry"] = True
            cursor = con.execute(
                "UPDATE jobs SET status='pending', locked_by=NULL, last_error=NULL, finished_at=?, updated_at=?, metadata_json=? WHERE id=? AND status='running'",
                (now, now, json.dumps(metadata, sort_keys=True), job_id),
            )
            if cursor.rowcount:
                self.runtime.record_job_run_finished(con, job_id, "requeued", now)
                self.runtime.record_worklog(
                    con, job_id, row["work_key"], "retry", summary, detail
                )
                attempts = con.execute(
                    "SELECT attempts FROM jobs WHERE id=?", (job_id,)
                ).fetchone()["attempts"]
                self.commit_statuses.set_desired(
                    con,
                    job_id,
                    "pending",
                    f"Agent retry scheduled (attempt {int(attempts) + 1})",
                    now,
                )
            return bool(cursor.rowcount)

    def block_running(
        self,
        summary: str,
        detail: str,
        *,
        job_ids: list[int] | None = None,
        locked_by: set[str] | None = None,
        older_than_seconds: int | None = None,
    ) -> list[int]:
        now = utc_now()
        with self.database.transaction(
            TransactionMode.IMMEDIATE,
            operation="jobs.block_running",
        ) as con:
            clauses = ["status='running'"]
            args: list[object] = []
            if job_ids is not None:
                if not job_ids:
                    return []
                clauses.append(f"id IN ({','.join('?' for _ in job_ids)})")
                args.extend(job_ids)
            if locked_by is not None:
                if not locked_by:
                    return []
                clauses.append(f"locked_by IN ({','.join('?' for _ in locked_by)})")
                args.extend(sorted(locked_by))
            if older_than_seconds is not None:
                clauses.append(
                    "started_at IS NOT NULL AND "
                    "(julianday('now') - julianday(started_at)) * 86400 > ?"
                )
                args.append(older_than_seconds)
            rows = con.execute(
                f"SELECT id, work_key, metadata_json FROM jobs WHERE {' AND '.join(clauses)} ORDER BY id",
                args,
            ).fetchall()
            blocked_ids: list[int] = []
            for row in rows:
                cursor = con.execute(
                    """UPDATE jobs
                    SET status='blocked', locked_by=NULL, last_error=?,
                        finished_at=?, updated_at=?
                    WHERE id=? AND status='running'""",
                    (detail, now, now, row["id"]),
                )
                if not cursor.rowcount:
                    continue
                job_id = int(row["id"])
                blocked_ids.append(job_id)
                self.runtime.record_job_run_finished(con, job_id, "blocked", now)
                self.runtime.record_worklog(
                    con, job_id, row["work_key"], "blocked", summary, detail
                )
                metadata = json.loads(row["metadata_json"] or "{}")
                session_id = str(
                    metadata.get("openclaw_session_id")
                    or session_id_for_job(job_id)
                )
                self.runtime.record_session_event(
                    con,
                    job_id,
                    row["work_key"],
                    session_id,
                    "blocked",
                    summary,
                    detail,
                )
                self.runtime.record_progress(
                    con,
                    job_id,
                    row["work_key"],
                    "semantic",
                    "blocked",
                    summary,
                    detail,
                )
                self.commit_statuses.set_desired(
                    con,
                    job_id,
                    "error",
                    "Agent blocked; attention required",
                    now,
                )
            return blocked_ids

    def update_work_intent(
        self, job_id: int, work_intent: str, summary: str
    ) -> Job | None:
        now = utc_now()
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="jobs.update_work_intent",
        ) as con:
            row = con.execute(
                "SELECT work_key FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
            if row is None:
                return None
            con.execute(
                "UPDATE jobs SET work_intent=?, updated_at=? WHERE id=?",
                (work_intent, now, job_id),
            )
            self.runtime.record_worklog(
                con, job_id, row["work_key"], "intent_update", summary, None
            )
            return job_from_row(
                con.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            )

    def list(self, status: str | None = None, limit: int = 20) -> list[Job]:
        sql = "SELECT * FROM jobs"
        args: tuple[object, ...] = ()
        if status:
            sql += " WHERE status=?"
            args = (status,)
        sql += " ORDER BY id DESC LIMIT ?"
        args = (*args, limit)
        with self.database.read("jobs.list") as con:
            return [
                job
                for job in (job_from_row(row) for row in con.execute(sql, args))
                if job is not None
            ]

    def retry(self, job_id: int, *, actor: str | None = None) -> bool:
        now = utc_now()
        summary = f"job requeued by @{actor}" if actor else "job requeued"
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="jobs.retry",
        ) as con:
            cursor = con.execute(
                "UPDATE jobs SET status='pending', locked_by=NULL, last_error=NULL, updated_at=? WHERE id=? AND status='blocked' AND decision='auto_trusted'",
                (now, job_id),
            )
            if cursor.rowcount:
                row = con.execute(
                    "SELECT work_key,attempts FROM jobs WHERE id=?", (job_id,)
                ).fetchone()
                self.runtime.record_worklog(
                    con,
                    job_id,
                    row["work_key"] if row else None,
                    "retry",
                    summary,
                    None,
                )
                self.commit_statuses.set_desired(
                    con,
                    job_id,
                    "pending",
                    f"Agent retry scheduled (attempt {int(row['attempts']) + 1})",
                    now,
                )
            return bool(cursor.rowcount)

    def dismiss(self, job_id: int, reason: str) -> bool:
        now = utc_now()
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="jobs.dismiss",
        ) as con:
            cursor = con.execute(
                "UPDATE jobs SET status='done', locked_by=NULL, last_error=NULL, finished_at=COALESCE(finished_at, ?), updated_at=? WHERE id=? AND status IN ('blocked','denied','waiting_approval')",
                (now, now, job_id),
            )
            if cursor.rowcount:
                row = con.execute(
                    "SELECT work_key FROM jobs WHERE id=?", (job_id,)
                ).fetchone()
                self.runtime.record_worklog(
                    con,
                    job_id,
                    row["work_key"] if row else None,
                    "dismissed",
                    "job dismissed manually",
                    reason,
                )
            return bool(cursor.rowcount)

    def unlock_stale(
        self, older_than_seconds: int, job_ids: list[int] | None = None
    ) -> int:
        with self.database.transaction(
            TransactionMode.IMMEDIATE,
            operation="jobs.unlock_stale",
        ) as con:
            args: list[object] = [older_than_seconds]
            sql = "SELECT id, work_key FROM jobs WHERE status='running' AND started_at IS NOT NULL AND (julianday('now') - julianday(started_at)) * 86400 > ?"
            if job_ids is not None:
                if not job_ids:
                    return 0
                sql += f" AND id IN ({','.join('?' for _ in job_ids)})"
                args.extend(job_ids)
            rows = con.execute(sql, args).fetchall()
            now = utc_now()
            for row in rows:
                con.execute(
                    "UPDATE jobs SET status='pending', locked_by=NULL, finished_at=?, updated_at=? WHERE id=?",
                    (now, now, row["id"]),
                )
                self.runtime.record_job_run_finished(
                    con, int(row["id"]), "requeued", now
                )
                self.runtime.record_worklog(
                    con,
                    row["id"],
                    row["work_key"],
                    "unlock_stale",
                    f"running job older than {older_than_seconds}s requeued",
                    None,
                )
                attempts = con.execute(
                    "SELECT attempts FROM jobs WHERE id=?", (row["id"],)
                ).fetchone()["attempts"]
                self.commit_statuses.set_desired(
                    con,
                    int(row["id"]),
                    "pending",
                    f"Agent retry scheduled (attempt {int(attempts) + 1})",
                    now,
                )
            return len(rows)

    def get(self, job_id: int) -> Job | None:
        with self.database.read("jobs.get") as con:
            return job_from_row(
                con.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            )

    def find_by_message_id(self, message_id: str) -> Job | None:
        with self.database.read("jobs.find_by_message_id") as con:
            return job_from_row(
                con.execute(
                    "SELECT * FROM jobs WHERE message_id=?", (message_id,)
                ).fetchone()
            )

    def coalesced_contexts(self, job_id: int) -> list[GitHubContext]:
        with self.database.read("jobs.coalesced_contexts") as con:
            rows = con.execute(
                "SELECT context_json FROM coalesced_notifications WHERE job_id=? ORDER BY id",
                (job_id,),
            ).fetchall()
        return [GitHubContext.from_json(row["context_json"]) for row in rows]

    def coalesced_trigger_actors(self, job_id: int) -> list[str]:
        with self.database.read("jobs.coalesced_trigger_actors") as con:
            rows = con.execute(
                "SELECT trigger_actor FROM coalesced_notifications WHERE job_id=? ORDER BY id",
                (job_id,),
            ).fetchall()
        return [row["trigger_actor"] for row in rows if row["trigger_actor"]]

    def stats(self) -> dict[str, int]:
        with self.database.read("jobs.stats") as con:
            return {
                row["status"]: row["count"]
                for row in con.execute(
                    "SELECT status, count(*) count FROM jobs GROUP BY status"
                )
            }

    def pending_age_seconds(self) -> int | None:
        with self.database.read("jobs.pending_age_seconds") as con:
            row = con.execute(
                "SELECT CAST((julianday('now') - julianday(min(created_at))) * 86400 AS INTEGER) age FROM jobs WHERE status='pending'"
            ).fetchone()
            return None if row is None or row["age"] is None else int(row["age"])
