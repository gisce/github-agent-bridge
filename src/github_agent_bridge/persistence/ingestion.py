from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass

from ..actors import TriggerActor
from ..models import GitHubContext, Job, Notification, utc_now
from .acknowledgements import AcknowledgementRepository
from .commit_statuses import CommitStatusRepository
from .database import Database, TransactionMode
from .jobs import JobRepository, job_from_row
from .runtime import RuntimeRepository

COALESCE_STATUSES = ("pending", "waiting_approval")
CommitStatusFeedback = Callable[[int, str, int], tuple[str, str] | None]


def semantic_event_identity(
    action: str, ctx: GitHubContext, trigger_actor: str | None
) -> tuple[object, ...]:
    """Return the stable GitHub target identity used across notification variants."""
    target_id = (
        ctx.comment_id
        or ctx.review_comment_id
        or ctx.review_id
        or ctx.commit_comment_id
        or ctx.workflow_run_id
        or ctx.commit_sha
    )
    return (
        ctx.work_key,
        action,
        ctx.target_kind,
        target_id,
        (trigger_actor or "").lower(),
    )


@dataclass(frozen=True)
class IngestionRequest:
    notification: Notification
    context: GitHubContext
    source: str
    source_key: str
    event_key: str
    payload_hash: str
    status: str
    action: str
    decision: str
    work_intent: str
    metadata: dict[str, object]
    trigger_actor: TriggerActor | None = None
    commit_status_feedback: CommitStatusFeedback | None = None


@dataclass(frozen=True)
class IngestionResult:
    job: Job | None
    state: str
    capture_feedback: bool = False


class IngestionRepository:
    """Persist receipts, canonical events, deduplication and coalescing."""

    def __init__(
        self,
        database: Database,
        jobs: JobRepository,
        runtime: RuntimeRepository,
        acknowledgements: AcknowledgementRepository,
        commit_statuses: CommitStatusRepository,
    ) -> None:
        self.database = database
        self.jobs = jobs
        self.runtime = runtime
        self.acknowledgements = acknowledgements
        self.commit_statuses = commit_statuses

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        n = request.notification
        ctx = request.context
        actor = request.trigger_actor
        now = utc_now()
        try:
            with self.database.transaction(TransactionMode.IMMEDIATE) as con:
                try:
                    cursor = con.execute(
                        "INSERT INTO ingest_receipts(source,source_key,payload_hash,event_key,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                        (
                            request.source,
                            request.source_key,
                            request.payload_hash,
                            request.event_key,
                            "received",
                            now,
                            now,
                        ),
                    )
                    receipt_id = int(cursor.lastrowid)
                except sqlite3.IntegrityError:
                    receipt = con.execute(
                        "SELECT job_id FROM ingest_receipts WHERE source=? AND source_key=?",
                        (request.source, request.source_key),
                    ).fetchone()
                    job = (
                        job_from_row(
                            con.execute(
                                "SELECT * FROM jobs WHERE id=?",
                                (int(receipt["job_id"]),),
                            ).fetchone()
                        )
                        if receipt and receipt["job_id"]
                        else None
                    )
                    return IngestionResult(job, "duplicate")

                event = con.execute(
                    "SELECT job_id FROM github_events WHERE event_key=?",
                    (request.event_key,),
                ).fetchone()
                if event is not None:
                    con.execute(
                        "UPDATE ingest_receipts SET status='duplicate',job_id=?,updated_at=? WHERE id=?",
                        (event["job_id"], now, receipt_id),
                    )
                    job = self._job_by_id(con, event["job_id"])
                    if job is not None:
                        self._add_commit_status(con, request, job, now)
                    return IngestionResult(job, "duplicate")

                con.execute(
                    "INSERT INTO github_events(event_key,first_source,context_json,created_at,updated_at) VALUES(?,?,?,?,?)",
                    (
                        request.event_key,
                        request.source,
                        ctx.to_json(),
                        now,
                        now,
                    ),
                )
                existing = con.execute(
                    f"SELECT * FROM jobs WHERE work_key=? AND status IN ({','.join('?' for _ in COALESCE_STATUSES)}) ORDER BY id LIMIT 1",
                    (ctx.work_key, *COALESCE_STATUSES),
                ).fetchone()
                if existing is None and request.decision == "auto_trusted":
                    running_rows = con.execute(
                        "SELECT * FROM jobs WHERE work_key=? AND status='running' ORDER BY id",
                        (ctx.work_key,),
                    ).fetchall()
                    event_identity = semantic_event_identity(
                        request.action, ctx, actor.login if actor else None
                    )
                    existing = next(
                        (
                            row
                            for row in running_rows
                            if semantic_event_identity(
                                row["action"],
                                GitHubContext.from_json(row["context_json"]),
                                row["trigger_actor"],
                            )
                            == event_identity
                        ),
                        None,
                    )
                if existing and request.decision == "auto_trusted":
                    con.execute(
                        "INSERT OR IGNORE INTO coalesced_notifications(job_id,uid,message_id,subject,trigger_actor,trigger_actor_avatar_url,context_json,created_at) VALUES(?,?,?,?,?,?,?,?)",
                        (
                            existing["id"],
                            n.uid,
                            n.message_id,
                            n.subject,
                            actor.login if actor else None,
                            actor.avatar_url if actor else None,
                            ctx.to_json(),
                            now,
                        ),
                    )
                    if existing["status"] == "running":
                        con.execute(
                            "UPDATE jobs SET coalesced_count=coalesced_count+1, uid=?, updated_at=? WHERE id=?",
                            (n.uid, now, existing["id"]),
                        )
                    else:
                        con.execute(
                            "UPDATE jobs SET coalesced_count=coalesced_count+1, uid=?, message_id=message_id, subject=?, context_json=?, updated_at=? WHERE id=?",
                            (n.uid, n.subject, ctx.to_json(), now, existing["id"]),
                        )
                    self.runtime.record_worklog(
                        con,
                        existing["id"],
                        ctx.work_key,
                        "coalesced",
                        "Notification coalesced into active job",
                        n.message_id,
                    )
                    self.acknowledgements.add_pending(
                        con, int(existing["id"]), ctx, now
                    )
                    self._add_commit_status(
                        con, request, job_from_row(existing), now
                    )
                    con.execute(
                        "UPDATE github_events SET job_id=?,updated_at=? WHERE event_key=?",
                        (existing["id"], now, request.event_key),
                    )
                    con.execute(
                        "UPDATE ingest_receipts SET status='accepted',job_id=?,updated_at=? WHERE id=?",
                        (existing["id"], now, receipt_id),
                    )
                    return IngestionResult(
                        job_from_row(existing),
                        "coalesced",
                        capture_feedback=existing["message_id"] != n.message_id,
                    )

                cursor = con.execute(
                    "INSERT INTO jobs(work_key,repo,thread,status,action,decision,work_intent,subject,message_id,uid,trigger_actor,trigger_actor_avatar_url,context_json,metadata_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        ctx.work_key,
                        ctx.repo,
                        ctx.issue_number,
                        request.status,
                        request.action,
                        request.decision,
                        request.work_intent,
                        n.subject,
                        n.message_id,
                        n.uid,
                        actor.login if actor else None,
                        actor.avatar_url if actor else None,
                        ctx.to_json(),
                        json.dumps(request.metadata),
                        now,
                        now,
                    ),
                )
                job_id = int(cursor.lastrowid)
                con.execute(
                    "UPDATE github_events SET job_id=?,updated_at=? WHERE event_key=?",
                    (job_id, now, request.event_key),
                )
                con.execute(
                    "UPDATE ingest_receipts SET status='accepted',job_id=?,updated_at=? WHERE id=?",
                    (job_id, now, receipt_id),
                )
                self.runtime.record_worklog(
                    con,
                    job_id,
                    ctx.work_key,
                    "queued" if request.status == "pending" else request.status,
                    f"decision={request.decision} action={request.action}",
                    n.message_id,
                )
                if request.status == "pending":
                    self.acknowledgements.add_pending(con, job_id, ctx, now)
                job = self._job_by_id(con, job_id)
                self._add_commit_status(con, request, job, now)
                return IngestionResult(job, "enqueued", capture_feedback=True)
        except sqlite3.IntegrityError:
            return IngestionResult(
                self.jobs.find_by_message_id(n.message_id), "duplicate"
            )

    def quarantine(
        self,
        notification: Notification,
        *,
        reason: str,
        error: str,
        metadata: dict[str, object] | None = None,
        body_excerpt_chars: int = 2000,
    ) -> int:
        now = utc_now()
        try:
            with self.database.transaction(TransactionMode.IMMEDIATE) as con:
                cursor = con.execute(
                    """INSERT INTO quarantined_notifications(
                        uid,message_id,subject,from_addr,reason,error,body_excerpt,metadata_json,created_at
                    ) VALUES(?,?,?,?,?,?,?,?,?)""",
                    (
                        notification.uid,
                        notification.message_id,
                        notification.subject,
                        notification.from_addr,
                        reason,
                        error[:1000],
                        notification.body[:body_excerpt_chars],
                        json.dumps(
                            metadata or {}, ensure_ascii=False, sort_keys=True
                        ),
                        now,
                    ),
                )
                quarantine_id = int(cursor.lastrowid)
                self.runtime.record_worklog(
                    con,
                    None,
                    None,
                    "quarantined",
                    f"GitHub notification quarantined: {reason}",
                    notification.message_id or error[:500],
                )
                return quarantine_id
        except sqlite3.IntegrityError:
            if notification.message_id:
                with self.database.read_only() as con:
                    row = con.execute(
                        "SELECT id FROM quarantined_notifications WHERE message_id=?",
                        (notification.message_id,),
                    ).fetchone()
                if row:
                    return int(row["id"])
            raise

    @staticmethod
    def _job_by_id(con: sqlite3.Connection, job_id: object) -> Job | None:
        if not job_id:
            return None
        return job_from_row(
            con.execute("SELECT * FROM jobs WHERE id=?", (int(job_id),)).fetchone()
        )

    def _add_commit_status(
        self,
        con: sqlite3.Connection,
        request: IngestionRequest,
        job: Job | None,
        now: str,
    ) -> None:
        if (
            job is None
            or request.source != "webhook"
            or request.commit_status_feedback is None
        ):
            return
        row = con.execute(
            "SELECT decision FROM jobs WHERE id=?", (job.id,)
        ).fetchone()
        if row is None or row["decision"] != "auto_trusted":
            return
        desired = request.commit_status_feedback(job.id, job.status, job.attempts)
        if desired is not None:
            self.commit_statuses.add_for_job(
                con, job.id, request.context, desired[0], desired[1], now
            )
