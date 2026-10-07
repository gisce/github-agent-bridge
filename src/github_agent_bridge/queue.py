from __future__ import annotations

import hashlib
import json
import sqlite3
from importlib import resources
from pathlib import Path

from .models import GitHubContext, Job, Notification, utc_now
from .parser import classify_github_action, classify_work_intent, extract_github_context
from .persistence import (
    AcknowledgementRepository,
    ClosingConnection,
    CommitStatusClaim,
    CommitStatusRepository,
    Database,
    StateRepository,
)
from .policy import Policy
from .session_correlation import session_id_for_job, session_id_for_job_attempt
from . import feedback
from .actors import trigger_actor_details_for_enqueue, trigger_actor_details_from_notification
from .intent_classifier import ParserResult, classify_notification_with_llm, should_classify_with_llm
from .sql.migrations import apply_migrations

SCHEMA_PACKAGE = "github_agent_bridge.sql"


def load_schema() -> str:
    """Read the packaged SQLite schema resource."""
    return resources.files(SCHEMA_PACKAGE).joinpath("schema.sql").read_text(encoding="utf-8")


SCHEMA = load_schema()
ACTIVE_STATUSES = ("pending", "running", "waiting_approval")
COALESCE_STATUSES = ("pending", "waiting_approval")


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
    return (ctx.work_key, action, ctx.target_kind, target_id, (trigger_actor or "").lower())


def canonical_event_key(
    action: str,
    ctx: GitHubContext,
    source: str,
    source_key: str,
) -> str:
    """Identify a GitHub event across transports when immutable IDs prove identity.

    A source-specific fallback is deliberately used when an email does not expose
    enough immutable GitHub data. False negatives are safer than merging distinct
    user actions.
    """
    repo = (ctx.repo or "").lower()
    identities = (
        ("issue_comment", ctx.comment_id),
        ("pull_request_review_comment", ctx.review_comment_id),
        ("pull_request_review", ctx.review_id),
        ("commit_comment", ctx.commit_comment_id),
    )
    for event_type, target_id in identities:
        if repo and target_id:
            return f"{event_type}:created:{repo}:{target_id}"
    if repo and ctx.workflow_run_id:
        return f"workflow_run:{action}:{repo}:{ctx.workflow_run_id}"
    return f"{source}:{source_key}"


class JobQueue:
    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser()
        self.database = Database(self.path)
        self.init()
        self.acknowledgements = AcknowledgementRepository(self.database)
        self.commit_statuses = CommitStatusRepository(self.database)
        self.state = StateRepository(self.database)

    def connect(self) -> sqlite3.Connection:
        initialize = not self.path.exists()
        con = self.database.read_write()
        try:
            if initialize:
                self._initialize_database(con)
        except Exception:
            con.close()
            raise
        return con

    def init(self) -> None:
        with self.connect() as con:
            self._initialize_database(con)

    def _initialize_database(self, con: sqlite3.Connection) -> None:
        history_exists = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
        ).fetchone()
        baseline_applied = bool(
            history_exists
            and con.execute(
                "SELECT 1 FROM schema_migrations WHERE version=1"
            ).fetchone()
        )
        if baseline_applied:
            # Validate/apply immutable steps before the rolling schema snapshot can
            # touch a database created by this or a newer package version.
            apply_migrations(con)
            con.executescript(SCHEMA)
        else:
            # Legacy or incomplete histories need the snapshot to create missing
            # tables before the baseline migration can perform its backfills.
            con.executescript(SCHEMA)
            apply_migrations(con)

    def enqueue(self, n: Notification, policy: Policy) -> tuple[Job | None, str]:
        """Backward-compatible email enqueue entrypoint."""
        return self.ingest(n, policy, source="email", source_key=n.message_id)

    def ingest(
        self,
        n: Notification,
        policy: Policy,
        *,
        source: str = "email",
        source_key: str | None = None,
    ) -> tuple[Job | None, str]:
        source_key = source_key or n.message_id
        ctx = extract_github_context(n.body)
        action = classify_github_action(
            n.subject,
            n.body,
            policy.bot_logins,
            message_id=n.message_id,
        )
        intent = classify_work_intent(n.subject, n.body, policy.bot_logins)
        metadata: dict[str, object] = {"received_at": n.received_at}
        parser_result = ParserResult(action, intent)
        if should_classify_with_llm(n, ctx, parser_result, policy):
            metadata["intent_classifier"] = {
                "parser": {"action": action, "work_intent": intent},
                "enabled": True,
            }
            try:
                llm_result = classify_notification_with_llm(
                    n,
                    ctx,
                    parser_result,
                    policy.intent_classifier,
                    policy=policy,
                    agent=policy.route_for(ctx.repo).agent,
                    prompt_template=(
                        feedback.load_prompt_override(path)
                        if (path := policy.prompt_overrides.rule_path("intent_classifier"))
                        else None
                    ),
                )
                classifier_metadata = llm_result.to_metadata()
                metadata["intent_classifier"] = {
                    **metadata["intent_classifier"],
                    "llm": classifier_metadata,
                }
                if llm_result.applied:
                    action = llm_result.action
                    intent = llm_result.work_intent
            except Exception as exc:
                metadata["intent_classifier"] = {
                    **metadata["intent_classifier"],
                    "error": str(exc)[:500],
                }
        if action == "submit_review":
            intent = "review_only"
            metadata["intent_guardrail"] = "submit_review_read_only"
        decision = policy.decision(n, ctx, action)
        status = {"auto": "done", "ask": "waiting_approval", "deny": "denied"}.get(decision, "pending")
        now = utc_now()
        trigger_actor = (
            trigger_actor_details_from_notification(n)
            if source == "webhook"
            else trigger_actor_details_for_enqueue(n, ctx)
        )
        event_key = canonical_event_key(action, ctx, source, source_key)
        payload_hash = hashlib.sha256(n.body.encode("utf-8")).hexdigest()
        if trigger_actor and trigger_actor.user_id:
            metadata["trigger_actor_id"] = trigger_actor.user_id
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            try:
                try:
                    con.execute(
                        "INSERT INTO ingest_receipts(source,source_key,payload_hash,event_key,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                        (source, source_key, payload_hash, event_key, "received", now, now),
                    )
                    receipt_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
                except sqlite3.IntegrityError:
                    receipt = con.execute(
                        "SELECT job_id FROM ingest_receipts WHERE source=? AND source_key=?",
                        (source, source_key),
                    ).fetchone()
                    con.commit()
                    job = self.get(int(receipt["job_id"])) if receipt and receipt["job_id"] else None
                    return job, "duplicate"
                event = con.execute(
                    "SELECT job_id FROM github_events WHERE event_key=?",
                    (event_key,),
                ).fetchone()
                if event is not None:
                    con.execute(
                        "UPDATE ingest_receipts SET status='duplicate',job_id=?,updated_at=? WHERE id=?",
                        (event["job_id"], now, receipt_id),
                    )
                    if source == "webhook" and event["job_id"]:
                        duplicate_job = con.execute(
                            "SELECT status,decision,attempts FROM jobs WHERE id=?",
                            (event["job_id"],),
                        ).fetchone()
                        if duplicate_job is not None:
                            self._add_initial_commit_status(
                                con,
                                int(event["job_id"]),
                                ctx,
                                str(duplicate_job["status"]),
                                str(duplicate_job["decision"]),
                                int(duplicate_job["attempts"]),
                                now,
                            )
                    con.commit()
                    job = self.get(int(event["job_id"])) if event["job_id"] else None
                    return job, "duplicate"
                con.execute(
                    "INSERT INTO github_events(event_key,first_source,context_json,created_at,updated_at) VALUES(?,?,?,?,?)",
                    (event_key, source, ctx.to_json(), now, now),
                )
                existing = con.execute(
                    f"SELECT * FROM jobs WHERE work_key=? AND status IN ({','.join('?' for _ in COALESCE_STATUSES)}) ORDER BY id LIMIT 1",
                    (ctx.work_key, *COALESCE_STATUSES),
                ).fetchone()
                if existing is None and decision == "auto_trusted":
                    running_rows = con.execute(
                        "SELECT * FROM jobs WHERE work_key=? AND status='running' ORDER BY id",
                        (ctx.work_key,),
                    ).fetchall()
                    event_identity = semantic_event_identity(
                        action, ctx, trigger_actor.login if trigger_actor else None
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
                if existing and decision == "auto_trusted":
                    con.execute(
                        "INSERT OR IGNORE INTO coalesced_notifications(job_id,uid,message_id,subject,trigger_actor,trigger_actor_avatar_url,context_json,created_at) VALUES(?,?,?,?,?,?,?,?)",
                        (existing["id"], n.uid, n.message_id, n.subject, trigger_actor.login if trigger_actor else None, trigger_actor.avatar_url if trigger_actor else None, ctx.to_json(), now),
                    )
                    if existing["status"] == "running":
                        con.execute(
                            "UPDATE jobs SET coalesced_count=coalesced_count+1, uid=?, updated_at=? WHERE id=?",
                            (n.uid, now, existing["id"]),
                        )
                    else:
                        con.execute("UPDATE jobs SET coalesced_count=coalesced_count+1, uid=?, message_id=message_id, subject=?, context_json=?, updated_at=? WHERE id=?", (n.uid, n.subject, ctx.to_json(), now, existing["id"]))
                    self._log(con, existing["id"], ctx.work_key, "coalesced", "Notification coalesced into active job", n.message_id)
                    self.acknowledgements.add_pending(con, int(existing["id"]), ctx, now)
                    if source == "webhook":
                        self._add_initial_commit_status(
                            con,
                            int(existing["id"]),
                            ctx,
                            str(existing["status"]),
                            str(existing["decision"]),
                            int(existing["attempts"]),
                            now,
                        )
                    con.execute(
                        "UPDATE github_events SET job_id=?,updated_at=? WHERE event_key=?",
                        (existing["id"], now, event_key),
                    )
                    con.execute(
                        "UPDATE ingest_receipts SET status='accepted',job_id=?,updated_at=? WHERE id=?",
                        (existing["id"], now, receipt_id),
                    )
                    con.commit()
                    if policy.feedback_learning.enabled and existing["message_id"] != n.message_id:
                        feedback.capture_feedback(
                            self.path,
                            n,
                            ctx,
                            action,
                            decision,
                            intent,
                            trigger_actor=trigger_actor.login if trigger_actor else None,
                            trigger_actor_avatar_url=trigger_actor.avatar_url if trigger_actor else None,
                        )
                    return self._row_to_job(existing), "coalesced"
                con.execute(
                    "INSERT INTO jobs(work_key,repo,thread,status,action,decision,work_intent,subject,message_id,uid,trigger_actor,trigger_actor_avatar_url,context_json,metadata_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (ctx.work_key, ctx.repo, ctx.issue_number, status, action, decision, intent, n.subject, n.message_id, n.uid, trigger_actor.login if trigger_actor else None, trigger_actor.avatar_url if trigger_actor else None, ctx.to_json(), json.dumps(metadata), now, now),
                )
                job_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
                con.execute(
                    "UPDATE github_events SET job_id=?,updated_at=? WHERE event_key=?",
                    (job_id, now, event_key),
                )
                con.execute(
                    "UPDATE ingest_receipts SET status='accepted',job_id=?,updated_at=? WHERE id=?",
                    (job_id, now, receipt_id),
                )
                self._log(con, job_id, ctx.work_key, "queued" if status == "pending" else status, f"decision={decision} action={action}", n.message_id)
                if status == "pending":
                    self.acknowledgements.add_pending(con, job_id, ctx, now)
                if source == "webhook":
                    self._add_initial_commit_status(
                        con, job_id, ctx, status, decision, 0, now
                    )
                con.commit()
                if policy.feedback_learning.enabled:
                    feedback.capture_feedback(
                        self.path,
                        n,
                        ctx,
                        action,
                        decision,
                        intent,
                        trigger_actor=trigger_actor.login if trigger_actor else None,
                        trigger_actor_avatar_url=trigger_actor.avatar_url if trigger_actor else None,
                    )
                return self.get(job_id), "enqueued"
            except sqlite3.IntegrityError:
                con.rollback()
                row = con.execute("SELECT * FROM jobs WHERE message_id=?", (n.message_id,)).fetchone()
                return self._row_to_job(row) if row else None, "duplicate"

    def claim_acknowledgement(self, job_id: int | None = None) -> tuple[int, int, GitHubContext] | None:
        """Reserve one durable GitHub acknowledgement without claiming its job."""
        claim = self.acknowledgements.claim(job_id)
        if claim is None:
            return None
        return claim.id, claim.job_id, claim.context

    def recover_acknowledgements(self) -> int:
        """Release acknowledgements interrupted by an executor restart."""
        return self.acknowledgements.recover_interrupted()

    def finish_acknowledgement(self, acknowledgement_id: int, ok: bool, error: str | None = None) -> None:
        self.acknowledgements.finish(acknowledgement_id, ok, error)

    def acknowledgement_ok(self, job_id: int) -> bool:
        return self.acknowledgements.all_succeeded(job_id)

    def claim_commit_status(self, job_id: int | None = None) -> CommitStatusClaim | None:
        return self.commit_statuses.claim(job_id)

    def pin_commit_status_sha(self, status_id: int, sha: str) -> str:
        return self.commit_statuses.pin_sha(status_id, sha)

    def finish_commit_status(
        self,
        status_id: int,
        revision: int,
        ok: bool,
        error: str | None = None,
    ) -> None:
        self.commit_statuses.finish(status_id, revision, ok, error)

    def recover_commit_statuses(self) -> int:
        return self.commit_statuses.recover_interrupted()

    def quarantine_notification(
        self,
        n: Notification,
        *,
        reason: str,
        error: str,
        metadata: dict[str, object] | None = None,
        body_excerpt_chars: int = 2000,
    ) -> int:
        body_excerpt = n.body[:body_excerpt_chars]
        now = utc_now()
        metadata_json = json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True)
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            try:
                con.execute(
                    """
                    INSERT INTO quarantined_notifications(
                        uid,message_id,subject,from_addr,reason,error,body_excerpt,metadata_json,created_at
                    ) VALUES(?,?,?,?,?,?,?,?,?)
                    """,
                    (n.uid, n.message_id, n.subject, n.from_addr, reason, error[:1000], body_excerpt, metadata_json, now),
                )
                quarantine_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
                self._log(
                    con,
                    None,
                    None,
                    "quarantined",
                    f"GitHub notification quarantined: {reason}",
                    n.message_id or error[:500],
                )
                con.commit()
                return quarantine_id
            except sqlite3.IntegrityError:
                con.rollback()
                if n.message_id:
                    row = con.execute(
                        "SELECT id FROM quarantined_notifications WHERE message_id=?",
                        (n.message_id,),
                    ).fetchone()
                    if row:
                        return int(row["id"])
                raise

    def claim_next(self, worker_id: str, work_intents: frozenset[str] | set[str] | None = None) -> Job | None:
        now = utc_now()
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            # Serialize the pause check with claims so a completed pause blocks later claims.
            if self.state.executor_paused(connection=con):
                con.commit()
                return None
            intent_filter = ""
            args: list[object] = []
            if work_intents is not None:
                if not work_intents:
                    con.commit()
                    return None
                intent_filter = f"AND j.work_intent IN ({','.join('?' for _ in work_intents)})"
                args.extend(sorted(work_intents))
            row = con.execute(
                f"""SELECT * FROM jobs j WHERE j.status='pending'
                {intent_filter}
                AND NOT EXISTS (SELECT 1 FROM jobs r WHERE r.work_key=j.work_key AND r.status='running')
                ORDER BY j.created_at LIMIT 1""",
                args,
            ).fetchone()
            if not row:
                con.commit(); return None
            metadata = json.loads(row["metadata_json"] or "{}")
            metadata.pop("runtime_process", None)
            fresh_session = bool(metadata.get("fresh_session_on_retry")) and int(row["attempts"]) > 0
            if fresh_session or row["work_intent"] == "work_allowed":
                metadata["openclaw_session_id"] = session_id_for_job_attempt(int(row["id"]), int(row["attempts"]) + 1)
            else:
                metadata.setdefault("openclaw_session_id", session_id_for_job(int(row["id"])))
            attempt = int(row["attempts"]) + 1
            con.execute(
                "UPDATE jobs SET status='running', locked_by=?, attempts=attempts+1, started_at=?, finished_at=NULL, updated_at=?, metadata_json=? WHERE id=?",
                (worker_id, now, now, json.dumps(metadata, sort_keys=True), row["id"]),
            )
            con.execute(
                """INSERT INTO job_runs(job_id,attempt,started_at,worker_id,session_id)
                VALUES(?,?,?,?,?)""",
                (row["id"], attempt, now, worker_id, metadata["openclaw_session_id"]),
            )
            self._log(con, row["id"], row["work_key"], "running", f"claimed by {worker_id}", None)
            self._session_event(con, row["id"], row["work_key"], metadata["openclaw_session_id"], "claimed", f"claimed by {worker_id}", None)
            self._progress(con, row["id"], row["work_key"], "semantic", "claimed", f"claimed by {worker_id}", None)
            self._set_commit_status_desired(
                con,
                int(row["id"]),
                "pending",
                f"Agent working (attempt {attempt})",
                now,
            )
            con.commit()
            return self.get(int(row["id"]))

    def record_worker_heartbeat(
        self,
        worker_id: str,
        executor_id: str,
        pid: int,
        loop_state: str,
        active_job_id: int | None = None,
        recent_error_count: int = 0,
    ) -> None:
        now = utc_now()
        with self.connect() as con:
            con.execute(
                """INSERT INTO worker_heartbeats(
                       worker_id, executor_id, pid, last_seen, active_job_id, loop_state, recent_error_count
                   ) VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(worker_id) DO UPDATE SET
                       executor_id=excluded.executor_id,
                       pid=excluded.pid,
                       last_seen=excluded.last_seen,
                       active_job_id=excluded.active_job_id,
                       loop_state=excluded.loop_state,
                       recent_error_count=excluded.recent_error_count""",
                (worker_id, executor_id, pid, now, active_job_id, loop_state, recent_error_count),
            )

    def delete_worker_heartbeats_except(self, executor_id: str) -> int:
        """Remove heartbeat rows left by previous executor processes."""
        with self.connect() as con:
            cur = con.execute(
                "DELETE FROM worker_heartbeats WHERE executor_id != ?",
                (executor_id,),
            )
            return cur.rowcount

    def register_runtime_process(
        self,
        job_id: int,
        worker_id: str,
        executor_id: str,
        identity: dict[str, int],
    ) -> bool:
        """Persist the exact process that owns a running job."""
        now = utc_now()
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT work_key, metadata_json FROM jobs WHERE id=? AND status='running' AND locked_by=?",
                (job_id, worker_id),
            ).fetchone()
            if row is None:
                con.commit()
                return False
            metadata = json.loads(row["metadata_json"] or "{}")
            runtime_process = {
                "state": "running",
                "executor_id": executor_id,
                "worker_id": worker_id,
                "pid": int(identity["pid"]),
                "ppid": int(identity["ppid"]),
                "pgid": int(identity["pgid"]),
                "sid": int(identity["sid"]),
                "start_time_ticks": int(identity["start_time_ticks"]),
                "registered_at": now,
            }
            metadata["runtime_process"] = runtime_process
            con.execute(
                "UPDATE jobs SET metadata_json=?, updated_at=? WHERE id=? AND status='running' AND locked_by=?",
                (json.dumps(metadata, sort_keys=True), now, job_id, worker_id),
            )
            self._session_event(
                con,
                job_id,
                row["work_key"],
                str(metadata.get("openclaw_session_id") or session_id_for_job(job_id)),
                "process_registered",
                f"runtime process {runtime_process['pid']} registered",
                json.dumps(runtime_process, sort_keys=True),
            )
            con.commit()
            return True

    def mark_runtime_process_exited(self, job_id: int, worker_id: str) -> bool:
        """Mark a registered process as exited while result handling finishes."""
        now = utc_now()
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT metadata_json FROM jobs WHERE id=? AND status='running' AND locked_by=?",
                (job_id, worker_id),
            ).fetchone()
            if row is None:
                con.commit()
                return False
            metadata = json.loads(row["metadata_json"] or "{}")
            runtime_process = metadata.get("runtime_process")
            if not isinstance(runtime_process, dict):
                con.commit()
                return False
            runtime_process["state"] = "exited"
            runtime_process["exited_at"] = now
            metadata["runtime_process"] = runtime_process
            cur = con.execute(
                "UPDATE jobs SET metadata_json=?, updated_at=? WHERE id=? AND status='running' AND locked_by=?",
                (json.dumps(metadata, sort_keys=True), now, job_id, worker_id),
            )
            con.commit()
            return bool(cur.rowcount)

    def finish(self, job_id: int, status: str, summary: str, detail: str | None = None) -> None:
        now = utc_now()
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute("SELECT work_key FROM jobs WHERE id=?", (job_id,)).fetchone()
            metadata = self._job_metadata(con, job_id)
            cancellation = metadata.get("cancellation")
            if isinstance(cancellation, dict) and cancellation.get("state") in {"requested", "cancelled"}:
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
            run_result = "cancelled" if isinstance(cancellation, dict) and cancellation.get("state") in {"requested", "cancelled"} else status
            self._finish_run(con, job_id, run_result, now)
            self._log(con, job_id, row["work_key"] if row else None, status, summary, detail)
            session_id = metadata.get("openclaw_session_id") or session_id_for_job(job_id)
            self._session_event(con, job_id, row["work_key"] if row else None, str(session_id), status, summary, detail)
            self._progress(con, job_id, row["work_key"] if row else None, "semantic", status, summary, detail)
            if isinstance(cancellation, dict) and cancellation.get("state") in {"requested", "cancelled"}:
                self._set_commit_status_desired(
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
                self._set_commit_status_desired(
                    con, job_id, "success", description, now
                )
            elif status == "blocked":
                self._set_commit_status_desired(
                    con,
                    job_id,
                    "error",
                    "Agent blocked; attention required",
                    now,
                )
            con.commit()

    def request_cancel_running(
        self,
        job_id: int,
        *,
        actor: str,
        reason: str | None = None,
    ) -> Job | None:
        """Record a manual cancellation request for a running job."""
        now = utc_now()
        clean_actor = actor.strip().lstrip("@") or "unknown"
        clean_reason = (reason or "").strip()
        summary = f"job cancellation requested by @{clean_actor}"
        detail = f"reason={clean_reason}" if clean_reason else "reason not provided"
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT work_key, metadata_json FROM jobs WHERE id=? AND status='running'",
                (job_id,),
            ).fetchone()
            if row is None:
                con.commit()
                return None
            metadata = json.loads(row["metadata_json"] or "{}")
            existing_cancellation = metadata.get("cancellation")
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
            self._log(con, job_id, row["work_key"], "cancel_requested", summary, detail)
            session_id = str(metadata.get("openclaw_session_id") or session_id_for_job(job_id))
            self._session_event(con, job_id, row["work_key"], session_id, "cancel_requested", summary, detail)
            self._progress(con, job_id, row["work_key"], "semantic", "cancel_requested", summary, detail)
            con.commit()
        return self.get(job_id)

    def mark_cancelled(
        self,
        job_id: int,
        *,
        actor: str,
        reason: str | None = None,
        signal_detail: str | None = None,
        followup_url: str | None = None,
    ) -> Job | None:
        """Mark a running job as manually cancelled."""
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
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT status, work_key, metadata_json FROM jobs WHERE id=?",
                (job_id,),
            ).fetchone()
            if row is None:
                con.commit()
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
            if row["status"] != "running":
                if not (isinstance(existing_cancellation, dict) and existing_cancellation.get("state") == "requested"):
                    con.commit()
                    return None
            cur = con.execute(
                """UPDATE jobs
                SET status='done', locked_by=NULL, last_error=NULL, finished_at=COALESCE(finished_at, ?), updated_at=?, metadata_json=?
                WHERE id=?""",
                (now, now, json.dumps(metadata, sort_keys=True), job_id),
            )
            if not cur.rowcount:
                con.commit()
                return None
            self._finish_run(con, job_id, "cancelled", now)
            self._log(con, job_id, row["work_key"], "cancelled", summary, detail)
            session_id = str(metadata.get("openclaw_session_id") or session_id_for_job(job_id))
            self._session_event(con, job_id, row["work_key"], session_id, "cancelled", summary, detail)
            self._progress(con, job_id, row["work_key"], "semantic", "cancelled", summary, detail)
            self._set_commit_status_desired(
                con,
                job_id,
                "error",
                "Agent cancelled; attention required",
                now,
            )
            con.commit()
        return self.get(job_id)

    def requeue_running(
        self,
        job_id: int,
        summary: str,
        detail: str | None = None,
        *,
        fresh_session: bool = False,
    ) -> bool:
        now = utc_now()
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT work_key, metadata_json FROM jobs WHERE id=? AND status='running'",
                (job_id,),
            ).fetchone()
            if row is None:
                con.commit()
                return False
            metadata = json.loads(row["metadata_json"] or "{}")
            if fresh_session:
                metadata["fresh_session_on_retry"] = True
            cur = con.execute(
                "UPDATE jobs SET status='pending', locked_by=NULL, last_error=NULL, finished_at=?, updated_at=?, metadata_json=? WHERE id=? AND status='running'",
                (now, now, json.dumps(metadata, sort_keys=True), job_id),
            )
            if cur.rowcount:
                self._finish_run(con, job_id, "requeued", now)
                self._log(con, job_id, row["work_key"], "retry", summary, detail)
                attempts = con.execute(
                    "SELECT attempts FROM jobs WHERE id=?", (job_id,)
                ).fetchone()["attempts"]
                self._set_commit_status_desired(
                    con,
                    job_id,
                    "pending",
                    f"Agent retry scheduled (attempt {int(attempts) + 1})",
                    now,
                )
            con.commit()
            return bool(cur.rowcount)

    def block_running(
        self,
        summary: str,
        detail: str,
        *,
        job_ids: list[int] | None = None,
        locked_by: set[str] | None = None,
        older_than_seconds: int | None = None,
    ) -> list[int]:
        """Mark selected running jobs as blocked without requeuing them."""
        now = utc_now()
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            clauses = ["status='running'"]
            args: list[object] = []
            if job_ids is not None:
                if not job_ids:
                    con.commit()
                    return []
                clauses.append(f"id IN ({','.join('?' for _ in job_ids)})")
                args.extend(job_ids)
            if locked_by is not None:
                if not locked_by:
                    con.commit()
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
                cur = con.execute(
                    """UPDATE jobs
                    SET status='blocked', locked_by=NULL, last_error=?,
                        finished_at=?, updated_at=?
                    WHERE id=? AND status='running'""",
                    (detail, now, now, row["id"]),
                )
                if not cur.rowcount:
                    continue
                job_id = int(row["id"])
                blocked_ids.append(job_id)
                self._finish_run(con, job_id, "blocked", now)
                self._log(con, job_id, row["work_key"], "blocked", summary, detail)
                metadata = json.loads(row["metadata_json"] or "{}")
                session_id = str(metadata.get("openclaw_session_id") or session_id_for_job(job_id))
                self._session_event(con, job_id, row["work_key"], session_id, "blocked", summary, detail)
                self._progress(con, job_id, row["work_key"], "semantic", "blocked", summary, detail)
                self._set_commit_status_desired(
                    con,
                    job_id,
                    "error",
                    "Agent blocked; attention required",
                    now,
                )
            con.commit()
            return blocked_ids

    def update_work_intent(self, job_id: int, work_intent: str, summary: str) -> Job | None:
        now = utc_now()
        with self.connect() as con:
            row = con.execute("SELECT work_key, action FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return None
            if row["action"] == "submit_review":
                work_intent = "review_only"
            con.execute("UPDATE jobs SET work_intent=?, updated_at=? WHERE id=?", (work_intent, now, job_id))
            self._log(con, job_id, row["work_key"], "intent_update", summary, None)
        return self.get(job_id)

    def add_session_event(self, job_id: int, event_type: str, summary: str, detail: str | None = None) -> None:
        now = utc_now()
        with self.connect() as con:
            row = con.execute("SELECT work_key, metadata_json FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return
            metadata = json.loads(row["metadata_json"] or "{}")
            session_id = str(metadata.get("openclaw_session_id") or session_id_for_job(job_id))
            con.execute("UPDATE jobs SET updated_at=? WHERE id=?", (now, job_id))
            self._session_event(con, job_id, row["work_key"], session_id, event_type, summary, detail)
            kind = "visible" if event_type.startswith("openclaw_") else "semantic"
            self._progress(con, job_id, row["work_key"], kind, event_type[:80], summary, detail)

    def add_worklog(self, job_id: int, phase: str, summary: str, detail: str | None = None) -> None:
        with self.connect() as con:
            row = con.execute("SELECT work_key FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is not None:
                self._log(con, job_id, row["work_key"], phase, summary, detail)

    def list_jobs(self, status: str | None = None, limit: int = 20) -> list[Job]:
        sql = "SELECT * FROM jobs"
        args: tuple[object, ...] = ()
        if status:
            sql += " WHERE status=?"
            args = (status,)
        sql += " ORDER BY id DESC LIMIT ?"
        args = (*args, limit)
        with self.connect() as con:
            return [j for j in (self._row_to_job(r) for r in con.execute(sql, args)) if j]

    def retry(self, job_id: int, *, actor: str | None = None) -> bool:
        now = utc_now()
        summary = f"job requeued by @{actor}" if actor else "job requeued"
        with self.connect() as con:
            cur = con.execute("UPDATE jobs SET status='pending', locked_by=NULL, last_error=NULL, updated_at=? WHERE id=? AND status='blocked' AND decision='auto_trusted'", (now, job_id))
            if cur.rowcount:
                row = con.execute("SELECT work_key,attempts FROM jobs WHERE id=?", (job_id,)).fetchone()
                self._log(con, job_id, row["work_key"] if row else None, "retry", summary, None)
                self._set_commit_status_desired(
                    con,
                    job_id,
                    "pending",
                    f"Agent retry scheduled (attempt {int(row['attempts']) + 1})",
                    now,
                )
            return bool(cur.rowcount)

    def dismiss(self, job_id: int, reason: str) -> bool:
        now = utc_now()
        with self.connect() as con:
            cur = con.execute(
                "UPDATE jobs SET status='done', locked_by=NULL, last_error=NULL, finished_at=COALESCE(finished_at, ?), updated_at=? WHERE id=? AND status IN ('blocked','denied','waiting_approval')",
                (now, now, job_id),
            )
            if cur.rowcount:
                row = con.execute("SELECT work_key FROM jobs WHERE id=?", (job_id,)).fetchone()
                self._log(con, job_id, row["work_key"] if row else None, "dismissed", "job dismissed manually", reason)
            return bool(cur.rowcount)

    def unlock_stale(self, older_than_seconds: int, job_ids: list[int] | None = None) -> int:
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            args: list[object] = [older_than_seconds]
            sql = "SELECT id, work_key FROM jobs WHERE status='running' AND started_at IS NOT NULL AND (julianday('now') - julianday(started_at)) * 86400 > ?"
            if job_ids is not None:
                if not job_ids:
                    con.commit()
                    return 0
                sql += f" AND id IN ({','.join('?' for _ in job_ids)})"
                args.extend(job_ids)
            rows = con.execute(sql, args).fetchall()
            now = utc_now()
            for row in rows:
                con.execute("UPDATE jobs SET status='pending', locked_by=NULL, finished_at=?, updated_at=? WHERE id=?", (now, now, row["id"]))
                self._finish_run(con, int(row["id"]), "requeued", now)
                self._log(con, row["id"], row["work_key"], "unlock_stale", f"running job older than {older_than_seconds}s requeued", None)
                attempts = con.execute(
                    "SELECT attempts FROM jobs WHERE id=?", (row["id"],)
                ).fetchone()["attempts"]
                self._set_commit_status_desired(
                    con,
                    int(row["id"]),
                    "pending",
                    f"Agent retry scheduled (attempt {int(attempts) + 1})",
                    now,
                )
            con.commit()
            return len(rows)

    def get(self, job_id: int) -> Job | None:
        with self.connect() as con:
            return self._row_to_job(con.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())

    def coalesced_contexts(self, job_id: int) -> list[GitHubContext]:
        with self.connect() as con:
            rows = con.execute(
                "SELECT context_json FROM coalesced_notifications WHERE job_id=? ORDER BY id",
                (job_id,),
            ).fetchall()
        return [GitHubContext.from_json(row["context_json"]) for row in rows]

    def coalesced_trigger_actors(self, job_id: int) -> list[str]:
        with self.connect() as con:
            rows = con.execute(
                "SELECT trigger_actor FROM coalesced_notifications WHERE job_id=? ORDER BY id",
                (job_id,),
            ).fetchall()
        return [row["trigger_actor"] for row in rows if row["trigger_actor"]]

    def stats(self) -> dict[str, int]:
        with self.connect() as con:
            return {r["status"]: r["count"] for r in con.execute("SELECT status, count(*) count FROM jobs GROUP BY status")}

    def pending_age_seconds(self) -> int | None:
        with self.connect() as con:
            row = con.execute("SELECT CAST((julianday('now') - julianday(min(created_at))) * 86400 AS INTEGER) age FROM jobs WHERE status='pending'").fetchone()
            return None if row is None or row["age"] is None else int(row["age"])

    def set_state(self, key: str, value: str) -> None:
        self.state.set(key, value)

    def get_state(self, key: str, default: str = "") -> str:
        return self.state.get(key, default)

    def pause_executor(self, reason: str = "") -> None:
        self.state.pause_executor(reason)

    def resume_executor(self) -> None:
        self.state.resume_executor()

    def executor_pause_state(self) -> dict[str, object]:
        return self.state.executor_pause_state().to_dict()

    def executor_paused(self) -> bool:
        return self.state.executor_paused()

    def _log(self, con: sqlite3.Connection, job_id: int | None, work_key: str | None, phase: str, summary: str, detail: str | None) -> None:
        con.execute("INSERT INTO worklog(ts,job_id,work_key,phase,summary,detail) VALUES(?,?,?,?,?,?)", (utc_now(), job_id, work_key, phase, summary, detail))

    @staticmethod
    def _commit_status_feedback(
        job_id: int,
        status: str,
        attempts: int,
    ) -> tuple[str, str] | None:
        if status == "pending":
            if attempts:
                return "pending", f"Agent retry scheduled (attempt {attempts + 1})"
            return "pending", f"Agent queued (job #{job_id})"
        if status == "running":
            return "pending", f"Agent working (attempt {max(1, attempts)})"
        if status == "done":
            return "success", "Agent finished; follow-up available"
        if status == "blocked":
            return "error", "Agent blocked; attention required"
        return None

    def _add_initial_commit_status(
        self,
        con: sqlite3.Connection,
        job_id: int,
        ctx: GitHubContext,
        status: str,
        decision: str,
        attempts: int,
        now: str,
    ) -> None:
        if decision != "auto_trusted":
            return
        desired = self._commit_status_feedback(job_id, status, attempts)
        if desired is None:
            return
        self.commit_statuses.add_for_job(
            con, job_id, ctx, desired[0], desired[1], now
        )

    def _set_commit_status_desired(
        self,
        con: sqlite3.Connection,
        job_id: int,
        state: str,
        description: str,
        now: str,
    ) -> None:
        self.commit_statuses.set_desired(
            con, job_id, state, description, now
        )

    def _session_event(self, con: sqlite3.Connection, job_id: int, work_key: str | None, session_id: str, event_type: str, summary: str, detail: str | None) -> None:
        con.execute(
            "INSERT INTO job_session_events(ts,job_id,work_key,session_id,event_type,summary,detail) VALUES(?,?,?,?,?,?,?)",
            (utc_now(), job_id, work_key, session_id, event_type, summary, detail),
        )

    def _progress(self, con: sqlite3.Connection, job_id: int, work_key: str | None, kind: str, phase: str, summary: str, detail: str | None) -> None:
        con.execute(
            "INSERT INTO job_progress(ts,job_id,work_key,kind,phase,summary,detail) VALUES(?,?,?,?,?,?,?)",
            (utc_now(), job_id, work_key, kind, phase, summary, detail),
        )

    def _job_metadata(self, con: sqlite3.Connection, job_id: int) -> dict[str, object]:
        row = con.execute("SELECT metadata_json FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            return {}
        return json.loads(row["metadata_json"] or "{}")

    def _finish_run(self, con: sqlite3.Connection, job_id: int, result: str, finished_at: str) -> None:
        con.execute(
            """UPDATE job_runs
            SET finished_at=?, result=?
            WHERE id=(
                SELECT id FROM job_runs
                WHERE job_id=? AND finished_at IS NULL
                ORDER BY attempt DESC LIMIT 1
            ) AND finished_at IS NULL""",
            (finished_at, result, job_id),
        )

    def _row_to_job(self, row: sqlite3.Row | None) -> Job | None:
        if row is None:
            return None
        return Job(id=row["id"], work_key=row["work_key"], repo=row["repo"], thread=row["thread"], status=row["status"], action=row["action"], work_intent=row["work_intent"], subject=row["subject"], message_id=row["message_id"], uid=row["uid"], trigger_actor=row["trigger_actor"], trigger_actor_avatar_url=row["trigger_actor_avatar_url"], context=GitHubContext.from_json(row["context_json"]), attempts=row["attempts"], coalesced_count=row["coalesced_count"], last_error=row["last_error"], locked_by=row["locked_by"], created_at=row["created_at"], updated_at=row["updated_at"], metadata=json.loads(row["metadata_json"] or "{}"))
