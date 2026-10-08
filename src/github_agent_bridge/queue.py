from __future__ import annotations

import hashlib
import sqlite3
from importlib import resources
from pathlib import Path

from .models import GitHubContext, Job, Notification
from .parser import classify_github_action, classify_work_intent, extract_github_context
from .persistence import (
    AcknowledgementRepository,
    ClosingConnection,
    CommitStatusClaim,
    CommitStatusRepository,
    Database,
    IngestionRepository,
    IngestionRequest,
    JobRepository,
    RuntimeProcess,
    RuntimeRepository,
    StateRepository,
)
from .policy import Policy
from . import feedback
from .actors import trigger_actor_details_for_enqueue, trigger_actor_details_from_notification
from .intent_classifier import ParserResult, classify_notification_with_llm, should_classify_with_llm
from .sql.migrations import apply_migrations, validate_migrations

SCHEMA_PACKAGE = "github_agent_bridge.sql"


def load_schema() -> str:
    """Read the packaged SQLite schema resource."""
    return resources.files(SCHEMA_PACKAGE).joinpath("schema.sql").read_text(encoding="utf-8")


SCHEMA = load_schema()


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
    if action == "sync_after_merge" and repo and ctx.issue_number:
        return f"pull_request:merged:{repo}:{ctx.issue_number}"
    if repo and ctx.workflow_run_id:
        return f"workflow_run:{action}:{repo}:{ctx.workflow_run_id}"
    return f"{source}:{source_key}"


class JobQueue:
    def __init__(self, path: str | Path, *, migrate: bool = False):
        self.path = Path(path).expanduser()
        self.database = Database(self.path)
        if migrate:
            self.migrate()
        else:
            self.init()
        self.acknowledgements = AcknowledgementRepository(self.database)
        self.commit_statuses = CommitStatusRepository(self.database)
        self.runtime = RuntimeRepository(self.database)
        self.state = StateRepository(self.database)
        self.jobs = JobRepository(
            self.database,
            self.runtime,
            self.state,
            self.commit_statuses,
        )
        self.ingestion = IngestionRepository(
            self.database,
            self.jobs,
            self.runtime,
            self.acknowledgements,
            self.commit_statuses,
        )

    def connect(self) -> sqlite3.Connection:
        if not self.path.exists():
            self.init()
        return self.database.read_write()

    def init(self) -> None:
        if self.path.exists():
            with self.database.read_only() as con:
                validate_migrations(con)
            return
        with self.database.read_write() as con:
            self._initialize_database(con)

    def migrate(self) -> None:
        """Initialize or migrate a database through an explicit operator path."""
        with self.database.read_write() as con:
            self._initialize_database(con)

    def _ensure_initialized(self) -> None:
        if not self.path.exists():
            self.init()

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
        self._ensure_initialized()
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
        feedback_actionability = str(n.metadata.get("feedback_actionability") or "")
        structured_feedback = feedback_actionability in {
            "mentioned",
            "assigned",
            "pr_authored_by_bot",
            "defer_to_executor",
        }
        bot_authored_changes_requested = bool(
            n.metadata.get("github_event") == "pull_request_review"
            and n.metadata.get("review_state") == "changes_requested"
            and feedback_actionability == "pr_authored_by_bot"
        )
        if n.metadata:
            metadata.update(n.metadata)
        if structured_feedback:
            action = "reply_comment"
        if bot_authored_changes_requested:
            intent = "work_allowed"
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
        if structured_feedback:
            action = "reply_comment"
            metadata["action_guardrail"] = "structured_feedback_actionable"
        if bot_authored_changes_requested:
            intent = "work_allowed"
            metadata["intent_guardrail"] = (
                "bot_authored_pr_changes_requested_work_allowed"
            )
        if action == "submit_review":
            intent = "review_only"
            metadata["intent_guardrail"] = "submit_review_read_only"
        elif action == "sync_after_merge":
            intent = "review_only"
            metadata["intent_guardrail"] = "sync_after_merge_read_only"
        decision = policy.decision(n, ctx, action)
        status = {"auto": "done", "ask": "waiting_approval", "deny": "denied"}.get(decision, "pending")
        trigger_actor = (
            trigger_actor_details_from_notification(n)
            if source == "webhook"
            else trigger_actor_details_for_enqueue(n, ctx)
        )
        event_key = canonical_event_key(action, ctx, source, source_key)
        payload_hash = hashlib.sha256(n.body.encode("utf-8")).hexdigest()
        if trigger_actor and trigger_actor.user_id:
            metadata["trigger_actor_id"] = trigger_actor.user_id
        result = self.ingestion.ingest(
            IngestionRequest(
                notification=n,
                context=ctx,
                source=source,
                source_key=source_key,
                event_key=event_key,
                payload_hash=payload_hash,
                status=status,
                action=action,
                decision=decision,
                work_intent=intent,
                metadata=metadata,
                trigger_actor=trigger_actor,
                commit_status_feedback=self._commit_status_feedback,
            )
        )
        if policy.feedback_learning.enabled and result.capture_feedback:
            feedback.capture_feedback(
                self.path,
                n,
                ctx,
                action,
                decision,
                intent,
                trigger_actor=trigger_actor.login if trigger_actor else None,
                trigger_actor_avatar_url=(
                    trigger_actor.avatar_url if trigger_actor else None
                ),
            )
        return result.job, result.state

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
        self._ensure_initialized()
        return self.ingestion.quarantine(
            n,
            reason=reason,
            error=error,
            metadata=metadata,
            body_excerpt_chars=body_excerpt_chars,
        )

    def claim_next(self, worker_id: str, work_intents: frozenset[str] | set[str] | None = None) -> Job | None:
        self._ensure_initialized()
        return self.jobs.claim_next(worker_id, work_intents)

    def record_worker_heartbeat(
        self,
        worker_id: str,
        executor_id: str,
        pid: int,
        loop_state: str,
        active_job_id: int | None = None,
        recent_error_count: int = 0,
    ) -> None:
        self.runtime.record_worker_heartbeat(
            worker_id,
            executor_id,
            pid,
            loop_state,
            active_job_id,
            recent_error_count,
        )

    def delete_worker_heartbeats_except(self, executor_id: str) -> int:
        return self.runtime.delete_worker_heartbeats_except(executor_id)

    def register_runtime_process(
        self,
        job_id: int,
        worker_id: str,
        executor_id: str,
        identity: dict[str, int],
    ) -> bool:
        process = RuntimeProcess.from_identity(executor_id, worker_id, identity)
        return self.runtime.register_process(job_id, process)

    def mark_runtime_process_exited(self, job_id: int, worker_id: str) -> bool:
        return self.runtime.mark_process_exited(job_id, worker_id)

    def finish(self, job_id: int, status: str, summary: str, detail: str | None = None) -> None:
        self._ensure_initialized()
        self.jobs.finish(job_id, status, summary, detail)

    def request_cancel_running(
        self,
        job_id: int,
        *,
        actor: str,
        reason: str | None = None,
    ) -> Job | None:
        """Record a manual cancellation request for a running job."""
        self._ensure_initialized()
        return self.jobs.request_cancel_running(job_id, actor=actor, reason=reason)

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
        self._ensure_initialized()
        return self.jobs.mark_cancelled(
            job_id,
            actor=actor,
            reason=reason,
            signal_detail=signal_detail,
            followup_url=followup_url,
        )

    def requeue_running(
        self,
        job_id: int,
        summary: str,
        detail: str | None = None,
        *,
        fresh_session: bool = False,
    ) -> bool:
        self._ensure_initialized()
        return self.jobs.requeue_running(
            job_id, summary, detail, fresh_session=fresh_session
        )

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
        self._ensure_initialized()
        return self.jobs.block_running(
            summary,
            detail,
            job_ids=job_ids,
            locked_by=locked_by,
            older_than_seconds=older_than_seconds,
        )

    def update_work_intent(self, job_id: int, work_intent: str, summary: str) -> Job | None:
        self._ensure_initialized()
        job = self.jobs.get(job_id)
        if job is None:
            return None
        if job.action == "submit_review":
            work_intent = "review_only"
        return self.jobs.update_work_intent(job_id, work_intent, summary)

    def add_session_event(self, job_id: int, event_type: str, summary: str, detail: str | None = None) -> None:
        progress_kind = "visible" if event_type.startswith("openclaw_") else "semantic"
        self.runtime.add_job_session_event(
            job_id,
            event_type,
            summary,
            detail,
            progress_kind=progress_kind,
        )

    def add_worklog(self, job_id: int, phase: str, summary: str, detail: str | None = None) -> None:
        self.runtime.add_job_worklog(job_id, phase, summary, detail)

    def list_jobs(self, status: str | None = None, limit: int = 20) -> list[Job]:
        self._ensure_initialized()
        return self.jobs.list(status, limit)

    def retry(self, job_id: int, *, actor: str | None = None) -> bool:
        self._ensure_initialized()
        return self.jobs.retry(job_id, actor=actor)

    def dismiss(self, job_id: int, reason: str) -> bool:
        self._ensure_initialized()
        return self.jobs.dismiss(job_id, reason)

    def unlock_stale(self, older_than_seconds: int, job_ids: list[int] | None = None) -> int:
        self._ensure_initialized()
        return self.jobs.unlock_stale(older_than_seconds, job_ids)

    def get(self, job_id: int) -> Job | None:
        self._ensure_initialized()
        return self.jobs.get(job_id)

    def coalesced_contexts(self, job_id: int) -> list[GitHubContext]:
        self._ensure_initialized()
        return self.jobs.coalesced_contexts(job_id)

    def coalesced_trigger_actors(self, job_id: int) -> list[str]:
        self._ensure_initialized()
        return self.jobs.coalesced_trigger_actors(job_id)

    def stats(self) -> dict[str, int]:
        self._ensure_initialized()
        return self.jobs.stats()

    def pending_age_seconds(self) -> int | None:
        self._ensure_initialized()
        return self.jobs.pending_age_seconds()

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
