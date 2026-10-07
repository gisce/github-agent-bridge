from __future__ import annotations

import fcntl
import os
import signal
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass

from .dispatch import GitHubClient, OpenClawDispatcher, RunMode
from .models import GitHubContext
from .policy import Policy, complexity_from_metadata
from .queue import JobQueue
from .session_events import redact_event_detail
from .web_push import notify_job_completion


NO_FOLLOWUP_OK_MARKERS = (
    "no github follow-up comment was appropriate",
    "no github follow-up was appropriate",
    "no new github follow-up was appropriate",
)
NO_FOLLOWUP_DUPLICATE_MARKERS = (
    "duplicate",
    "already contains",
    "already has",
    "already reported",
    "no new information",
    "no new repository state",
    "no new state",
    "prior cleanup note",
    "repeat",
)
TRANSIENT_DISPATCH_ERROR_MARKERS = (
    "cli transcript compaction failed",
    "summarization failed: connection error",
    "codex app-server client closed before turn completed",
)


def _is_sqlite_contention_error(exc: sqlite3.OperationalError) -> bool:
    error_code = getattr(exc, "sqlite_errorcode", None)
    if isinstance(error_code, int) and error_code & 0xFF in {
        sqlite3.SQLITE_BUSY,
        sqlite3.SQLITE_LOCKED,
    }:
        return True
    message = str(exc).lower()
    return "database" in message and "locked" in message


@dataclass(frozen=True)
class ExecutorConfig:
    workers: int = 4
    idle_sleep_seconds: float = 1.0
    run_once: bool = False
    work_intents: frozenset[str] | None = None
    missing_followup_retries: int = 1
    transient_dispatch_retries: int = 2
    heartbeat_interval_seconds: float = 5.0


class ExecutorPool:
    def __init__(self, queue: JobQueue, policy: Policy, dispatcher: OpenClawDispatcher, github: GitHubClient | None = None, config: ExecutorConfig | None = None):
        self.queue = queue
        self.policy = policy
        self.dispatcher = dispatcher
        self.github = github or GitHubClient()
        self.config = config or ExecutorConfig()
        self.stop_event = threading.Event()
        self._commit_status_wakeup = threading.Event()
        self.executor_id = f"executor-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._executor_lock_file = None
        self._worker_failure_lock = threading.Lock()
        self._worker_failures: list[tuple[str, BaseException]] = []
        self._worker_state_lock = threading.Lock()
        self._worker_states: dict[str, tuple[str, int | None, int]] = {}

    def _set_worker_state(self, worker_id: str, loop_state: str, active_job_id: int | None = None, *, error: bool = False) -> None:
        with self._worker_state_lock:
            _, _, errors = self._worker_states.get(worker_id, ("starting", None, 0))
            self._worker_states[worker_id] = (loop_state, active_job_id, errors + int(error))

    def _record_worker_heartbeat(self, worker_id: str) -> None:
        with self._worker_state_lock:
            loop_state, active_job_id, errors = self._worker_states.get(worker_id, ("starting", None, 0))
        self.queue.record_worker_heartbeat(
            worker_id, self.executor_id, os.getpid(), loop_state, active_job_id, errors
        )

    def _record_worker_storage_error(self, worker_id: str) -> None:
        with self._worker_state_lock:
            loop_state, active_job_id, errors = self._worker_states.get(
                worker_id, ("starting", None, 0)
            )
            self._worker_states[worker_id] = (loop_state, active_job_id, errors + 1)

    def _heartbeat_loop(self, worker_id: str) -> None:
        while not self.stop_event.is_set():
            try:
                self._record_worker_heartbeat(worker_id)
            except sqlite3.OperationalError as exc:
                if not _is_sqlite_contention_error(exc):
                    raise
                self._record_worker_storage_error(worker_id)
                if self.stop_event.wait(self.config.idle_sleep_seconds):
                    break
                continue
            self.stop_event.wait(self.config.heartbeat_interval_seconds)
        try:
            self._record_worker_heartbeat(worker_id)
        except sqlite3.OperationalError as exc:
            if not _is_sqlite_contention_error(exc):
                raise

    def _claim_acknowledgement(
        self, job_id: int | None = None
    ) -> tuple[int, int, GitHubContext] | None:
        while not self.stop_event.is_set():
            try:
                return self.queue.claim_acknowledgement(job_id)
            except sqlite3.OperationalError as exc:
                if not _is_sqlite_contention_error(exc):
                    raise
                if self.stop_event.wait(self.config.idle_sleep_seconds):
                    return None
        return None

    def acknowledge_one(self, job_id: int | None = None) -> bool:
        acknowledgement = self._claim_acknowledgement(job_id)
        if acknowledgement is None:
            return False
        acknowledgement_id, acknowledged_job_id, ctx = acknowledgement
        try:
            ok = self.github.react_eyes(ctx)
            self.queue.finish_acknowledgement(acknowledgement_id, ok)
        except Exception as exc:
            ok = False
            self.queue.finish_acknowledgement(acknowledgement_id, False, f"{type(exc).__name__}: {exc}")
        self.queue.add_worklog(
            acknowledged_job_id,
            "acknowledged" if ok else "acknowledgement_failed",
            "GitHub 👀 reaction added" if ok else "GitHub 👀 reaction failed",
            ctx.short_url,
        )
        return True

    def acknowledge_job(self, job_id: int) -> bool:
        while self.acknowledge_one(job_id):
            pass
        return self.queue.acknowledgement_ok(job_id)

    def _claim_commit_status(self, job_id: int | None = None):
        while not self.stop_event.is_set():
            try:
                return self.queue.claim_commit_status(job_id)
            except sqlite3.OperationalError as exc:
                if not _is_sqlite_contention_error(exc):
                    raise
                if self.stop_event.wait(self.config.idle_sleep_seconds):
                    return None
        return None

    def publish_commit_status_one(self, job_id: int | None = None) -> bool:
        claim = self._claim_commit_status(job_id)
        if claim is None:
            return False
        live = (
            str(getattr(self.github, "mode", RunMode.LIVE.value))
            == RunMode.LIVE.value
        )
        try:
            error = None
            if not live:
                # Consume the outbox item without resolving the PR or calling
                # GitHub so a later live run cannot leak shadow/dry-run work.
                ok = True
            else:
                sha = claim.sha
                if not sha:
                    sha, error = self.github.resolve_commit_sha(
                        claim.github_context
                    )
                    if sha:
                        sha = self.queue.pin_commit_status_sha(claim.id, sha)
                if not sha:
                    ok = False
                else:
                    dashboard_url = os.getenv(
                        "GITHUB_AGENT_BRIDGE_DASHBOARD_PUBLIC_URL", ""
                    ).rstrip("/")
                    target_url = (
                        f"{dashboard_url}/jobs/{claim.job_id}"
                        if dashboard_url
                        else None
                    )
                    ok, error = self.github.create_commit_status(
                        claim.repo,
                        sha,
                        claim.desired_state,
                        claim.context_name,
                        claim.description,
                        target_url,
                    )
            self.queue.finish_commit_status(
                claim.id, claim.revision, ok, error
            )
        except Exception as exc:
            ok = False
            error = f"{type(exc).__name__}: {exc}"
            self.queue.finish_commit_status(
                claim.id, claim.revision, False, error
            )
        if live:
            self.queue.add_worklog(
                claim.job_id,
                "commit_status_published" if ok else "commit_status_failed",
                (
                    f"GitHub commit status {claim.desired_state} published"
                    if ok
                    else f"GitHub commit status {claim.desired_state} delivery failed"
                ),
                error,
            )
        return True

    def publish_commit_statuses(self, job_id: int | None = None) -> None:
        while self.publish_commit_status_one(job_id):
            pass

    def _wake_commit_status_publisher(self) -> None:
        self._commit_status_wakeup.set()

    def work_one(self, worker_id: str | None = None) -> bool:
        worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        if self.queue.executor_paused():
            self._set_worker_state(worker_id, "paused")
            return False
        self._set_worker_state(worker_id, "claiming")
        job = self.queue.claim_next(worker_id, self.config.work_intents)
        if not job:
            self._set_worker_state(worker_id, "idle")
            return False
        self._set_worker_state(worker_id, "running", job.id)
        self._wake_commit_status_publisher()
        if self.stop_event.is_set():
            self.queue.block_running(
                "executor shutdown interrupted job before dispatch",
                "The executor received a shutdown request after claiming this job. It was not dispatched or auto-requeued.",
                job_ids=[job.id],
                locked_by={worker_id},
            )
            self._wake_commit_status_publisher()
            return True
        dispatched = False
        try:
            assigned_to_bot = self.github.is_assigned_to_current_user(job.context)
            authored_by_bot = self.github.is_pull_request_authored_by_current_user(job.context)
            if job.action == "reply_comment" and job.context.review_id and self.github.is_non_actionable_review(job.context):
                reaction_ok = self.acknowledge_job(job.id)
                ack_ok = self.github.react_ack_no_comment(job.context)
                summary = "non-actionable review; skipped dispatch"
                detail = f"eyes={reaction_ok} ack={ack_ok}"
                self._finish(job, "done", summary, detail)
                return True
            if job.action == "reply_comment" and job.context.comment_id and not assigned_to_bot and not self.github.issue_comment_addresses_current_user(job.context):
                reaction_ok = self.acknowledge_job(job.id)
                ack_ok = self.github.react_ack_no_comment(job.context)
                summary = "comment not addressed to bot and bot not assigned; skipped dispatch"
                detail = f"eyes={reaction_ok} ack={ack_ok}"
                self._finish(job, "done", summary, detail)
                return True
            if job.action == "reply_comment" and job.work_intent == "review_only" and (assigned_to_bot or authored_by_bot):
                reason = "PR/issue assigned to authenticated bot" if assigned_to_bot else "PR authored by authenticated bot"
                self.queue.add_session_event(
                    job.id,
                    "action_mode_retained",
                    "review_only retained; assignment/authorship alone does not grant write permission",
                    reason,
                )
            reaction_ok = self.acknowledge_job(job.id)
            self.queue.add_session_event(job.id, "dispatch_started", "OpenClaw agent dispatch started", f"reaction_ok={reaction_ok}")
            complexity = complexity_from_metadata(job.metadata)
            model_route = self.policy.model_route_for(job.repo, job.action, job.work_intent, complexity)
            self.queue.add_session_event(job.id, "model_route_selected", "OpenClaw model route selected", model_route.summary())
            if job.metadata.get("fresh_session_on_retry") and job.attempts > 1:
                self.queue.add_session_event(
                    job.id,
                    "session_rescue_selected",
                    "fresh OpenClaw session selected after compaction failure",
                    f"attempt={job.attempts}",
                )
            try:
                result = self.dispatcher.dispatch(
                    job,
                    self.policy,
                    reaction_ok=reaction_ok,
                    activity_callback=lambda event_type, summary, detail: self.queue.add_session_event(job.id, event_type, summary, redact_event_detail(detail)),
                    process_callback=lambda identity: self.queue.register_runtime_process(
                        job.id,
                        worker_id,
                        self.executor_id,
                        identity,
                    ),
                )
            finally:
                self.queue.mark_runtime_process_exited(job.id, worker_id)
            dispatched = True
            dispatch_detail = "\n".join(part for part in [result.stdout, result.stderr] if part)
            self.queue.add_session_event(
                job.id,
                "dispatch_finished" if result.ok else "dispatch_failed",
                f"OpenClaw agent exited rc={result.returncode}",
                redact_event_detail(dispatch_detail),
            )
            if result.ok:
                followup_url = self.github.visible_followup_after_trigger(job.context)
                missing_followup_ok = self._missing_followup_is_acceptable(job, result)
                if job.work_intent == "work_allowed" and job.action not in {"archive_notification", "workflow_run_failed"} and not followup_url and not missing_followup_ok:
                    summary = "agent finished without visible GitHub follow-up"
                    detail = result.detail or "OpenClaw command succeeded, but no new bot comment was found in the GitHub thread."
                    if job.attempts <= self.config.missing_followup_retries:
                        self.queue.requeue_running(job.id, "agent finished without visible GitHub follow-up; auto-requeued", detail)
                        self._wake_commit_status_publisher()
                        return True
                    self._finish(job, "blocked", summary, detail, notify_completion=True)
                    return True
                summary = "👀 reaction ok + agent dispatch queued" if reaction_ok else "agent dispatch queued; reaction failed or unavailable"
                detail = f"followup_url={followup_url}; {result.detail}" if followup_url else result.detail
                self._finish(job, "done", summary, detail, notify_completion=True, followup_url=followup_url)
            else:
                reason = (
                    "executor shutdown interrupted dispatch"
                    if result.cancelled
                    else "dispatch timeout"
                    if result.timed_out
                    else f"dispatch failed rc={result.returncode}"
                )
                followup_url = self.github.visible_followup_after_trigger(job.context)
                if followup_url:
                    summary = "dispatch failed after producing visible GitHub follow-up"
                    detail = f"followup_url={followup_url}; {reason}; {result.detail}"
                    self._finish(job, "blocked", summary, detail, notify_completion=True, followup_url=followup_url)
                    return True
                if self._dispatch_failure_is_retryable(result) and job.attempts <= self.config.transient_dispatch_retries:
                    self.queue.requeue_running(
                        job.id,
                        "transient OpenClaw dispatch failure; auto-requeued",
                        result.detail,
                        fresh_session=self._dispatch_failure_needs_fresh_session(result),
                    )
                    self._wake_commit_status_publisher()
                    return True
                self._finish(job, "blocked", reason, result.detail, notify_completion=True)
        except Exception as exc:
            self._finish(job, "blocked", f"executor exception: {type(exc).__name__}", str(exc), notify_completion=dispatched)
        return True

    def _finish(
        self,
        job,
        status: str,
        summary: str,
        detail: str | None = None,
        *,
        notify_completion: bool = False,
        followup_url: str | None = None,
    ) -> None:
        self.queue.finish(job.id, status, summary, detail)
        self._wake_commit_status_publisher()
        if not notify_completion:
            return
        actors = [actor for actor in [job.trigger_actor, *self.queue.coalesced_trigger_actors(job.id)] if actor]
        notify_job_completion(
            self.queue.path,
            actors=actors,
            job_id=job.id,
            work_key=job.work_key,
            status=status,
            summary=summary,
            detail=detail,
            followup_url=followup_url,
        )

    def _missing_followup_is_acceptable(self, job, result) -> bool:
        if job.action not in {"reply_comment", "sync_after_merge"}:
            return False
        output = f"{result.stdout}\n{result.stderr}".lower()
        return any(marker in output for marker in NO_FOLLOWUP_OK_MARKERS) and any(marker in output for marker in NO_FOLLOWUP_DUPLICATE_MARKERS)

    def _dispatch_failure_is_retryable(self, result: DispatchResult) -> bool:
        if result.timed_out:
            return False
        output = f"{result.stdout}\n{result.stderr}\n{result.detail}".lower()
        return any(marker in output for marker in TRANSIENT_DISPATCH_ERROR_MARKERS)

    @staticmethod
    def _dispatch_failure_needs_fresh_session(result: DispatchResult) -> bool:
        output = f"{result.stdout}\n{result.stderr}\n{result.detail}".lower()
        return "transcript compaction failed" in output or "turn prefix summarization failed" in output

    def _acknowledgement_loop(self) -> None:
        while not self.stop_event.is_set():
            if not self.acknowledge_one():
                self.stop_event.wait(self.config.idle_sleep_seconds)

    def _commit_status_loop(self) -> None:
        while not self.stop_event.is_set():
            if self.publish_commit_status_one():
                continue
            self._commit_status_wakeup.wait(self.config.idle_sleep_seconds)
            self._commit_status_wakeup.clear()

    def _loop(self, worker_id: str) -> None:
        while not self.stop_event.is_set():
            did = self.work_one(worker_id)
            if self.config.run_once:
                return
            if not did:
                time.sleep(self.config.idle_sleep_seconds)

    def _run_worker(self, worker_id: str) -> None:
        try:
            self._loop(worker_id)
        except BaseException as exc:
            self._set_worker_state(worker_id, "error", error=True)
            self._record_worker_heartbeat(worker_id)
            with self._worker_failure_lock:
                self._worker_failures.append((worker_id, exc))
            self._request_shutdown()

    def _request_shutdown(self) -> None:
        self.stop_event.set()
        self._commit_status_wakeup.set()
        shutdown = getattr(self.dispatcher, "shutdown", None)
        if callable(shutdown):
            shutdown()

    def _handle_signal(self, signum, frame) -> None:
        self._request_shutdown()

    def _install_signal_handlers(self) -> dict[signal.Signals, object]:
        if threading.current_thread() is not threading.main_thread():
            return {}
        previous = {}
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous[sig] = signal.getsignal(sig)
            signal.signal(sig, self._handle_signal)
        return previous

    @staticmethod
    def _restore_signal_handlers(previous: dict[signal.Signals, object]) -> None:
        for sig, handler in previous.items():
            signal.signal(sig, handler)

    def _acquire_executor_lock(self) -> None:
        lock_path = self.queue.path.with_name(f"{self.queue.path.name}.executor.lock")
        lock_file = lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock_file.close()
            raise RuntimeError(f"another executor already holds {lock_path}") from None
        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(f"{self.executor_id}\n")
        lock_file.flush()
        self._executor_lock_file = lock_file

    def _release_executor_lock(self) -> None:
        if self._executor_lock_file is None:
            return
        fcntl.flock(self._executor_lock_file.fileno(), fcntl.LOCK_UN)
        self._executor_lock_file.close()
        self._executor_lock_file = None

    def run(self) -> None:
        self._acquire_executor_lock()
        previous_handlers = self._install_signal_handlers()
        worker_count = 1 if self.config.run_once or self.config.workers <= 1 else self.config.workers
        worker_ids = [f"{self.executor_id}/worker-{i}" for i in range(worker_count)]
        threads: list[threading.Thread] = []
        try:
            self.queue.delete_worker_heartbeats_except(self.executor_id)
            self.queue.block_running(
                "orphaned running job recovered at executor startup",
                "No prior executor process owns this running job. It was blocked, not auto-requeued, to avoid duplicate external actions.",
            )
            self.queue.recover_acknowledgements()
            self.queue.recover_commit_statuses()
            self.queue.set_state("executor_process_tracking_id", self.executor_id)
            self.queue.set_state("executor_worker_count", str(worker_count))
            for worker_id in worker_ids:
                self._set_worker_state(worker_id, "starting")
                self._record_worker_heartbeat(worker_id)
            heartbeat_threads = [
                threading.Thread(target=self._heartbeat_loop, args=(worker_id,), daemon=True)
                for worker_id in worker_ids
            ]
            worker_threads = [
                threading.Thread(target=self._run_worker, args=(worker_id,), daemon=False)
                for worker_id in worker_ids
            ]
            acknowledgement_thread = threading.Thread(target=self._acknowledgement_loop, daemon=False)
            commit_status_thread = threading.Thread(target=self._commit_status_loop, daemon=False)
            threads = [*heartbeat_threads, commit_status_thread, *worker_threads, acknowledgement_thread]
            for thread in threads:
                thread.start()
            while any(thread.is_alive() for thread in worker_threads):
                time.sleep(0.5)
        except KeyboardInterrupt:
            self._request_shutdown()
        finally:
            self._request_shutdown()
            for thread in threads:
                thread.join()
            self.queue.block_running(
                "executor shutdown interrupted running job",
                "The executor stopped before this job completed. It was blocked, not auto-requeued, to avoid duplicate external actions.",
                locked_by=set(worker_ids),
            )
            self._restore_signal_handlers(previous_handlers)
            self._release_executor_lock()
        if self._worker_failures:
            failures = ", ".join(f"{worker_id}: {type(exc).__name__}: {exc}" for worker_id, exc in self._worker_failures)
            raise RuntimeError(f"executor worker terminated unexpectedly: {failures}")
