import sqlite3

from github_agent_bridge.dashboard_data import job_session_events
from github_agent_bridge.dispatch import DispatchResult
from github_agent_bridge.executor import ExecutorConfig, ExecutorPool
from github_agent_bridge.models import Notification
from github_agent_bridge.policy import Policy
from github_agent_bridge.queue import JobQueue


class FakeGitHub:
    def __init__(self, assigned: bool, mentioned: bool = True, non_actionable_review: bool = False, authored: bool = False, answered_url: str | None = None):
        self.assigned = assigned
        self.mentioned = mentioned
        self.non_actionable_review = non_actionable_review
        self.authored = authored
        self.answered_url = answered_url
        self.followup_url = answered_url or "https://github.com/gisce/erp/issues/27315#issuecomment-2"
        self.eyes = 0
        self.acks = 0
        self.eye_comment_ids = []

    def is_assigned_to_current_user(self, ctx):
        return self.assigned

    def is_pull_request_authored_by_current_user(self, ctx):
        return self.authored

    def issue_comment_addresses_current_user(self, ctx):
        return self.mentioned

    def is_non_actionable_review(self, ctx):
        return self.non_actionable_review

    def current_user_commented_after(self, ctx):
        return self.answered_url

    def visible_followup_after_trigger(self, ctx):
        return self.followup_url

    def react_eyes(self, ctx):
        self.eyes += 1
        self.eye_comment_ids.append(ctx.comment_id)
        return True

    def react_ack_no_comment(self, ctx):
        self.acks += 1
        return True


class RecordingDispatcher:
    def __init__(self):
        self.jobs = []

    def dispatch(self, job, policy, reaction_ok=None, activity_callback=None):
        self.jobs.append(job)
        if activity_callback:
            activity_callback("openclaw_stdout", "OpenClaw CLI output", "thinking about the change")
            activity_callback("openclaw_stderr", "OpenClaw CLI error output", "token=secret ghp_abcdefghijklmnopqrstuvwxyz")
        return DispatchResult(True, 0, "ok", "", False, reaction_ok, ["openclaw"])


def enqueue_pr_review(queue: JobQueue):
    notification = Notification(
        uid=2,
        message_id="<gisce/erp/pull/27737/review/4282224025@github.com>",
        subject="Re: [gisce/erp] Endurecer ir.values sin nuevos pickles (PR #27737)",
        from_addr="notifications@github.com",
        body="Copilot wasn't able to review any files in this pull request. https://github.com/gisce/erp/pull/27737#pullrequestreview-4282224025",
    )
    job, state = queue.enqueue(notification, Policy(trusted_orgs={"gisce"}))
    assert state == "enqueued"
    assert job is not None
    assert job.action == "reply_comment"
    assert job.context.review_id == 4282224025
    return job


def enqueue_pr_comment(queue: JobQueue):
    notification = Notification(
        uid=1,
        message_id="<gisce/erp/pull/27315/c1@github.com>",
        subject="Re: [gisce/erp] Permitir caller en los dominios (PR #27315)",
        from_addr="notifications@github.com",
        body="@pilipilisbot però la transacció en què s'executa que entra per eval_domain és readonly https://github.com/gisce/erp/pull/27315#issuecomment-1",
    )
    job, state = queue.enqueue(notification, Policy(trusted_orgs={"gisce"}))
    assert state == "enqueued"
    assert job is not None
    assert job.action == "reply_comment"
    assert job.work_intent == "review_only"
    return job


def enqueue_workflow_run_failed(queue: JobQueue):
    notification = Notification(
        uid=3,
        message_id="<gisce/erp/actions/runs/26325244472@github.com>",
        subject="[gisce/erp] Run failed: tests - main",
        from_addr="notifications@github.com",
        body="Run failed: https://github.com/gisce/erp/actions/runs/26325244472",
    )
    job, state = queue.enqueue(notification, Policy(trusted_orgs={"gisce"}))
    assert state == "enqueued"
    assert job is not None
    assert job.action == "workflow_run_failed"
    return job


def test_assigned_pr_comment_upgrades_to_work_allowed(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    enqueue_pr_comment(queue)
    dispatcher = RecordingDispatcher()

    pool = ExecutorPool(queue, Policy(trusted_orgs={"gisce"}), dispatcher, github=FakeGitHub(assigned=True), config=ExecutorConfig(run_once=True))
    assert pool.work_one("worker-test") is True

    assert dispatcher.jobs[0].work_intent == "work_allowed"
    stored = queue.get(dispatcher.jobs[0].id)
    assert stored is not None
    assert stored.work_intent == "work_allowed"


def test_executor_records_session_activity_events(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    queue = JobQueue(db)
    enqueue_pr_comment(queue)
    dispatcher = RecordingDispatcher()

    pool = ExecutorPool(queue, Policy(trusted_orgs={"gisce"}), dispatcher, github=FakeGitHub(assigned=False, mentioned=True), config=ExecutorConfig(run_once=True))
    assert pool.work_one("worker-test") is True

    event_types = [event["event_type"] for event in job_session_events(db, dispatcher.jobs[0].id)]
    assert event_types == ["claimed", "dispatch_started", "openclaw_stdout", "openclaw_stderr", "dispatch_finished", "done"]
    stderr_event = job_session_events(db, dispatcher.jobs[0].id)[3]
    assert stderr_event["detail"] == "token=[redacted] [redacted]"


def test_executor_survives_when_failure_cannot_be_recorded(tmp_path, monkeypatch, caplog):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    job = enqueue_pr_comment(queue)
    dispatcher = RecordingDispatcher()
    original_finish = queue.finish
    finish_calls = 0

    def fail_finish(*args, **kwargs):
        nonlocal finish_calls
        finish_calls += 1
        raise sqlite3.OperationalError("database or disk is full")

    monkeypatch.setattr(queue, "finish", fail_finish)
    pool = ExecutorPool(queue, Policy(trusted_orgs={"gisce"}), dispatcher, github=FakeGitHub(assigned=False, mentioned=True), config=ExecutorConfig(run_once=True))

    assert pool.work_one("worker-test") is True
    assert finish_calls == 2
    assert "failed to mark job" in caplog.text
    assert "database or disk is full" in caplog.text

    monkeypatch.setattr(queue, "finish", original_finish)
    stored = queue.get(job.id)
    assert stored is not None
    assert stored.status == "running"


def test_unassigned_mentioned_pr_comment_stays_review_only(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    enqueue_pr_comment(queue)
    dispatcher = RecordingDispatcher()

    pool = ExecutorPool(queue, Policy(trusted_orgs={"gisce"}), dispatcher, github=FakeGitHub(assigned=False, mentioned=True), config=ExecutorConfig(run_once=True))
    assert pool.work_one("worker-test") is True

    assert dispatcher.jobs[0].work_intent == "review_only"
    stored = queue.get(dispatcher.jobs[0].id)
    assert stored is not None
    assert stored.work_intent == "review_only"


def test_coalesced_notifications_are_reacted_to_before_dispatch(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    enqueue_pr_comment(queue)
    notification = Notification(
        uid=2,
        message_id="<gisce/erp/pull/27315/c2@github.com>",
        subject="Re: [gisce/erp] Permitir caller en los dominios (PR #27315)",
        from_addr="notifications@github.com",
        body="@pilipilisbot segon comentari https://github.com/gisce/erp/pull/27315#issuecomment-2",
    )
    job, state = queue.enqueue(notification, Policy(trusted_orgs={"gisce"}))
    assert state == "coalesced"
    dispatcher = RecordingDispatcher()
    github = FakeGitHub(assigned=False, mentioned=True)

    pool = ExecutorPool(queue, Policy(trusted_orgs={"gisce"}), dispatcher, github=github, config=ExecutorConfig(run_once=True))
    assert pool.work_one("worker-test") is True

    assert len(dispatcher.jobs) == 1
    assert dispatcher.jobs[0].id == job.id
    assert 2 in github.eye_comment_ids


def test_bot_authored_pr_review_comment_upgrades_to_work_allowed(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    enqueue_pr_comment(queue)
    dispatcher = RecordingDispatcher()

    pool = ExecutorPool(queue, Policy(trusted_orgs={"gisce"}), dispatcher, github=FakeGitHub(assigned=False, mentioned=True, authored=True), config=ExecutorConfig(run_once=True))
    assert pool.work_one("worker-test") is True

    assert dispatcher.jobs[0].work_intent == "work_allowed"
    stored = queue.get(dispatcher.jobs[0].id)
    assert stored is not None
    assert stored.work_intent == "work_allowed"


def test_unassigned_unmentioned_pr_comment_reacts_without_dispatch(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    job = enqueue_pr_comment(queue)
    dispatcher = RecordingDispatcher()
    github = FakeGitHub(assigned=False, mentioned=False)

    pool = ExecutorPool(queue, Policy(trusted_orgs={"gisce"}), dispatcher, github=github, config=ExecutorConfig(run_once=True))
    assert pool.work_one("worker-test") is True

    assert dispatcher.jobs == []
    assert github.eyes == 1
    assert github.acks == 1
    stored = queue.get(job.id)
    assert stored is not None
    assert stored.status == "done"


def test_first_attempt_dispatches_even_when_bot_already_commented_after_trigger(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    job = enqueue_pr_comment(queue)
    dispatcher = RecordingDispatcher()
    github = FakeGitHub(assigned=True, answered_url="https://github.com/gisce/erp/pull/27315#issuecomment-2")

    pool = ExecutorPool(queue, Policy(trusted_orgs={"gisce"}), dispatcher, github=github, config=ExecutorConfig(run_once=True))
    assert pool.work_one("worker-test") is True

    assert len(dispatcher.jobs) == 1
    assert dispatcher.jobs[0].id == job.id
    assert github.eyes == 1
    stored = queue.get(job.id)
    assert stored is not None
    assert stored.status == "done"


def test_retry_dispatches_even_when_prior_bot_comment_exists(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    job = enqueue_pr_comment(queue)
    dispatcher = RecordingDispatcher()
    github = FakeGitHub(assigned=True, answered_url="https://github.com/gisce/erp/pull/27315#issuecomment-2")

    queue.requeue_running(job.id, "simulate retry")
    with queue.connect() as con:
        con.execute("UPDATE jobs SET attempts=1, status='pending' WHERE id=?", (job.id,))

    pool = ExecutorPool(queue, Policy(trusted_orgs={"gisce"}), dispatcher, github=github, config=ExecutorConfig(run_once=True))
    assert pool.work_one("worker-test") is True

    assert len(dispatcher.jobs) == 1
    assert dispatcher.jobs[0].id == job.id
    assert github.eyes == 1
    stored = queue.get(job.id)
    assert stored is not None
    assert stored.status == "done"


def test_work_allowed_dispatch_auto_retries_once_without_visible_github_followup(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    job = enqueue_pr_comment(queue)
    dispatcher = RecordingDispatcher()
    github = FakeGitHub(assigned=True)
    github.followup_url = None

    pool = ExecutorPool(queue, Policy(trusted_orgs={"gisce"}), dispatcher, github=github, config=ExecutorConfig(run_once=True))
    assert pool.work_one("worker-test") is True

    assert dispatcher.jobs
    stored = queue.get(job.id)
    assert stored is not None
    assert stored.status == "pending"
    assert stored.last_error is None
    assert stored.attempts == 1


def test_work_allowed_dispatch_blocks_after_auto_retry_without_visible_github_followup(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    job = enqueue_pr_comment(queue)
    dispatcher = RecordingDispatcher()
    github = FakeGitHub(assigned=True)
    github.followup_url = None

    pool = ExecutorPool(queue, Policy(trusted_orgs={"gisce"}), dispatcher, github=github, config=ExecutorConfig(run_once=True))
    assert pool.work_one("worker-test") is True
    assert pool.work_one("worker-test") is True

    assert len(dispatcher.jobs) == 2
    stored = queue.get(job.id)
    assert stored is not None
    assert stored.status == "blocked"
    assert stored.last_error == "ok"
    assert stored.attempts == 2


def test_workflow_run_failed_dispatch_does_not_require_thread_followup(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    job = enqueue_workflow_run_failed(queue)
    dispatcher = RecordingDispatcher()
    github = FakeGitHub(assigned=False, mentioned=False)
    github.followup_url = None

    pool = ExecutorPool(queue, Policy(trusted_orgs={"gisce"}), dispatcher, github=github, config=ExecutorConfig(run_once=True))
    assert pool.work_one("worker-test") is True

    assert dispatcher.jobs
    assert github.eyes == 1
    stored = queue.get(job.id)
    assert stored is not None
    assert stored.status == "done"


def test_non_actionable_review_reacts_without_dispatch_even_when_assigned(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    job = enqueue_pr_review(queue)
    dispatcher = RecordingDispatcher()
    github = FakeGitHub(assigned=True, non_actionable_review=True)

    pool = ExecutorPool(queue, Policy(trusted_orgs={"gisce"}), dispatcher, github=github, config=ExecutorConfig(run_once=True))
    assert pool.work_one("worker-test") is True

    assert dispatcher.jobs == []
    assert github.eyes == 1
    assert github.acks == 1
    stored = queue.get(job.id)
    assert stored is not None
    assert stored.status == "done"
