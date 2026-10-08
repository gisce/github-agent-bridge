import sqlite3

from github_agent_bridge.dispatch import DispatchResult
from github_agent_bridge.executor import ExecutorConfig, ExecutorPool
from github_agent_bridge.models import Notification
from github_agent_bridge.policy import Policy
from github_agent_bridge.queue import JobQueue


SHA = "0123456789abcdef0123456789abcdef01234567"


def webhook_notification(
    *,
    delivery: str,
    target: str = "pull",
    number: int = 266,
    include_sha: bool = False,
) -> Notification:
    commit_url = (
        f"https://github.com/gisce/github-agent-bridge/commit/{SHA}\n"
        if include_sha
        else ""
    )
    return Notification(
        uid=None,
        message_id=f"<{delivery}@github.com>",
        subject="[gisce/github-agent-bridge] Commit status feedback (#266)",
        from_addr="ecarreras <notifications@github.com>",
        body=(
            f"@giscebot implementa-ho\n{commit_url}"
            f"https://github.com/gisce/github-agent-bridge/{target}/{number}"
            f"#issuecomment-{number}"
        ),
        auth={"spf": True, "dkim": True, "dmarc": True},
    )


def commit_notification(*, delivery: str) -> Notification:
    return Notification(
        uid=None,
        message_id=f"<{delivery}@github.com>",
        subject="[gisce/github-agent-bridge] Commit status feedback",
        from_addr="ecarreras <notifications@github.com>",
        body=(
            "@giscebot implementa-ho\n"
            f"https://github.com/gisce/github-agent-bridge/commit/{SHA}"
            "#commitcomment-266"
        ),
        auth={"spf": True, "dkim": True, "dmarc": True},
    )


def ingest_webhook(queue: JobQueue, notification: Notification):
    job, state = queue.ingest(
        notification,
        Policy(trusted_orgs={"gisce"}, bot_logins={"giscebot"}),
        source="webhook",
        source_key=notification.message_id,
    )
    assert state == "enqueued"
    assert job is not None
    return job


def commit_status_row(queue: JobQueue, job_id: int) -> sqlite3.Row | None:
    with queue.connect() as con:
        return con.execute(
            "SELECT * FROM job_commit_statuses WHERE job_id=?", (job_id,)
        ).fetchone()


def test_only_webhook_pr_and_commit_jobs_create_commit_status_feedback(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    pr_job = ingest_webhook(
        queue,
        webhook_notification(delivery="webhook-pr", include_sha=True),
    )
    commit_job = ingest_webhook(
        queue,
        commit_notification(delivery="webhook-commit"),
    )
    issue_job = ingest_webhook(
        queue,
        webhook_notification(delivery="webhook-issue", target="issues", number=267),
    )
    email_job, state = queue.enqueue(
        webhook_notification(delivery="email-pr", number=268, include_sha=True),
        Policy(trusted_orgs={"gisce"}, bot_logins={"giscebot"}),
    )

    assert state == "enqueued"
    assert email_job is not None
    status = commit_status_row(queue, pr_job.id)
    assert status is not None
    assert status["sha"] is None
    assert status["desired_state"] == "pending"
    assert status["description"] == f"Agent queued (job #{pr_job.id})"
    assert commit_status_row(queue, commit_job.id)["sha"] == SHA
    assert commit_status_row(queue, issue_job.id) is None
    assert commit_status_row(queue, email_job.id) is None


def test_issue_text_cannot_turn_a_plain_issue_into_commit_status_target(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    notification = Notification(
        uid=None,
        message_id="<plain-issue-with-pr-link@github.com>",
        subject="[gisce/github-agent-bridge] Plain issue (#266)",
        from_addr="ecarreras <notifications@github.com>",
        body=(
            "@giscebot revisa també "
            "https://github.com/gisce/github-agent-bridge/pull/266\n"
            "https://github.com/gisce/github-agent-bridge/issues/266"
            "#issuecomment-999"
        ),
        auth={"spf": True, "dkim": True, "dmarc": True},
    )

    job = ingest_webhook(queue, notification)

    assert job.context.short_url.endswith("/issues/266#issuecomment-999")
    assert job.context.is_pull_request is False
    assert commit_status_row(queue, job.id) is None


def test_commit_status_desired_state_follows_job_lifecycle_and_keeps_sha(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    job = ingest_webhook(
        queue,
        commit_notification(delivery="lifecycle"),
    )

    assert queue.claim_next("worker-1") is not None
    running = commit_status_row(queue, job.id)
    assert running["desired_state"] == "pending"
    assert running["description"] == "Agent working (attempt 1)"

    assert queue.requeue_running(job.id, "retry transient failure") is True
    retry = commit_status_row(queue, job.id)
    assert retry["sha"] == SHA
    assert retry["description"] == "Agent retry scheduled (attempt 2)"

    assert queue.claim_next("worker-2") is not None
    queue.finish(job.id, "done", "agent response published")
    terminal = commit_status_row(queue, job.id)
    assert terminal["sha"] == SHA
    assert terminal["desired_state"] == "success"
    assert terminal["description"] == "Agent finished; follow-up available"


def test_stale_delivery_generation_cannot_hide_a_newer_desired_status(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    job = ingest_webhook(
        queue,
        webhook_notification(delivery="stale-generation", include_sha=True),
    )
    claim = queue.claim_commit_status()
    assert claim is not None

    assert queue.claim_next("worker-1") is not None
    queue.finish_commit_status(claim.id, claim.revision, True)

    status = commit_status_row(queue, job.id)
    assert status["delivered_revision"] == claim.revision
    assert status["revision"] > status["delivered_revision"]
    assert status["delivery_status"] == "pending"
    assert status["description"] == "Agent working (attempt 1)"


class RecordingDispatcher:
    def dispatch(self, job, policy, reaction_ok=None, activity_callback=None, process_callback=None):
        return DispatchResult(True, 0, "done", "", reaction_ok=reaction_ok)


class StatusGitHub:
    def __init__(self, *, status_ok: bool = True, mode: str = "live"):
        self.status_ok = status_ok
        self.mode = mode
        self.statuses = []

    def resolve_commit_sha(self, ctx):
        if ctx.target_kind in {"commit", "commit_comment"}:
            return ctx.commit_sha, None
        return SHA, None

    def create_commit_status(
        self,
        repo,
        sha,
        state,
        context,
        description,
        target_url=None,
    ):
        self.statuses.append(
            {
                "repo": repo,
                "sha": sha,
                "state": state,
                "context": context,
                "description": description,
                "target_url": target_url,
            }
        )
        return self.status_ok, None if self.status_ok else "GitHub status API failed"

    def is_assigned_to_current_user(self, ctx):
        return True

    def is_pull_request_authored_by_current_user(self, ctx):
        return False

    def issue_comment_addresses_current_user(self, ctx):
        return True

    def is_non_actionable_review(self, ctx):
        return False

    def react_eyes(self, ctx):
        return True

    def visible_followup_after_trigger(self, ctx):
        return "https://github.com/gisce/github-agent-bridge/issues/266#issuecomment-2"


def test_executor_publishes_pinned_queued_and_terminal_statuses(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "GITHUB_AGENT_BRIDGE_DASHBOARD_PUBLIC_URL", "https://bridge.example.com/"
    )
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    job = ingest_webhook(queue, webhook_notification(delivery="executor"))
    github = StatusGitHub()
    pool = ExecutorPool(
        queue,
        Policy(trusted_orgs={"gisce"}),
        RecordingDispatcher(),
        github=github,
        config=ExecutorConfig(run_once=True),
    )

    pool.publish_commit_statuses(job.id)
    assert pool.work_one("worker-test") is True
    assert [status["state"] for status in github.statuses] == ["pending"]
    pool.publish_commit_statuses(job.id)

    assert [status["state"] for status in github.statuses] == ["pending", "success"]
    assert {status["sha"] for status in github.statuses} == {SHA}
    assert {status["context"] for status in github.statuses} == {
        "github-agent-bridge/agent"
    }
    assert github.statuses[-1]["target_url"] == (
        f"https://bridge.example.com/jobs/{job.id}"
    )
    assert queue.get(job.id).status == "done"


def test_commit_status_delivery_failure_never_blocks_completed_job(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    job = ingest_webhook(
        queue,
        webhook_notification(delivery="delivery-failure"),
    )
    github = StatusGitHub(status_ok=False)
    pool = ExecutorPool(
        queue,
        Policy(trusted_orgs={"gisce"}),
        RecordingDispatcher(),
        github=github,
        config=ExecutorConfig(run_once=True),
    )

    assert pool.work_one("worker-test") is True
    pool.publish_commit_statuses(job.id)

    stored = queue.get(job.id)
    status = commit_status_row(queue, job.id)
    assert stored is not None
    assert stored.status == "done"
    assert status["desired_state"] == "success"
    assert status["delivery_status"] == "failed"
    assert status["attempts"] == 3
    assert status["last_error"] == "GitHub status API failed"


def test_shadow_consumes_commit_status_without_external_calls(tmp_path):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    job = ingest_webhook(queue, webhook_notification(delivery="shadow"))
    github = StatusGitHub(mode="shadow")
    pool = ExecutorPool(
        queue,
        Policy(trusted_orgs={"gisce"}),
        RecordingDispatcher(),
        github=github,
        config=ExecutorConfig(run_once=True),
    )

    assert pool.publish_commit_status_one(job.id) is True

    status = commit_status_row(queue, job.id)
    assert status["delivery_status"] == "succeeded"
    assert status["sha"] is None
    assert github.statuses == []
