import hashlib
import json

from github_agent_bridge.persistence import (
    AcknowledgementRepository,
    ExecutorPauseState,
    IngestionRepository,
    IngestionRequest,
    JobRepository,
    RuntimeProcess,
    RuntimeRepository,
    StateRepository,
)
from github_agent_bridge.models import GitHubContext, Notification
from github_agent_bridge.policy import Policy
from github_agent_bridge.queue import JobQueue


def notification() -> Notification:
    return Notification(
        uid=1,
        message_id="<repository-ack@github.com>",
        subject="Re: [gisce/erp] PR",
        from_addr="Edu <notifications@github.com>",
        body=(
            "@pilipilisbot one "
            "https://github.com/gisce/erp/pull/1#issuecomment-10"
        ),
        auth={"spf": True, "dkim": True, "dmarc": True},
    )


def test_acknowledgement_repository_maps_claim_and_records_result(tmp_path):
    queue = JobQueue(tmp_path / "queue.sqlite3")
    job, status = queue.enqueue(
        notification(),
        Policy(trusted_orgs={"gisce"}, bot_logins={"pilipilisbot"}),
    )
    assert status == "enqueued"
    repository = AcknowledgementRepository(queue.database)

    claim = repository.claim(job.id)

    assert claim is not None
    assert claim.job_id == job.id
    assert claim.context.comment_id == 10
    assert repository.all_succeeded(job.id) is False

    repository.finish(claim.id, True)

    assert repository.all_succeeded(job.id) is True


def test_state_repository_maps_executor_pause_state(tmp_path):
    queue = JobQueue(tmp_path / "queue.sqlite3")
    repository = StateRepository(queue.database)

    repository.set("custom", "value")
    repository.pause_executor("maintenance")

    assert repository.get("custom") == "value"
    pause_state = repository.executor_pause_state()
    assert isinstance(pause_state, ExecutorPauseState)
    assert pause_state.paused is True
    assert pause_state.reason == "maintenance"
    assert pause_state.updated_at

    repository.set("executor_paused", json.dumps(["invalid shape"]))

    assert repository.executor_pause_state() == ExecutorPauseState(
        paused=False,
        error="invalid_executor_pause_state",
    )


def test_runtime_repository_persists_process_heartbeat_and_events(tmp_path):
    queue = JobQueue(tmp_path / "queue.sqlite3")
    job, status = queue.enqueue(
        notification(),
        Policy(trusted_orgs={"gisce"}, bot_logins={"pilipilisbot"}),
    )
    assert status == "enqueued"
    assert queue.claim_next("worker-1") is not None
    repository = RuntimeRepository(queue.database)
    process = RuntimeProcess.from_identity(
        "executor-1",
        "worker-1",
        {"pid": 101, "ppid": 100, "pgid": 101, "sid": 99, "start_time_ticks": 1234},
    )

    repository.record_worker_heartbeat(
        "worker-1", "executor-1", 101, "running", job.id, 2
    )
    assert repository.register_process(job.id, process) is True
    assert repository.add_job_session_event(
        job.id,
        "openclaw_stdout",
        "runtime output",
        "line one",
        progress_kind="visible",
    ) is True
    assert repository.add_job_worklog(
        job.id, "runtime_test", "repository event", None
    ) is True
    assert repository.mark_process_exited(job.id, "worker-1") is True

    with queue.database.read_only() as con:
        heartbeat = con.execute(
            "SELECT * FROM worker_heartbeats WHERE worker_id='worker-1'"
        ).fetchone()
        event = con.execute(
            "SELECT * FROM job_session_events WHERE job_id=? AND event_type='openclaw_stdout'",
            (job.id,),
        ).fetchone()
        progress = con.execute(
            "SELECT * FROM job_progress WHERE job_id=? AND phase='openclaw_stdout'",
            (job.id,),
        ).fetchone()
        worklog = con.execute(
            "SELECT * FROM worklog WHERE job_id=? AND phase='runtime_test'",
            (job.id,),
        ).fetchone()

    assert heartbeat["executor_id"] == "executor-1"
    assert heartbeat["active_job_id"] == job.id
    assert heartbeat["recent_error_count"] == 2
    assert event["summary"] == "runtime output"
    assert progress["kind"] == "visible"
    assert worklog["summary"] == "repository event"
    runtime_process = queue.get(job.id).metadata["runtime_process"]
    assert runtime_process["pid"] == 101
    assert runtime_process["state"] == "exited"


def test_job_repository_maps_and_transitions_jobs(tmp_path):
    queue = JobQueue(tmp_path / "queue.sqlite3")
    job, status = queue.enqueue(
        notification(),
        Policy(trusted_orgs={"gisce"}, bot_logins={"pilipilisbot"}),
    )
    assert status == "enqueued"
    assert isinstance(queue.jobs, JobRepository)

    claimed = queue.jobs.claim_next("repository-worker")

    assert claimed is not None
    assert claimed.id == job.id
    assert claimed.status == "running"
    assert claimed.locked_by == "repository-worker"
    queue.jobs.finish(job.id, "done", "repository lifecycle complete")
    assert queue.jobs.get(job.id).status == "done"


def test_ingestion_repository_deduplicates_receipts_in_one_transaction(tmp_path):
    queue = JobQueue(tmp_path / "queue.sqlite3")
    assert isinstance(queue.ingestion, IngestionRepository)
    item = notification()
    context = GitHubContext(
        urls=["https://github.com/gisce/erp/pull/1#issuecomment-10"],
        repo="gisce/erp",
        issue_number=1,
        comment_id=10,
        target_kind="issue_comment",
    )
    request = IngestionRequest(
        notification=item,
        context=context,
        source="email",
        source_key=item.message_id,
        event_key="issue_comment:created:gisce/erp:10",
        payload_hash=hashlib.sha256(item.body.encode("utf-8")).hexdigest(),
        status="pending",
        action="reply_comment",
        decision="auto_trusted",
        work_intent="work_allowed",
        metadata={"received_at": item.received_at},
    )

    first = queue.ingestion.ingest(request)
    duplicate = queue.ingestion.ingest(request)

    assert first.state == "enqueued"
    assert first.job is not None
    assert duplicate.state == "duplicate"
    assert duplicate.job is not None
    assert duplicate.job.id == first.job.id
    with queue.database.read_only() as con:
        assert con.execute("SELECT count(*) FROM jobs").fetchone()[0] == 1
        assert con.execute("SELECT count(*) FROM ingest_receipts").fetchone()[0] == 1
