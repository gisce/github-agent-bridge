import json

from github_agent_bridge.persistence import (
    AcknowledgementRepository,
    ExecutorPauseState,
    RuntimeProcess,
    RuntimeRepository,
    StateRepository,
)
from github_agent_bridge.models import Notification
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
