import json

from github_agent_bridge.persistence import (
    AcknowledgementRepository,
    ExecutorPauseState,
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
