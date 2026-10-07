import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

import github_agent_bridge.queue as queue_module
from github_agent_bridge.models import GitHubContext, Notification
from github_agent_bridge.queue import canonical_event_key
from github_agent_bridge.intent_classifier import IntentClassification
from github_agent_bridge.policy import FeedbackLearning, IntentClassifier, Policy
from github_agent_bridge.queue import JobQueue
from github_agent_bridge.sql.migrations import load_migrations

BODY1 = "@pilipilisbot one https://github.com/gisce/erp/pull/1#issuecomment-10"
BODY2 = "@pilipilisbot two https://github.com/gisce/erp/pull/1#issuecomment-11"
BODY_OTHER = "@pilipilisbot other https://github.com/gisce/erp/pull/2#issuecomment-12"


def notif(uid, mid, body):
    return Notification(uid=uid, message_id=mid, subject="Re: [gisce/erp] PR", from_addr="Edu <notifications@github.com>", body=body, auth={"spf": True, "dkim": True, "dmarc": True})


def policy():
    return Policy(trusted_orgs={"gisce"}, bot_logins={"pilipilisbot"})


def intent_policy(**kwargs):
    return Policy(
        trusted_orgs={"gisce"},
        bot_logins={"pilipilisbot"},
        intent_classifier=IntentClassifier(enabled=True, model="gpt-5.4-mini", **kwargs),
    )


@pytest.mark.parametrize("decision,status", [("deny", "denied"), ("ask", "waiting_approval")])
def test_retry_cannot_override_policy_decision(tmp_path, decision, status):
    q = JobQueue(tmp_path / "q.sqlite3")
    job, _ = q.enqueue(notif(1, "<policy@github.com>", BODY1), policy())
    with q.connect() as con:
        con.execute("UPDATE jobs SET decision=?, status=? WHERE id=?", (decision, status, job.id))

    assert q.retry(job.id, actor="admin") is False
    assert q.get(job.id).status == status
    with q.connect() as con:
        assert con.execute(
            "SELECT count(*) FROM worklog WHERE job_id=? AND phase='retry'", (job.id,)
        ).fetchone()[0] == 0


def test_retry_blocked_job_requires_executable_decision(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    job, _ = q.enqueue(notif(1, "<blocked@github.com>", BODY1), policy())
    q.finish(job.id, "blocked", "failed", "boom")

    assert q.retry(job.id, actor="admin") is True
    assert q.get(job.id).status == "pending"

    q.finish(job.id, "blocked", "failed", "boom")
    with q.connect() as con:
        con.execute("UPDATE jobs SET decision='deny' WHERE id=?", (job.id,))
    assert q.retry(job.id, actor="admin") is False
    assert q.get(job.id).status == "blocked"


def test_queue_expands_user_in_db_path(tmp_path, monkeypatch):
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.chdir(tmp_path)

    JobQueue("~/state/q.sqlite3")

    assert (home / "state" / "q.sqlite3").exists()
    assert not (tmp_path / "~").exists()


def test_executor_pause_state_round_trips_without_schema_change(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")

    assert q.executor_pause_state() == {"paused": False}
    assert q.executor_paused() is False

    q.pause_executor("upgrade window")

    paused = q.executor_pause_state()
    assert paused["paused"] is True
    assert paused["reason"] == "upgrade window"
    assert paused["updated_at"]
    assert q.executor_paused() is True

    q.resume_executor()

    assert q.executor_pause_state()["paused"] is False
    assert q.executor_paused() is False


def test_paused_queue_does_not_claim_pending_job(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    q.pause_executor("upgrade window")
    job, state = q.enqueue(notif(1, "<pause@github.com>", BODY1), policy())
    assert state == "enqueued"

    assert q.claim_next("worker") is None
    assert q.get(job.id).status == "pending"

    q.resume_executor()
    assert q.claim_next("worker").id == job.id


def test_claim_next_reads_pause_inside_claim_transaction(tmp_path, monkeypatch):
    db = tmp_path / "q.sqlite3"
    q = JobQueue(db)
    job, state = q.enqueue(notif(1, "<pause-race@github.com>", BODY1), policy())
    assert state == "enqueued"
    q.pause_executor("upgrade window")

    events = []
    original_connect = queue_module.sqlite3.connect

    class TracingConnection(queue_module.ClosingConnection):
        def execute(self, sql, parameters=(), /):
            normalized = " ".join(str(sql).split())
            if normalized == "BEGIN IMMEDIATE":
                events.append("begin_immediate")
            elif normalized.startswith("SELECT value FROM state WHERE key="):
                events.append("pause_state_read")
            return super().execute(sql, parameters)

    def tracing_connect(*args, **kwargs):
        kwargs["factory"] = TracingConnection
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(queue_module.sqlite3, "connect", tracing_connect)

    assert q.claim_next("worker") is None
    assert events[:2] == ["begin_immediate", "pause_state_read"]
    assert q.get(job.id).status == "pending"


def test_connect_recreates_missing_parent_directory(tmp_path):
    q = JobQueue(tmp_path / "missing" / "q.sqlite3")
    for child in q.path.parent.iterdir():
        child.unlink()
    q.path.parent.rmdir()

    assert q.claim_next("worker") is None
    with sqlite3.connect(q.path) as con:
        assert con.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='jobs'"
        ).fetchone()[0] == 1


def test_init_adds_quarantine_schema_to_existing_database(tmp_path):
    db = tmp_path / "q.sqlite3"
    with sqlite3.connect(db) as con:
        con.execute("CREATE TABLE state (key TEXT PRIMARY KEY, value TEXT NOT NULL)")

    JobQueue(db)

    with sqlite3.connect(db) as con:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        indexes = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
        }

    assert "quarantined_notifications" in tables
    assert "idx_quarantined_notifications_message_id" in indexes
    assert "idx_quarantined_notifications_unresolved" in indexes


def test_fresh_database_records_packaged_migrations(tmp_path):
    db = tmp_path / "q.sqlite3"

    JobQueue(db)

    with sqlite3.connect(db) as con:
        rows = con.execute(
            "SELECT version,name,checksum,applied_at FROM schema_migrations ORDER BY version"
        ).fetchall()

    packaged = load_migrations()
    assert [(row[0], row[1], row[2]) for row in rows] == [
        (migration.version, migration.name, migration.checksum)
        for migration in packaged
    ]
    assert all(row[3].endswith("Z") for row in rows)


def test_versioned_migration_adds_columns_to_existing_database(tmp_path):
    db = tmp_path / "q.sqlite3"
    q = JobQueue(db)
    with q.connect() as con:
        con.execute("ALTER TABLE jobs DROP COLUMN trigger_actor")
        con.execute("ALTER TABLE jobs DROP COLUMN trigger_actor_avatar_url")
        con.execute("DELETE FROM schema_migrations WHERE version=1")

    JobQueue(db)

    with sqlite3.connect(db) as con:
        columns = {row[1] for row in con.execute("PRAGMA table_info(jobs)")}
        applied = con.execute(
            "SELECT count(*) FROM schema_migrations WHERE version=1"
        ).fetchone()[0]

    assert {"trigger_actor", "trigger_actor_avatar_url"} <= columns
    assert applied == 1


def test_queue_expands_user_in_db_path(tmp_path, monkeypatch):
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.chdir(tmp_path)

    JobQueue("~/state/q.sqlite3")

    assert (home / "state" / "q.sqlite3").exists()
    assert not (tmp_path / "~").exists()


def test_connect_recreates_missing_parent_directory(tmp_path):
    q = JobQueue(tmp_path / "missing" / "q.sqlite3")
    for child in q.path.parent.iterdir():
        child.unlink()
    q.path.parent.rmdir()

    assert q.claim_next("worker") is None
    with sqlite3.connect(q.path) as con:
        assert con.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='jobs'"
        ).fetchone()[0] == 1


def test_enqueue_and_coalesce_same_work_key(tmp_path, monkeypatch):
    monkeypatch.setattr("github_agent_bridge.actors.github_actor_details_for_context", lambda ctx, *, gh_bin="gh": None)
    q = JobQueue(tmp_path / "q.sqlite3")
    job1, state1 = q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    job2, state2 = q.enqueue(notif(2, "<2@github.com>", BODY2), policy())
    assert state1 == "enqueued"
    assert state2 == "coalesced"
    assert job1.id == job2.id
    assert q.stats()["pending"] == 1
    contexts = q.coalesced_contexts(job1.id)
    assert len(contexts) == 1
    assert contexts[0].comment_id == 11
    assert job1.trigger_actor == "Edu"
    assert job1.trigger_actor_avatar_url == "https://github.com/Edu.png?size=80"


def test_canonical_event_key_uses_immutable_comment_id_across_sources():
    ctx = GitHubContext(
        urls=["https://github.com/gisce/erp/issues/42#issuecomment-123"],
        repo="gisce/erp",
        issue_number=42,
        comment_id=123,
        target_kind="issue",
    )

    assert canonical_event_key("reply_comment", ctx, "email", "<mail@github.com>") == (
        "issue_comment:created:gisce/erp:123"
    )
    assert canonical_event_key("reply_comment", ctx, "webhook", "delivery-1") == (
        "issue_comment:created:gisce/erp:123"
    )


def test_canonical_event_key_falls_back_to_source_receipt_when_identity_is_uncertain():
    ctx = GitHubContext(
        urls=["https://github.com/gisce/erp/issues/42"],
        repo="gisce/erp",
        issue_number=42,
        target_kind="issue",
    )

    assert canonical_event_key("mention", ctx, "email", "<mail@github.com>") == (
        "email:<mail@github.com>"
    )


def test_ingest_records_receipt_and_event_and_deduplicates_same_event(tmp_path, monkeypatch):
    monkeypatch.setattr("github_agent_bridge.actors.github_actor_details_for_context", lambda ctx, *, gh_bin="gh": None)
    q = JobQueue(tmp_path / "q.sqlite3")

    first, first_state = q.ingest(notif(1, "<first@github.com>", BODY1), policy())
    duplicate = Notification(
        uid=2,
        message_id="<second@github.com>",
        subject="Re: [gisce/erp] PR",
        from_addr="Edu <notifications@github.com>",
        body=BODY1,
        auth={"spf": True, "dkim": True, "dmarc": True},
    )
    second, second_state = q.ingest(duplicate, policy())

    assert first_state == "enqueued"
    assert second_state == "duplicate"
    assert second.id == first.id
    with q.connect() as con:
        receipts = con.execute(
            "SELECT source_key,status,job_id FROM ingest_receipts ORDER BY id"
        ).fetchall()
        events = con.execute("SELECT event_key,job_id FROM github_events").fetchall()
    assert [(row["source_key"], row["status"], row["job_id"]) for row in receipts] == [
        ("<first@github.com>", "accepted", first.id),
        ("<second@github.com>", "duplicate", first.id),
    ]
    assert len(events) == 1
    assert events[0]["event_key"] == "issue_comment:created:gisce/erp:10"
    assert events[0]["job_id"] == first.id


def test_concurrent_ingestion_creates_one_canonical_job_per_event(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "github_agent_bridge.actors.github_actor_details_for_context",
        lambda ctx, *, gh_bin="gh": None,
    )
    q = JobQueue(tmp_path / "q.sqlite3")
    barrier = Barrier(2)

    def ingest(uid, message_id, source):
        barrier.wait()
        return q.ingest(notif(uid, message_id, BODY1), policy(), source=source, source_key=message_id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda args: ingest(*args),
                [
                    (1, "<email@github.com>", "email"),
                    (2, "delivery-1", "webhook"),
                ],
            )
        )

    assert sorted(state for _, state in results) == ["duplicate", "enqueued"]
    assert len({job.id for job, _ in results}) == 1
    with q.connect() as con:
        assert con.execute("SELECT count(*) FROM jobs").fetchone()[0] == 1
        assert con.execute("SELECT count(*) FROM github_events").fetchone()[0] == 1
        assert con.execute("SELECT count(*) FROM ingest_receipts").fetchone()[0] == 2


def test_enqueue_rolls_back_job_when_acknowledgement_cannot_be_persisted(tmp_path, monkeypatch):
    q = JobQueue(tmp_path / "q.sqlite3")

    def fail_acknowledgement(*args, **kwargs):
        raise RuntimeError("acknowledgement persistence failed")

    monkeypatch.setattr(q, "_queue_acknowledgement", fail_acknowledgement)

    with pytest.raises(RuntimeError, match="acknowledgement persistence failed"):
        q.enqueue(notif(1, "<atomic-ack@github.com>", BODY1), policy())

    with q.connect() as con:
        assert con.execute("SELECT count(*) FROM jobs").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM job_acknowledgements").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM github_events").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM ingest_receipts").fetchone()[0] == 0


def test_equivalent_open_issue_notification_coalesces_after_claim(tmp_path, monkeypatch):
    monkeypatch.setattr("github_agent_bridge.actors.github_actor_details_for_context", lambda ctx, *, gh_bin="gh": None)
    q = JobQueue(tmp_path / "q.sqlite3")
    first = Notification(
        uid=1,
        message_id="<gisce/erp/issues/29307@github.com>",
        subject="[gisce/erp] Example issue (Issue #29307)",
        from_addr="polsala <notifications@github.com>",
        body="@pilipilisbot was assigned\nhttps://github.com/gisce/erp/issues/29307",
        auth={"spf": True, "dkim": True, "dmarc": True},
    )
    duplicate = Notification(
        uid=2,
        message_id="<gisce/erp/issue/29307/issue_event/32055234716@github.com>",
        subject="Re: [gisce/erp] Example issue (Issue #29307)",
        from_addr="polsala <notifications@github.com>",
        body="@pilipilisbot was assigned\nhttps://github.com/gisce/erp/issues/29307#event-32055234716",
        auth={"spf": True, "dkim": True, "dmarc": True},
    )

    job, state = q.enqueue(first, policy())
    assert state == "enqueued"
    assert q.claim_next("worker").id == job.id

    coalesced, duplicate_state = q.enqueue(duplicate, policy())

    assert duplicate_state == "coalesced"
    assert coalesced.id == job.id
    assert q.stats().get("pending", 0) == 0
    stored = q.get(job.id)
    assert stored.coalesced_count == 1
    with q.connect() as con:
        row = con.execute(
            "SELECT message_id FROM coalesced_notifications WHERE job_id=?",
            (job.id,),
        ).fetchone()
    assert row["message_id"] == duplicate.message_id


def test_distinct_comment_remains_pending_while_same_work_key_is_running(tmp_path, monkeypatch):
    monkeypatch.setattr("github_agent_bridge.actors.github_actor_details_for_context", lambda ctx, *, gh_bin="gh": None)
    q = JobQueue(tmp_path / "q.sqlite3")
    running, _ = q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    assert q.claim_next("worker").id == running.id

    followup, state = q.enqueue(notif(2, "<2@github.com>", BODY2), policy())

    assert state == "enqueued"
    assert followup.id != running.id
    assert followup.status == "pending"
    assert q.get(running.id).coalesced_count == 0


def test_enqueue_stores_trigger_actor_and_coalesced_actor(tmp_path, monkeypatch):
    monkeypatch.setattr("github_agent_bridge.actors.github_actor_details_for_context", lambda ctx, *, gh_bin="gh": None)
    q = JobQueue(tmp_path / "q.sqlite3")
    job, state = q.enqueue(Notification(uid=1, message_id="<1@github.com>", subject="Re: [gisce/erp] PR", from_addr="ecarreras <notifications@github.com>", body=BODY1, auth={"spf": True, "dkim": True, "dmarc": True}), policy())
    q.enqueue(Notification(uid=2, message_id="<2@github.com>", subject="Re: [gisce/erp] PR", from_addr="marc <notifications@github.com>", body=BODY2, auth={"spf": True, "dkim": True, "dmarc": True}), policy())

    assert state == "enqueued"
    assert job.trigger_actor == "ecarreras"
    assert job.trigger_actor_avatar_url == "https://github.com/ecarreras.png?size=80"
    with q.connect() as con:
        row = con.execute("SELECT trigger_actor, trigger_actor_avatar_url FROM coalesced_notifications WHERE job_id=?", (job.id,)).fetchone()
    assert row["trigger_actor"] == "marc"
    assert row["trigger_actor_avatar_url"] == "https://github.com/marc.png?size=80"


def test_enqueue_prefers_context_actor_over_notification_sender(tmp_path, monkeypatch):
    calls = []

    def fake_actor(ctx, *, gh_bin="gh"):
        calls.append((ctx.repo, ctx.issue_number, ctx.comment_id, gh_bin))
        from github_agent_bridge.actors import TriggerActor

        return TriggerActor(login="ecarreras", avatar_url="https://avatars.githubusercontent.com/u/294235?v=4", user_id=294235)

    monkeypatch.setattr("github_agent_bridge.actors.github_actor_details_for_context", fake_actor)
    q = JobQueue(tmp_path / "q.sqlite3")

    job, state = q.enqueue(
        Notification(
            uid=1,
            message_id="<1@github.com>",
            subject="Re: [gisce/erp] PR",
            from_addr="GitHub <notifications@github.com>",
            body="https://github.com/gisce/erp/pull/1#issuecomment-99",
            auth={"spf": True, "dkim": True, "dmarc": True},
        ),
        policy(),
    )

    assert state == "enqueued"
    assert calls == [("gisce/erp", 1, 99, "gh")]
    assert job.trigger_actor == "ecarreras"
    assert job.trigger_actor_avatar_url == "https://avatars.githubusercontent.com/u/294235?v=4"
    assert job.metadata["trigger_actor_id"] == 294235


def test_enqueue_accepts_github_app_bot_actor_from_context(tmp_path, monkeypatch):
    def fake_actor(ctx, *, gh_bin="gh"):
        from github_agent_bridge.actors import TriggerActor

        return TriggerActor(
            login="copilot-pull-request-reviewer[bot]",
            avatar_url="https://avatars.githubusercontent.com/in/946600?v=4",
        )

    monkeypatch.setattr("github_agent_bridge.actors.github_actor_details_for_context", fake_actor)
    q = JobQueue(tmp_path / "q.sqlite3")

    job, state = q.enqueue(
        Notification(
            uid=1,
            message_id="<1@github.com>",
            subject="Re: [gisce/erp] PR",
            from_addr="GitHub <notifications@github.com>",
            body="https://github.com/gisce/erp/pull/1#pullrequestreview-99",
            auth={"spf": True, "dkim": True, "dmarc": True},
        ),
        policy(),
    )

    assert state == "enqueued"
    assert job.trigger_actor == "copilot-pull-request-reviewer[bot]"
    assert job.trigger_actor_avatar_url == "https://avatars.githubusercontent.com/in/946600?v=4"


def test_enqueue_falls_back_to_context_actor_for_generic_github_sender(tmp_path, monkeypatch):
    calls = []

    def fake_actor(ctx, *, gh_bin="gh"):
        calls.append((ctx.repo, ctx.issue_number, ctx.comment_id, gh_bin))
        from github_agent_bridge.actors import TriggerActor

        return TriggerActor(login="ecarreras", avatar_url="https://avatars.githubusercontent.com/u/294235?v=4")

    monkeypatch.setattr("github_agent_bridge.actors.github_actor_details_for_context", fake_actor)
    q = JobQueue(tmp_path / "q.sqlite3")

    job, state = q.enqueue(
        Notification(
            uid=1,
            message_id="<1@github.com>",
            subject="Re: [gisce/erp] issue",
            from_addr="GitHub <notifications@github.com>",
            body="https://github.com/gisce/erp/issues/1#issuecomment-99",
            auth={"spf": True, "dkim": True, "dmarc": True},
        ),
        policy(),
    )

    assert state == "enqueued"
    assert calls == [("gisce/erp", 1, 99, "gh")]
    assert job.trigger_actor == "ecarreras"
    assert job.trigger_actor_avatar_url == "https://avatars.githubusercontent.com/u/294235?v=4"


def test_enqueue_falls_back_to_notification_sender_when_context_lookup_fails(tmp_path, monkeypatch):
    monkeypatch.setattr("github_agent_bridge.actors.github_actor_details_for_context", lambda ctx, *, gh_bin="gh": None)
    q = JobQueue(tmp_path / "q.sqlite3")

    job, state = q.enqueue(
        Notification(
            uid=1,
            message_id="<1@github.com>",
            subject="Re: [gisce/erp] issue",
            from_addr="ecarreras <notifications@github.com>",
            body="https://github.com/gisce/erp/issues/1#issuecomment-99",
            auth={"spf": True, "dkim": True, "dmarc": True},
        ),
        policy(),
    )

    assert state == "enqueued"
    assert job.trigger_actor == "ecarreras"
    assert job.trigger_actor_avatar_url == "https://github.com/ecarreras.png?size=80"


def test_enqueue_leaves_actor_null_when_context_lookup_fails_for_generic_sender(tmp_path, monkeypatch):
    monkeypatch.setattr("github_agent_bridge.actors.github_actor_details_for_context", lambda ctx, *, gh_bin="gh": None)
    q = JobQueue(tmp_path / "q.sqlite3")

    job, state = q.enqueue(
        Notification(
            uid=1,
            message_id="<1@github.com>",
            subject="Re: [gisce/erp] issue",
            from_addr="GitHub <notifications@github.com>",
            body="https://github.com/gisce/erp/issues/1#issuecomment-99",
            auth={"spf": True, "dkim": True, "dmarc": True},
        ),
        policy(),
    )

    assert state == "enqueued"
    assert job.trigger_actor is None
    assert job.trigger_actor_avatar_url is None


def test_claim_parallel_different_work_keys_but_not_same(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    q.enqueue(notif(2, "<2@github.com>", BODY2), policy())
    q.enqueue(notif(3, "<3@github.com>", BODY_OTHER), policy())
    j1 = q.claim_next("w1")
    j2 = q.claim_next("w2")
    assert {j1.work_key, j2.work_key} == {"gisce/erp#1", "gisce/erp#2"}
    assert q.claim_next("w3") is None


def test_concurrent_workers_cannot_claim_the_same_job(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    job, _ = q.enqueue(notif(1, "<claim-race@github.com>", BODY1), policy())
    barrier = Barrier(2)

    def claim(worker_id):
        barrier.wait()
        return q.claim_next(worker_id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        claims = list(executor.map(claim, ["worker-1", "worker-2"]))

    claimed = [candidate for candidate in claims if candidate is not None]
    assert [candidate.id for candidate in claimed] == [job.id]
    assert q.get(job.id).attempts == 1


def test_claim_rolls_back_job_and_run_when_audit_write_fails(tmp_path, monkeypatch):
    q = JobQueue(tmp_path / "q.sqlite3")
    job, _ = q.enqueue(notif(1, "<atomic-run@github.com>", BODY1), policy())

    def fail_audit(*args, **kwargs):
        raise RuntimeError("audit persistence failed")

    monkeypatch.setattr(q, "_log", fail_audit)

    with pytest.raises(RuntimeError, match="audit persistence failed"):
        q.claim_next("worker")

    stored = q.get(job.id)
    assert stored.status == "pending"
    assert stored.attempts == 0
    with q.connect() as con:
        assert con.execute("SELECT count(*) FROM job_runs WHERE job_id=?", (job.id,)).fetchone()[0] == 0


def test_worker_heartbeat_upserts_liveness_and_active_job(tmp_path):
    q = JobQueue(tmp_path / "bridge.sqlite3")
    job, _ = q.enqueue(notif(1, "<heartbeat@github.com>", BODY1), policy())

    q.record_worker_heartbeat("executor-1/worker-0", "executor-1", 123, "idle")
    q.record_worker_heartbeat("executor-1/worker-0", "executor-1", 123, "running", job.id, 2)

    with q.connect() as con:
        row = con.execute("SELECT * FROM worker_heartbeats").fetchone()
    assert row["worker_id"] == "executor-1/worker-0"
    assert row["active_job_id"] == job.id
    assert row["loop_state"] == "running"
    assert row["recent_error_count"] == 2


def test_claim_can_filter_by_work_intent(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    work_job, _ = q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    review_job, _ = q.enqueue(notif(3, "<3@github.com>", BODY_OTHER), policy())
    q.update_work_intent(work_job.id, "work_allowed", "explicit implementation request")
    q.update_work_intent(review_job.id, "review_only", "review-only request")

    claimed = q.claim_next("review-worker", {"review_only"})

    assert claimed.id == review_job.id
    assert claimed.work_intent == "review_only"


def test_submit_review_intent_cannot_be_elevated_after_enqueue(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    job, _ = q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    with q.connect() as con:
        con.execute(
            "UPDATE jobs SET action='submit_review', work_intent='review_only' WHERE id=?",
            (job.id,),
        )

    updated = q.update_work_intent(job.id, "work_allowed", "classifier requested write access")

    assert updated.work_intent == "review_only"


def test_job_runs_preserve_each_attempt_across_requeue(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    job, _ = q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    q.update_work_intent(job.id, "work_allowed", "implementation request")

    first = q.claim_next("worker-1")
    assert first is not None
    assert q.requeue_running(first.id, "transient failure") is True
    second = q.claim_next("worker-2")
    assert second is not None
    q.finish(second.id, "done", "completed")

    with q.connect() as con:
        runs = con.execute(
            "SELECT * FROM job_runs WHERE job_id=? ORDER BY attempt",
            (job.id,),
        ).fetchall()

    assert [run["attempt"] for run in runs] == [1, 2]
    assert [run["result"] for run in runs] == ["requeued", "done"]
    assert [run["worker_id"] for run in runs] == ["worker-1", "worker-2"]
    assert all(run["started_at"] for run in runs)
    assert all(run["finished_at"] for run in runs)
    assert [run["session_id"] for run in runs] == [
        f"github-agent-bridge-job-{job.id}-attempt-1",
        f"github-agent-bridge-job-{job.id}-attempt-2",
    ]


def test_block_and_cancel_close_active_job_runs(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    blocked, _ = q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    cancelled, _ = q.enqueue(notif(2, "<2@github.com>", BODY_OTHER), policy())

    assert q.claim_next("worker-1").id == blocked.id
    assert q.block_running("executor stopped", "shutdown", job_ids=[blocked.id]) == [blocked.id]
    assert q.claim_next("worker-2").id == cancelled.id
    assert q.mark_cancelled(cancelled.id, actor="ecarreras", reason="obsolete") is not None

    with q.connect() as con:
        results = dict(
            con.execute(
                "SELECT job_id, result FROM job_runs WHERE job_id IN (?, ?)",
                (blocked.id, cancelled.id),
            ).fetchall()
        )

    assert results == {blocked.id: "blocked", cancelled.id: "cancelled"}


def test_init_backfills_only_the_known_legacy_interval_as_estimated(tmp_path):
    db = tmp_path / "q.sqlite3"
    q = JobQueue(db)
    job, _ = q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    with q.connect() as con:
        con.execute("DROP TABLE job_runs")
        con.execute("DELETE FROM schema_migrations WHERE version=1")
        con.execute(
            """UPDATE jobs
            SET attempts=3, started_at=?, finished_at=?, metadata_json=?
            WHERE id=?""",
            (
                "2026-09-01T10:00:00Z",
                "2026-09-01T10:30:00Z",
                '{"openclaw_session_id":"legacy-session"}',
                job.id,
            ),
        )

    JobQueue(db)
    JobQueue(db)

    with sqlite3.connect(db) as con:
        con.row_factory = sqlite3.Row
        runs = con.execute("SELECT * FROM job_runs WHERE job_id=?", (job.id,)).fetchall()

    assert len(runs) == 1
    assert runs[0]["attempt"] == 3
    assert runs[0]["result"] == "historical"
    assert runs[0]["session_id"] == "legacy-session"
    assert runs[0]["is_estimated"] == 1


def test_cancel_running_records_actor_reason_and_finish_preserves_cancellation(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    job, _ = q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    claimed = q.claim_next("worker")
    assert claimed is not None

    requested = q.request_cancel_running(claimed.id, actor="ecarreras", reason="stale request")
    assert requested is not None
    assert requested.metadata["cancellation"]["state"] == "requested"

    cancelled = q.mark_cancelled(claimed.id, actor="ecarreras", reason="stale request", signal_detail="sent SIGTERM", followup_url="https://github.com/gisce/erp/issues/1#issuecomment-2")
    assert cancelled is not None
    assert cancelled.status == "done"
    assert cancelled.metadata["cancellation"]["state"] == "cancelled"
    assert cancelled.metadata["cancellation"]["actor"] == "ecarreras"
    assert cancelled.last_error is None

    q.finish(claimed.id, "done", "late dispatch completion", "ok")
    stored = q.get(claimed.id)
    assert stored is not None
    assert stored.status == "done"
    assert stored.last_error is None
    assert stored.metadata["cancellation"]["reason"] == "stale request"


def test_mark_cancelled_handles_finish_after_cancel_request(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    job, _ = q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    claimed = q.claim_next("worker")
    assert claimed is not None

    requested = q.request_cancel_running(claimed.id, actor="ecarreras", reason="stale request")
    assert requested is not None

    q.finish(claimed.id, "done", "late dispatch completion", "ok")

    cancelled = q.mark_cancelled(
        claimed.id,
        actor="ecarreras",
        reason="stale request",
        signal_detail="runtime already exited",
        followup_url="https://github.com/gisce/erp/issues/1#issuecomment-2",
    )

    assert cancelled is not None
    assert cancelled.status == "done"
    assert cancelled.metadata["cancellation"]["state"] == "cancelled"
    assert cancelled.metadata["cancellation"]["signal_detail"] == "runtime already exited"
    assert cancelled.last_error is None
    assert cancelled.metadata["cancellation"]["followup_url"] == "https://github.com/gisce/erp/issues/1#issuecomment-2"


def test_claim_fresh_review_retry_records_attempt_session_id(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    job, _ = q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    q.update_work_intent(job.id, "review_only", "review-only request")

    first = q.claim_next("review-worker")
    assert first.metadata["openclaw_session_id"] == f"github-agent-bridge-job-{job.id}"
    assert q.requeue_running(job.id, "compaction failed", fresh_session=True) is True

    retry = q.claim_next("review-worker")

    assert retry.attempts == 2
    assert retry.metadata["openclaw_session_id"] == f"github-agent-bridge-job-{job.id}-attempt-2"


def test_claim_filter_preserves_running_work_key_guard(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    q.enqueue(notif(1, "<1@github.com>", BODY1), policy())

    running = q.claim_next("all-worker")
    followup_job, state = q.enqueue(notif(2, "<2@github.com>", BODY2), policy())

    assert running.work_key == "gisce/erp#1"
    assert state == "enqueued"
    assert followup_job.work_key == running.work_key
    assert q.claim_next("review-worker", {"review_only"}) is None
    assert q.claim_next("review-worker", set()) is None


def test_enqueue_does_not_coalesce_into_running_job(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    job1, state1 = q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    running = q.claim_next("worker")
    job2, state2 = q.enqueue(notif(2, "<2@github.com>", BODY2), policy())

    assert state1 == "enqueued"
    assert running.id == job1.id
    assert running.status == "running"
    assert state2 == "enqueued"
    assert job2.id != job1.id


def test_enqueue_captures_feedback_for_actionable_jobs(tmp_path, monkeypatch):
    captured = []

    def fake_capture(db_path, n, ctx, action, decision, work_intent, **kwargs):
        captured.append((db_path.name, n.message_id, ctx.work_key, action, decision, work_intent, kwargs))
        return True

    monkeypatch.setattr("github_agent_bridge.feedback.capture_feedback", fake_capture)

    q = JobQueue(tmp_path / "q.sqlite3")
    q.enqueue(notif(1, "<1@github.com>", BODY1), policy())

    assert captured == [
        (
            "q.sqlite3",
            "<1@github.com>",
            "gisce/erp#1",
            "reply_comment",
            "auto_trusted",
            "review_only",
            {"trigger_actor": "Edu", "trigger_actor_avatar_url": "https://github.com/Edu.png?size=80"},
        )
    ]


def test_enqueue_can_apply_enabled_llm_intent_classifier(tmp_path, monkeypatch):
    calls = []

    def fake_classify(n, ctx, parser_result, cfg, **kwargs):
        calls.append((parser_result.action, parser_result.work_intent, cfg.model, kwargs))
        return IntentClassification(
            action="reply_comment",
            work_intent="work_allowed",
            confidence=0.91,
            reason="User asks to create a test.",
            applied=True,
            addressed_to_agent=True,
            write_permission="state_change_allowed",
            scope="Create a test.",
        )

    monkeypatch.setattr("github_agent_bridge.queue.classify_notification_with_llm", fake_classify)
    q = JobQueue(tmp_path / "q.sqlite3")

    job, state = q.enqueue(
        notif(
            1,
            "<1@github.com>",
            "@pilipilisbot crea un test https://github.com/gisce/erp/pull/1#issuecomment-10",
        ),
        intent_policy(),
    )

    assert state == "enqueued"
    assert job.action == "reply_comment"
    assert job.work_intent == "work_allowed"
    assert calls[0][0:3] == ("reply_comment", "review_only", "gpt-5.4-mini")
    assert job.metadata["intent_classifier"]["llm"]["applied"] is True
    assert job.metadata["intent_classifier"]["parser"] == {"action": "reply_comment", "work_intent": "review_only"}


def test_enqueue_can_apply_llm_intent_classifier_to_review_comments(tmp_path, monkeypatch):
    calls = []

    def fake_classify(n, ctx, parser_result, cfg, **kwargs):
        calls.append(ctx.target_kind)
        return IntentClassification(
            action="reply_comment",
            work_intent="work_allowed",
            confidence=0.91,
            reason="User asks for an implementation change from a review comment.",
            applied=True,
            addressed_to_agent=True,
            write_permission="state_change_allowed",
            scope="Create a test.",
        )

    monkeypatch.setattr("github_agent_bridge.queue.classify_notification_with_llm", fake_classify)
    q = JobQueue(tmp_path / "q.sqlite3")

    job, state = q.enqueue(
        notif(
            1,
            "<1@github.com>",
            "@pilipilisbot crea un test https://github.com/gisce/erp/pull/1#discussion_r3195891007",
        ),
        intent_policy(),
    )

    assert state == "enqueued"
    assert calls == ["review_comment"]
    assert job.work_intent == "work_allowed"


def test_enqueue_falls_back_when_llm_intent_confidence_is_low(tmp_path, monkeypatch):
    def fake_classify(n, ctx, parser_result, cfg, **kwargs):
        return IntentClassification(
            action="reply_comment",
            work_intent="work_allowed",
            confidence=0.4,
            reason="Unsure.",
            applied=False,
            addressed_to_agent=True,
            write_permission="state_change_allowed",
        )

    monkeypatch.setattr("github_agent_bridge.queue.classify_notification_with_llm", fake_classify)
    q = JobQueue(tmp_path / "q.sqlite3")

    job, state = q.enqueue(
        notif(
            1,
            "<1@github.com>",
            "@pilipilisbot crea un test https://github.com/gisce/erp/pull/1#issuecomment-10",
        ),
        intent_policy(min_confidence=0.75),
    )

    assert state == "enqueued"
    assert job.action == "reply_comment"
    assert job.work_intent == "review_only"
    assert job.metadata["intent_classifier"]["llm"]["applied"] is False


def test_enqueue_falls_back_when_llm_intent_classifier_errors(tmp_path, monkeypatch):
    def fake_classify(n, ctx, parser_result, cfg, **kwargs):
        raise RuntimeError("classifier unavailable")

    monkeypatch.setattr("github_agent_bridge.queue.classify_notification_with_llm", fake_classify)
    q = JobQueue(tmp_path / "q.sqlite3")

    job, state = q.enqueue(
        notif(
            1,
            "<1@github.com>",
            "@pilipilisbot crea un test https://github.com/gisce/erp/pull/1#issuecomment-10",
        ),
        intent_policy(),
    )

    assert state == "enqueued"
    assert job.action == "reply_comment"
    assert job.work_intent == "review_only"
    assert "classifier unavailable" in job.metadata["intent_classifier"]["error"]


def test_enqueue_skips_llm_intent_classifier_when_disabled(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr("github_agent_bridge.queue.classify_notification_with_llm", lambda *args, **kwargs: calls.append(args) or None)
    q = JobQueue(tmp_path / "q.sqlite3")

    q.enqueue(
        notif(
            1,
            "<1@github.com>",
            "@pilipilisbot crea un test https://github.com/gisce/erp/pull/1#issuecomment-10",
        ),
        policy(),
    )

    assert calls == []


def test_enqueue_workflow_run_failed_notification(tmp_path):
    body = "Run failed: https://github.com/gisce/erp/actions/runs/26325244472"
    n = Notification(uid=1, message_id="<run@github.com>", subject="[gisce/erp] Run failed: tests - main", from_addr="Edu <notifications@github.com>", body=body, auth={"spf": True, "dkim": True, "dmarc": True})
    q = JobQueue(tmp_path / "q.sqlite3")

    job, state = q.enqueue(n, policy())

    assert state == "enqueued"
    assert job is not None
    assert job.action == "workflow_run_failed"
    assert job.work_intent == "work_allowed"
    assert job.work_key == "gisce/erp/actions/runs/26325244472"
    assert job.context.target_kind == "workflow_run"


def test_duplicate_enqueue_does_not_recapture_feedback(tmp_path, monkeypatch):
    captured = []
    monkeypatch.setattr("github_agent_bridge.feedback.capture_feedback", lambda *args, **kwargs: captured.append((args, kwargs)) or True)

    q = JobQueue(tmp_path / "q.sqlite3")
    q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    q.enqueue(notif(1, "<1@github.com>", BODY1), policy())

    assert len(captured) == 1


def test_enqueue_skips_feedback_when_policy_disables_it(tmp_path, monkeypatch):
    captured = []
    monkeypatch.setattr("github_agent_bridge.feedback.capture_feedback", lambda *args, **kwargs: captured.append((args, kwargs)) or True)

    q = JobQueue(tmp_path / "q.sqlite3")
    q.enqueue(notif(1, "<1@github.com>", BODY1), Policy(trusted_orgs={"gisce"}, feedback_learning=FeedbackLearning(enabled=False)))

    assert captured == []


def test_dismiss_blocked_job_marks_done(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    job, _ = q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    q.finish(job.id, "blocked", "boom", "details")
    with q.connect() as con:
        finished_at = con.execute("SELECT finished_at FROM jobs WHERE id=?", (job.id,)).fetchone()["finished_at"]

    assert q.dismiss(job.id, "already answered") is True
    stored = q.get(job.id)
    assert stored is not None
    assert stored.status == "done"
    assert stored.last_error is None
    with q.connect() as con:
        assert con.execute("SELECT finished_at FROM jobs WHERE id=?", (job.id,)).fetchone()["finished_at"] == finished_at


def test_unlock_stale_can_limit_to_selected_running_jobs(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    job1, _ = q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    q.claim_next("worker")
    job2, _ = q.enqueue(notif(2, "<2@github.com>", BODY_OTHER), policy())
    q.claim_next("worker")

    with q.connect() as con:
        con.execute("UPDATE jobs SET started_at='2000-01-01T00:00:00Z', updated_at='2000-01-01T00:00:00Z'")

    assert q.unlock_stale(older_than_seconds=1, job_ids=[job2.id]) == 1

    assert q.get(job1.id).status == "running"
    assert q.get(job2.id).status == "pending"
    with q.connect() as con:
        assert con.execute(
            "SELECT result FROM job_runs WHERE job_id=?",
            (job2.id,),
        ).fetchone()["result"] == "requeued"


def test_block_running_can_limit_jobs_and_never_requeues(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    job1, _ = q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    q.claim_next("old-worker-1")
    job2, _ = q.enqueue(notif(2, "<2@github.com>", BODY_OTHER), policy())
    q.claim_next("old-worker-2")

    blocked = q.block_running(
        "executor stopped",
        "process no longer exists; manual retry required",
        job_ids=[job2.id],
    )

    assert blocked == [job2.id]
    assert q.get(job1.id).status == "running"
    stored = q.get(job2.id)
    assert stored.status == "blocked"
    assert stored.locked_by is None
    assert stored.last_error == "process no longer exists; manual retry required"


def test_runtime_process_is_bound_to_running_job_worker(tmp_path):
    q = JobQueue(tmp_path / "q.sqlite3")
    queued, _ = q.enqueue(notif(1, "<1@github.com>", BODY1), policy())
    worker_id = "executor-123-deadbeef/worker-0"
    claimed = q.claim_next(worker_id)
    assert claimed is not None

    assert q.register_runtime_process(
        claimed.id,
        worker_id,
        "executor-123-deadbeef",
        {"pid": 456, "ppid": 123, "pgid": 456, "sid": 456, "start_time_ticks": 999},
    ) is True
    assert q.register_runtime_process(
        claimed.id,
        "another-worker",
        "executor-123-deadbeef",
        {"pid": 789, "ppid": 123, "pgid": 789, "sid": 789, "start_time_ticks": 1000},
    ) is False

    runtime = q.get(queued.id).metadata["runtime_process"]
    assert runtime["state"] == "running"
    assert runtime["pid"] == 456
    assert runtime["worker_id"] == worker_id

    assert q.mark_runtime_process_exited(claimed.id, worker_id) is True
    runtime = q.get(queued.id).metadata["runtime_process"]
    assert runtime["state"] == "exited"
    assert runtime["exited_at"].endswith("Z")


def test_context_manager_closes_connection(tmp_path):
    queue = JobQueue(tmp_path / "queue.sqlite3")

    with queue.connect() as con:
        assert con.execute("SELECT 1").fetchone()[0] == 1

    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        con.execute("SELECT 1")
