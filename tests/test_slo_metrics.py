from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from github_agent_bridge.backend import DashboardConfig, create_app
from github_agent_bridge.dashboard_data import metrics_summary
from github_agent_bridge.queue import JobQueue


NOW = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)


def insert_job(
    con,
    number: int,
    *,
    source_received_at: str | None,
    created_at: str,
    finished_at: str,
    terminal_outcome: str | None,
    decision: str = "auto_trusted",
) -> int:
    status = "blocked" if terminal_outcome == "blocked" else "done"
    cursor = con.execute(
        """INSERT INTO jobs(
            work_key,repo,thread,status,action,decision,work_intent,subject,
            message_id,source_received_at,context_json,metadata_json,created_at,
            updated_at,finished_at,terminal_outcome
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            f"gisce/example#{number}",
            "gisce/example",
            number,
            status,
            "reply_comment",
            decision,
            "work_allowed",
            f"job {number}",
            f"<slo-{number}@github.com>",
            source_received_at,
            "{}",
            "{}",
            created_at,
            finished_at,
            finished_at,
            terminal_outcome,
        ),
    )
    return int(cursor.lastrowid)


def insert_run(
    con,
    job_id: int,
    attempt: int,
    started_at: str,
    finished_at: str,
    result: str,
) -> None:
    con.execute(
        """INSERT INTO job_runs(
            job_id,attempt,started_at,finished_at,result,worker_id,session_id
        ) VALUES(?,?,?,?,?,?,?)""",
        (
            job_id,
            attempt,
            started_at,
            finished_at,
            result,
            "worker-1",
            f"slo-{job_id}-{attempt}",
        ),
    )


def test_slo_summary_uses_terminal_and_ingestion_cohorts(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    queue = JobQueue(db)
    with queue.connect() as con:
        completed = insert_job(
            con,
            1,
            source_received_at="2026-10-13T09:00:00Z",
            created_at="2026-10-13T09:01:00Z",
            finished_at="2026-10-13T10:00:00Z",
            terminal_outcome="completed",
        )
        no_op = insert_job(
            con,
            2,
            source_received_at=None,
            created_at="2026-10-12T11:00:00Z",
            finished_at="2026-10-12T12:00:00Z",
            terminal_outcome="no_op",
        )
        blocked = insert_job(
            con,
            3,
            source_received_at="2026-10-11T10:05:00Z",
            created_at="2026-10-11T10:00:00Z",
            finished_at="2026-10-11T11:00:00Z",
            terminal_outcome="blocked",
        )
        insert_job(
            con,
            4,
            source_received_at="2026-10-10T09:00:00Z",
            created_at="2026-10-10T09:01:00Z",
            finished_at="2026-10-10T09:30:00Z",
            terminal_outcome="cancelled",
        )
        insert_job(
            con,
            5,
            source_received_at=None,
            created_at="2026-10-09T09:00:00Z",
            finished_at="2026-10-09T09:30:00Z",
            terminal_outcome=None,
        )
        boundary = insert_job(
            con,
            6,
            source_received_at="2026-10-06T11:00:00Z",
            created_at="2026-10-06T11:05:00Z",
            finished_at="2026-10-06T12:00:00Z",
            terminal_outcome="completed",
        )
        insert_job(
            con,
            7,
            source_received_at="2026-10-06T10:00:00Z",
            created_at="2026-10-06T10:05:00Z",
            finished_at="2026-10-06T11:59:59Z",
            terminal_outcome="completed",
        )
        insert_job(
            con,
            8,
            source_received_at="2026-10-13T08:00:00Z",
            created_at="2026-10-13T08:01:00Z",
            finished_at="2026-10-13T08:01:00Z",
            terminal_outcome="denied",
            decision="denied",
        )

        insert_run(
            con,
            completed,
            1,
            "2026-10-13T09:02:00Z",
            "2026-10-13T09:04:00Z",
            "requeued",
        )
        insert_run(
            con,
            completed,
            2,
            "2026-10-13T09:05:00Z",
            "2026-10-13T10:00:00Z",
            "done",
        )
        insert_run(
            con,
            no_op,
            1,
            "2026-10-12T11:05:00Z",
            "2026-10-12T12:00:00Z",
            "done",
        )
        insert_run(
            con,
            boundary,
            1,
            "2026-10-06T11:30:00Z",
            "2026-10-06T12:00:00Z",
            "done",
        )
        con.executemany(
            """INSERT INTO job_progress(
                ts,job_id,work_key,kind,phase,summary,detail
            ) VALUES(?,?,?,?,?,?,?)""",
            [
                (
                    "2026-10-13T09:03:00Z",
                    completed,
                    "gisce/example#1",
                    "visible",
                    "working",
                    "first output",
                    None,
                ),
                (
                    "2026-10-06T11:40:00Z",
                    boundary,
                    "gisce/example#6",
                    "visible",
                    "working",
                    "first output",
                    None,
                ),
            ],
        )
        con.executemany(
            """INSERT INTO coalesced_notifications(
                job_id,message_id,subject,context_json,created_at
            ) VALUES(?,?,?,?,?)""",
            [
                (
                    completed,
                    "<coalesced-1@github.com>",
                    "coalesced 1",
                    "{}",
                    "2026-10-13T09:10:00Z",
                ),
                (
                    no_op,
                    "<coalesced-2@github.com>",
                    "coalesced 2",
                    "{}",
                    "2026-10-12T11:10:00Z",
                ),
                (
                    boundary,
                    "<coalesced-old@github.com>",
                    "outside window",
                    "{}",
                    "2026-10-06T11:59:59Z",
                ),
            ],
        )

    metrics = metrics_summary(
        db,
        timezone_name="America/New_York",
        window="7d",
        now=NOW,
    )
    slo = metrics["slo"]

    assert metrics["status_counts"]["done"] == 7
    assert "runtime_seconds" in metrics
    assert slo["window"] == {
        "value": "7d",
        "start": "2026-10-06T12:00:00Z",
        "end": "2026-10-13T12:00:00Z",
        "timezone": "America/New_York",
    }
    assert slo["cohort"] == {
        "decision": "auto_trusted",
        "terminal_outcomes": ["completed", "no_op", "blocked"],
        "sample_count": 4,
        "missing_count": 1,
        "excluded_count": 1,
    }
    assert slo["latencies"]["source_received_to_queued"] == {
        "unit": "seconds",
        "p50": 60,
        "p90": 300,
        "p99": 300,
        "sample_count": 2,
        "missing_count": 2,
    }
    assert slo["latencies"]["queued_to_claimed"] == {
        "unit": "seconds",
        "p50": 300,
        "p90": 1500,
        "p99": 1500,
        "sample_count": 3,
        "missing_count": 1,
    }
    assert slo["latencies"]["claimed_to_first_output"]["sample_count"] == 2
    assert slo["latencies"]["claimed_to_done"]["p50"] == 3300
    assert slo["rates"]["blocked"] == {
        "rate": 0.25,
        "percent": 25.0,
        "numerator": 1,
        "denominator": 4,
        "sample_count": 4,
        "missing_count": 0,
    }
    assert slo["rates"]["no_op"]["numerator"] == 1
    assert slo["rates"]["retry"] == {
        "rate": 0.3333,
        "percent": 33.33,
        "numerator": 1,
        "denominator": 3,
        "sample_count": 3,
        "missing_count": 1,
        "requeued_runs": 1,
    }
    assert slo["rates"]["coalescing"] == {
        "rate": 0.25,
        "percent": 25.0,
        "numerator": 2,
        "denominator": 8,
        "sample_count": 8,
        "missing_count": 0,
    }
    assert slo["trends"]["timezone"] == "America/New_York"
    assert slo["trends"]["daily"][0]["bucket"] == "2026-10-06"
    assert slo["trends"]["daily"][-1]["bucket"] == "2026-10-13"


def test_slo_summary_handles_missing_database_and_invalid_window(tmp_path):
    db = tmp_path / "missing.sqlite3"

    metrics = metrics_summary(db, window="24h", now=NOW)

    assert metrics["db_exists"] is False
    assert metrics["slo"]["cohort"]["sample_count"] == 0
    assert metrics["slo"]["rates"]["blocked"]["rate"] is None
    assert not db.exists()
    with pytest.raises(ValueError, match="unsupported SLO window"):
        metrics_summary(db, window="90d", now=NOW)


@pytest.mark.parametrize(
    ("schema_damage", "expected_error"),
    [
        ("pending", "pending migrations 5"),
        ("malformed", "source_received_at"),
        ("missing_avatar", "trigger_actor_avatar_url"),
    ],
)
def test_metrics_api_degrades_safely_for_incompatible_schema(
    tmp_path,
    schema_damage,
    expected_error,
):
    db = tmp_path / "bridge.sqlite3"
    queue = JobQueue(db)
    with queue.connect() as con:
        if schema_damage == "pending":
            con.execute("DELETE FROM schema_migrations WHERE version=5")
        elif schema_damage == "malformed":
            con.execute("ALTER TABLE jobs DROP COLUMN source_received_at")
        else:
            con.execute("ALTER TABLE jobs DROP COLUMN trigger_actor_avatar_url")

    client = TestClient(create_app(DashboardConfig(db=db, require_auth=False)))

    response = client.get("/api/metrics/summary")

    assert response.status_code == 200
    metrics = response.json()["metrics"]
    assert metrics["db_exists"] is True
    assert metrics["schema_ok"] is False
    assert expected_error in metrics["schema_error"]
    assert metrics["status_counts"] == {}
    assert metrics["runtime_usage"] == {"day": [], "month": []}
    assert metrics["runtime_usage_by_actor"] == []
    assert metrics["runtime_seconds"] == {
        "median": None,
        "p90": None,
        "p99": None,
    }
    assert metrics["queue_wait_seconds"] == {
        "median": None,
        "p90": None,
        "p99": None,
    }
    assert metrics["slo"]["cohort"]["sample_count"] == 0


def test_metrics_api_accepts_only_supported_slo_windows(tmp_path):
    db = tmp_path / "bridge.sqlite3"
    JobQueue(db)
    client = TestClient(create_app(DashboardConfig(db=db, require_auth=False)))

    response = client.get(
        "/api/metrics/summary",
        params={"window": "24h", "runtime_window": "all", "timezone": "Europe/Madrid"},
    )

    assert response.status_code == 200
    assert response.json()["metrics"]["slo"]["window"]["value"] == "24h"
    assert response.json()["metrics"]["slo"]["window"]["timezone"] == "Europe/Madrid"
    assert client.get(
        "/api/metrics/summary",
        params={"window": "90d"},
    ).status_code == 422
    assert client.get(
        "/api/metrics/summary",
        params={"runtime_window": "90d"},
    ).status_code == 422
