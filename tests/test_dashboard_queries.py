import inspect
import json
import sqlite3

from github_agent_bridge import backend, cli, monitor
from github_agent_bridge.dashboard_data import DashboardQueries, JobListFilters
from github_agent_bridge.models import Notification
from github_agent_bridge.persistence import Database
from github_agent_bridge.policy import Policy
from github_agent_bridge.queue import JobQueue


def _notification() -> Notification:
    return Notification(
        uid=1,
        message_id="<dashboard-queries@github.com>",
        subject="Re: [gisce/github-agent-bridge] dashboard",
        from_addr="Edu <notifications@github.com>",
        body=(
            "@giscebot inspect "
            "https://github.com/gisce/github-agent-bridge/issues/260#issuecomment-1"
        ),
        auth={"spf": True, "dkim": True, "dmarc": True},
    )


def test_job_list_filters_are_allowlisted_and_values_are_bound():
    where, args = JobListFilters(
        status="done' OR 1=1 --",
        repo="gisce/github-agent-bridge",
        thread=260,
        action="reply_comment",
        intent="work_allowed",
        actor="@ecarreras",
        since="2026-10-01T00:00:00Z",
        until="2026-10-31T23:59:59Z",
    ).where_clause()

    assert where == (
        " WHERE jobs.status=? AND jobs.repo=? AND jobs.thread=? "
        "AND jobs.action=? AND jobs.work_intent=? "
        "AND lower(jobs.trigger_actor)=lower(?) "
        "AND jobs.created_at>=? AND jobs.created_at<=?"
    )
    assert args[0] == "done' OR 1=1 --"
    assert "OR 1=1" not in where
    assert args[5] == "ecarreras"


def test_dashboard_job_list_executes_one_read_query(tmp_path, monkeypatch):
    db = tmp_path / "bridge.sqlite3"
    queue = JobQueue(db)
    queue.enqueue(
        _notification(),
        Policy(trusted_orgs={"gisce"}, bot_logins={"giscebot"}),
    )
    statements: list[str] = []
    original_read_only = Database.read_only

    def traced_read_only(database):
        connection = original_read_only(database)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(Database, "read_only", traced_read_only)

    rows = DashboardQueries(db).list_jobs(
        JobListFilters(repo="gisce/github-agent-bridge", thread=260),
        limit=20,
    )

    read_queries = [
        statement
        for statement in statements
        if statement.lstrip().upper().startswith(("SELECT", "WITH"))
    ]
    assert len(rows) == 1
    assert len(read_queries) == 1
    assert "sqlite_master" not in read_queries[0]
    assert "PRAGMA table_info" not in read_queries[0]


def test_knowledge_page_batches_enrichment_in_one_read_connection(
    tmp_path,
    monkeypatch,
):
    db = tmp_path / "bridge.sqlite3"
    queue = JobQueue(db)
    job, _ = queue.enqueue(
        _notification(),
        Policy(trusted_orgs={"gisce"}, bot_logins={"giscebot"}),
    )
    with sqlite3.connect(db) as con:
        for index in range(20):
            event_id = f"event-{index:02d}"
            con.execute(
                """INSERT INTO feedback_events(
                    id, occurred_at, captured_at, source, scope, actor,
                    comment, context_json, classification, confidence, memorable
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    event_id,
                    f"2026-10-13T10:{index:02d}:00Z",
                    f"2026-10-13T10:{index:02d}:01Z",
                    "github-agent-bridge",
                    "repo:gisce/github-agent-bridge",
                    "github",
                    "Use the dashboard read model.",
                    json.dumps({"message_id": _notification().message_id}),
                    "operating_rule",
                    0.9,
                    1,
                ),
            )
            con.execute(
                """INSERT INTO feedback_rules(
                    id, scope, type, confidence, rule, created_at, last_seen,
                    source_events_json, observations
                ) VALUES(?,?,?,?,?,?,?,?,?)""",
                (
                    f"rule-{index:02d}",
                    "repo:gisce/github-agent-bridge",
                    "operating_rule",
                    0.9,
                    f"Rule {index:02d}",
                    f"2026-10-13T10:{index:02d}:00Z",
                    f"2026-10-13T10:{index:02d}:01Z",
                    json.dumps([event_id]),
                    1,
                ),
            )

    statements: list[str] = []
    connections = 0
    original_read_only = Database.read_only

    def traced_read_only(database):
        nonlocal connections
        connections += 1
        connection = original_read_only(database)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(Database, "read_only", traced_read_only)

    page = DashboardQueries(db).knowledge_page(
        section="rules",
        repo="gisce/github-agent-bridge",
        limit=5,
        offset=0,
    )

    read_queries = [
        statement
        for statement in statements
        if statement.lstrip().upper().startswith(("SELECT", "WITH"))
    ]
    assert connections == 1
    assert len(read_queries) == 5
    assert len(page["rules"]) == 5
    assert page["events"] == []
    assert page["proposals"] == []
    assert page["next_offset"] == 5
    assert page["summary"]["rules"] == 20
    assert page["rules"][0]["source_event_details"][0]["source_job_id"] == job.id
    assert page["rules"][0]["source_event_details"][0]["source_url"].endswith(
        "/issues/260#issuecomment-1"
    )
    with sqlite3.connect(db) as con:
        query_plan = "\n".join(
            row[3]
            for statement in read_queries
            for row in con.execute(f"EXPLAIN QUERY PLAN {statement}")
        )
    assert "CORRELATED" not in query_plan
    assert "sqlite_autoindex_feedback_events_1" in query_plan
    assert "sqlite_autoindex_jobs_1" in query_plan
    assert "sqlite_autoindex_coalesced_notifications_1" in query_plan


def test_dashboard_detail_and_metrics_pin_query_counts_and_plans(
    tmp_path,
    monkeypatch,
):
    db = tmp_path / "bridge.sqlite3"
    queue = JobQueue(db)
    job, _ = queue.enqueue(
        _notification(),
        Policy(trusted_orgs={"gisce"}, bot_logins={"giscebot"}),
    )
    statements: list[str] = []
    original_read_only = Database.read_only

    def traced_read_only(database):
        connection = original_read_only(database)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(Database, "read_only", traced_read_only)
    queries = DashboardQueries(db)

    assert queries.get_job_detail(job.id) is not None
    detail_queries = [
        statement
        for statement in statements
        if statement.lstrip().upper().startswith(("SELECT", "WITH"))
    ]
    statements.clear()
    assert queries.metrics_summary()["status_counts"] == {"pending": 1}
    metric_queries = [
        statement
        for statement in statements
        if statement.lstrip().upper().startswith(("SELECT", "WITH"))
    ]

    assert len(detail_queries) == 5
    assert len(metric_queries) == 5
    assert "sqlite_master" in metric_queries[0]
    assert "schema_migrations" in metric_queries[1]
    metric_data_queries = metric_queries[2:]
    with sqlite3.connect(db) as con:
        detail_plans = [
            [row[3] for row in con.execute(f"EXPLAIN QUERY PLAN {statement}")]
            for statement in detail_queries
        ]
        metric_plans = [
            [row[3] for row in con.execute(f"EXPLAIN QUERY PLAN {statement}")]
            for statement in metric_data_queries
        ]

    detail_plan = "\n".join(
        detail for query_plan in detail_plans for detail in query_plan
    )
    assert "idx_job_session_events_job_id" in detail_plan
    assert "idx_jobs_work_status" in detail_plan
    assert "idx_worklog_job_id" in detail_plan
    assert "idx_job_progress_job_id" in detail_plan
    assert "sqlite_autoindex_job_runs_1" in detail_plan
    assert "idx_coalesced_notifications_job_id" in detail_plan
    assert any("SCAN jobs" in detail for detail in metric_plans[0])
    assert any(
        "SEARCH jobs USING INTEGER PRIMARY KEY" in detail
        for detail in metric_plans[1]
    )
    slo_plan = "\n".join(metric_plans[2])
    assert "idx_jobs_finished_at" in slo_plan
    assert "idx_jobs_created_at" in slo_plan
    assert "idx_coalesced_notifications_created_at" in slo_plan


def test_http_cli_and_monitor_use_dashboard_query_boundary():
    for module in (backend, cli, monitor):
        source = inspect.getsource(module)
        assert "sqlite3.connect(" not in source
        assert ".execute(" not in source

    assert "DashboardQueries(config.db)" in inspect.getsource(backend.create_app)
