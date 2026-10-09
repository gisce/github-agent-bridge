import inspect
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
    assert len(metric_queries) == 2
    with sqlite3.connect(db) as con:
        detail_plans = [
            [row[3] for row in con.execute(f"EXPLAIN QUERY PLAN {statement}")]
            for statement in detail_queries
        ]
        metric_plans = [
            [row[3] for row in con.execute(f"EXPLAIN QUERY PLAN {statement}")]
            for statement in metric_queries
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


def test_http_cli_and_monitor_use_dashboard_query_boundary():
    for module in (backend, cli, monitor):
        source = inspect.getsource(module)
        assert "sqlite3.connect(" not in source
        assert ".execute(" not in source

    assert "DashboardQueries(config.db)" in inspect.getsource(backend.create_app)
