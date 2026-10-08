import inspect

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


def test_http_cli_and_monitor_use_dashboard_query_boundary():
    for module in (backend, cli, monitor):
        source = inspect.getsource(module)
        assert "sqlite3.connect(" not in source
        assert ".execute(" not in source

    assert "DashboardQueries(config.db)" in inspect.getsource(backend.create_app)
