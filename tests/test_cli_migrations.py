import sqlite3

import pytest

from github_agent_bridge import cli
from github_agent_bridge.models import Notification
from github_agent_bridge.policy import Policy
from github_agent_bridge.queue import JobQueue
from github_agent_bridge.sql.migrations import MigrationRequiredError


def _enqueue_waiting_approval(queue: JobQueue) -> None:
    notification = Notification(
        uid=1,
        message_id="<migration-preflight@github.com>",
        subject="Re: [gisce/github-agent-bridge] migration",
        from_addr="Edu <notifications@github.com>",
        body=(
            "@giscebot review "
            "https://github.com/gisce/github-agent-bridge/issues/260#issuecomment-1"
        ),
        auth={"spf": True, "dkim": True, "dmarc": True},
    )
    policy = Policy(
        trusted_orgs={"gisce"},
        bot_logins={"giscebot"},
    )
    job, _ = queue.enqueue(notification, policy)
    assert job is not None
    with queue.connect() as con:
        con.execute(
            "UPDATE jobs SET status='waiting_approval' WHERE id=?",
            (job.id,),
        )


def test_migrate_db_blocks_active_jobs_without_mutating_legacy_schema(
    tmp_path,
    capsys,
):
    db = tmp_path / "bridge.sqlite3"
    queue = JobQueue(db)
    _enqueue_waiting_approval(queue)
    with queue.connect() as con:
        con.execute("DROP TABLE schema_migrations")
        con.execute("DROP TABLE job_commit_statuses")

    with pytest.raises(MigrationRequiredError, match="no migration history"):
        JobQueue(db)

    backup_dir = tmp_path / "backups"
    result = cli.main(
        ["--db", str(db), "migrate-db", "--backup-dir", str(backup_dir)]
    )

    assert result == 2
    assert "waiting_approval" in capsys.readouterr().err
    assert not backup_dir.exists()
    with sqlite3.connect(db) as con:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert "schema_migrations" not in tables
    assert "job_commit_statuses" not in tables


def test_migrate_db_backs_up_quiet_database_before_applying(tmp_path, capsys):
    db = tmp_path / "bridge.sqlite3"
    queue = JobQueue(db)
    with queue.connect() as con:
        con.execute("DELETE FROM schema_migrations WHERE version=2")
        con.execute("DROP TABLE job_commit_statuses")

    backup_dir = tmp_path / "backups"
    result = cli.main(
        ["--db", str(db), "migrate-db", "--backup-dir", str(backup_dir)]
    )

    assert result == 0
    assert len(list(backup_dir.glob("*.sqlite3"))) == 1
    assert '"backup":' in capsys.readouterr().out
    JobQueue(db)
