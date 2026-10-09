from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass

from ..models import utc_now
from ..session_correlation import session_id_for_job
from .database import Database, TransactionMode


@dataclass(frozen=True)
class RuntimeProcess:
    """Typed identity for the process executing one job attempt."""

    executor_id: str
    worker_id: str
    pid: int
    ppid: int
    pgid: int
    sid: int
    start_time_ticks: int

    @classmethod
    def from_identity(
        cls,
        executor_id: str,
        worker_id: str,
        identity: Mapping[str, int],
    ) -> "RuntimeProcess":
        return cls(
            executor_id=executor_id,
            worker_id=worker_id,
            pid=int(identity["pid"]),
            ppid=int(identity["ppid"]),
            pgid=int(identity["pgid"]),
            sid=int(identity["sid"]),
            start_time_ticks=int(identity["start_time_ticks"]),
        )

    def to_metadata(self, registered_at: str) -> dict[str, object]:
        return {
            "state": "running",
            "executor_id": self.executor_id,
            "worker_id": self.worker_id,
            "pid": self.pid,
            "ppid": self.ppid,
            "pgid": self.pgid,
            "sid": self.sid,
            "start_time_ticks": self.start_time_ticks,
            "registered_at": registered_at,
        }


class RuntimeRepository:
    """Persist execution attempts, processes, heartbeats and runtime events."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def record_worker_heartbeat(
        self,
        worker_id: str,
        executor_id: str,
        pid: int,
        loop_state: str,
        active_job_id: int | None = None,
        recent_error_count: int = 0,
    ) -> None:
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="runtime.record_worker_heartbeat",
        ) as con:
            con.execute(
                """INSERT INTO worker_heartbeats(
                       worker_id, executor_id, pid, last_seen, active_job_id, loop_state, recent_error_count
                   ) VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(worker_id) DO UPDATE SET
                       executor_id=excluded.executor_id,
                       pid=excluded.pid,
                       last_seen=excluded.last_seen,
                       active_job_id=excluded.active_job_id,
                       loop_state=excluded.loop_state,
                       recent_error_count=excluded.recent_error_count""",
                (
                    worker_id,
                    executor_id,
                    pid,
                    utc_now(),
                    active_job_id,
                    loop_state,
                    recent_error_count,
                ),
            )

    def delete_worker_heartbeats_except(self, executor_id: str) -> int:
        """Remove heartbeat rows left by previous executor processes."""
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="runtime.delete_worker_heartbeats_except",
        ) as con:
            cursor = con.execute(
                "DELETE FROM worker_heartbeats WHERE executor_id != ?",
                (executor_id,),
            )
            return cursor.rowcount

    def register_process(self, job_id: int, process: RuntimeProcess) -> bool:
        """Persist the exact process that owns a running job."""
        now = utc_now()
        with self.database.transaction(
            TransactionMode.IMMEDIATE,
            operation="runtime.register_process",
        ) as con:
            row = con.execute(
                "SELECT work_key, metadata_json FROM jobs WHERE id=? AND status='running' AND locked_by=?",
                (job_id, process.worker_id),
            ).fetchone()
            if row is None:
                return False
            metadata = json.loads(row["metadata_json"] or "{}")
            runtime_process = process.to_metadata(now)
            metadata["runtime_process"] = runtime_process
            con.execute(
                "UPDATE jobs SET metadata_json=?, updated_at=? WHERE id=? AND status='running' AND locked_by=?",
                (
                    json.dumps(metadata, sort_keys=True),
                    now,
                    job_id,
                    process.worker_id,
                ),
            )
            self.record_session_event(
                con,
                job_id,
                row["work_key"],
                str(metadata.get("openclaw_session_id") or session_id_for_job(job_id)),
                "process_registered",
                f"runtime process {process.pid} registered",
                json.dumps(runtime_process, sort_keys=True),
                occurred_at=now,
            )
            return True

    def mark_process_exited(self, job_id: int, worker_id: str) -> bool:
        """Mark a registered process as exited while result handling finishes."""
        now = utc_now()
        with self.database.transaction(
            TransactionMode.IMMEDIATE,
            operation="runtime.mark_process_exited",
        ) as con:
            row = con.execute(
                "SELECT metadata_json FROM jobs WHERE id=? AND status='running' AND locked_by=?",
                (job_id, worker_id),
            ).fetchone()
            if row is None:
                return False
            metadata = json.loads(row["metadata_json"] or "{}")
            runtime_process = metadata.get("runtime_process")
            if not isinstance(runtime_process, dict):
                return False
            runtime_process["state"] = "exited"
            runtime_process["exited_at"] = now
            metadata["runtime_process"] = runtime_process
            cursor = con.execute(
                "UPDATE jobs SET metadata_json=?, updated_at=? WHERE id=? AND status='running' AND locked_by=?",
                (json.dumps(metadata, sort_keys=True), now, job_id, worker_id),
            )
            return bool(cursor.rowcount)

    def add_job_session_event(
        self,
        job_id: int,
        event_type: str,
        summary: str,
        detail: str | None,
        *,
        progress_kind: str,
    ) -> bool:
        """Append a session event and its progress projection atomically."""
        now = utc_now()
        with self.database.transaction(
            TransactionMode.IMMEDIATE,
            operation="runtime.add_job_session_event",
        ) as con:
            row = con.execute(
                "SELECT work_key, metadata_json FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
            if row is None:
                return False
            metadata = json.loads(row["metadata_json"] or "{}")
            session_id = str(
                metadata.get("openclaw_session_id") or session_id_for_job(job_id)
            )
            con.execute("UPDATE jobs SET updated_at=? WHERE id=?", (now, job_id))
            self.record_session_event(
                con,
                job_id,
                row["work_key"],
                session_id,
                event_type,
                summary,
                detail,
                occurred_at=now,
            )
            self.record_progress(
                con,
                job_id,
                row["work_key"],
                progress_kind,
                event_type[:80],
                summary,
                detail,
                occurred_at=now,
            )
            return True

    def add_job_worklog(
        self,
        job_id: int,
        phase: str,
        summary: str,
        detail: str | None,
    ) -> bool:
        with self.database.transaction(
            TransactionMode.IMMEDIATE,
            operation="runtime.add_job_worklog",
        ) as con:
            row = con.execute(
                "SELECT work_key FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
            if row is None:
                return False
            self.record_worklog(con, job_id, row["work_key"], phase, summary, detail)
            return True

    @staticmethod
    def record_job_run_started(
        con: sqlite3.Connection,
        job_id: int,
        attempt: int,
        started_at: str,
        worker_id: str,
        session_id: str,
    ) -> None:
        con.execute(
            """INSERT INTO job_runs(job_id,attempt,started_at,worker_id,session_id)
            VALUES(?,?,?,?,?)""",
            (job_id, attempt, started_at, worker_id, session_id),
        )

    @staticmethod
    def record_job_run_finished(
        con: sqlite3.Connection,
        job_id: int,
        result: str,
        finished_at: str,
    ) -> None:
        con.execute(
            """UPDATE job_runs
            SET finished_at=?, result=?
            WHERE id=(
                SELECT id FROM job_runs
                WHERE job_id=? AND finished_at IS NULL
                ORDER BY attempt DESC LIMIT 1
            ) AND finished_at IS NULL""",
            (finished_at, result, job_id),
        )

    @staticmethod
    def record_worklog(
        con: sqlite3.Connection,
        job_id: int | None,
        work_key: str | None,
        phase: str,
        summary: str,
        detail: str | None,
        *,
        occurred_at: str | None = None,
    ) -> None:
        con.execute(
            "INSERT INTO worklog(ts,job_id,work_key,phase,summary,detail) VALUES(?,?,?,?,?,?)",
            (occurred_at or utc_now(), job_id, work_key, phase, summary, detail),
        )

    @staticmethod
    def record_session_event(
        con: sqlite3.Connection,
        job_id: int,
        work_key: str | None,
        session_id: str,
        event_type: str,
        summary: str,
        detail: str | None,
        *,
        occurred_at: str | None = None,
    ) -> None:
        con.execute(
            "INSERT INTO job_session_events(ts,job_id,work_key,session_id,event_type,summary,detail) VALUES(?,?,?,?,?,?,?)",
            (
                occurred_at or utc_now(),
                job_id,
                work_key,
                session_id,
                event_type,
                summary,
                detail,
            ),
        )

    @staticmethod
    def record_progress(
        con: sqlite3.Connection,
        job_id: int,
        work_key: str | None,
        kind: str,
        phase: str,
        summary: str,
        detail: str | None,
        *,
        occurred_at: str | None = None,
    ) -> None:
        con.execute(
            "INSERT INTO job_progress(ts,job_id,work_key,kind,phase,summary,detail) VALUES(?,?,?,?,?,?,?)",
            (
                occurred_at or utc_now(),
                job_id,
                work_key,
                kind,
                phase,
                summary,
                detail,
            ),
        )

    @staticmethod
    def job_metadata(con: sqlite3.Connection, job_id: int) -> dict[str, object]:
        row = con.execute(
            "SELECT metadata_json FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
        if row is None:
            return {}
        return json.loads(row["metadata_json"] or "{}")
