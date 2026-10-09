from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .database import Database, TransactionMode


@dataclass(frozen=True)
class ProcessSample:
    id: int
    ts: str
    executor_pid: int | None
    root_pid: int | None
    running_job_ids: list[int]
    cpu_ticks: int
    io_bytes: int
    active_since_last_sample: bool
    idle_seconds: int | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "ts": self.ts,
            "executor_pid": self.executor_pid,
            "root_pid": self.root_pid,
            "running_job_ids": self.running_job_ids,
            "cpu_ticks": self.cpu_ticks,
            "io_bytes": self.io_bytes,
            "active_since_last_sample": self.active_since_last_sample,
            "idle_seconds": self.idle_seconds,
        }


@dataclass(frozen=True)
class ObservabilityAlert:
    fingerprint: str
    source: str
    severity: str
    message: str
    context: dict[str, Any]
    first_seen: str
    last_seen: str
    resolved_at: str | None
    observations: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "fingerprint": self.fingerprint,
            "source": self.source,
            "severity": self.severity,
            "message": self.message,
            "context": self.context,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "resolved_at": self.resolved_at,
            "observations": self.observations,
        }


class ObservabilityRepository:
    """Persist process samples and the monitor alert lifecycle."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def record_monitor_observation(
        self,
        now: str,
        metrics: dict[str, Any],
        alerts: list[str],
        retention_seconds: int,
    ) -> None:
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="observability.record_monitor_observation",
        ) as con:
            children = metrics.get("executor_children") or []
            if not isinstance(children, list):
                children = []
            flattened = list(self._flatten_processes(children))
            root_pid = (
                int(children[0]["pid"])
                if children
                and isinstance(children[0], dict)
                and children[0].get("pid") is not None
                else None
            )
            cpu_ticks = sum(
                int(process.get("cpu_ticks") or 0) for process in flattened
            )
            io_bytes = sum(self._process_io_total(process) for process in flattened)
            running_job_ids = [
                job.get("id")
                for job in metrics.get("running_jobs", [])
                if isinstance(job, dict) and job.get("id") is not None
            ]
            previous = con.execute(
                "SELECT ts, root_pid, process_tree_json, cpu_ticks, io_bytes "
                "FROM process_samples ORDER BY id DESC LIMIT 1"
            ).fetchone()
            active = self._sample_active(
                previous,
                root_pid=root_pid,
                pids=[process.get("pid") for process in flattened],
                cpu_ticks=cpu_ticks,
                io_bytes=io_bytes,
            )
            idle_seconds = None
            if previous and not active and previous["ts"]:
                idle_seconds = self._elapsed_seconds(previous["ts"], now)
            con.execute(
                """INSERT INTO process_samples(
                  ts, executor_pid, root_pid, running_job_ids_json,
                  process_tree_json, cpu_ticks, io_bytes,
                  active_since_last_sample, idle_seconds
                ) VALUES(?,?,?,?,?,?,?,?,?)""",
                (
                    now,
                    metrics.get("executor_pid"),
                    root_pid,
                    json.dumps(running_job_ids, separators=(",", ":")),
                    json.dumps(
                        children,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    cpu_ticks,
                    io_bytes,
                    1 if active else 0,
                    idle_seconds,
                ),
            )
            if retention_seconds > 0:
                con.execute(
                    "DELETE FROM process_samples WHERE (julianday(?) - julianday(ts)) * 86400 > ?",
                    (now, retention_seconds),
                )
            self._record_alerts(con, now, metrics, alerts)

    def recent_process_samples(self, limit: int) -> list[ProcessSample]:
        with self.database.read("observability.recent_process_samples") as con:
            rows = con.execute(
                """SELECT id, ts, executor_pid, root_pid, running_job_ids_json,
                       cpu_ticks, io_bytes, active_since_last_sample, idle_seconds
                FROM process_samples ORDER BY id DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        return [self._process_sample_from_row(row) for row in reversed(rows)]

    def list_alerts(
        self, *, include_resolved: bool, limit: int
    ) -> list[ObservabilityAlert]:
        where = "" if include_resolved else "WHERE resolved_at IS NULL"
        with self.database.read("observability.list_alerts") as con:
            rows = con.execute(
                f"""SELECT fingerprint, source, severity, message, context_json,
                       first_seen, last_seen, resolved_at, observations
                FROM alerts {where}
                ORDER BY resolved_at IS NULL DESC, last_seen DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        return [self._alert_from_row(row) for row in rows]

    @classmethod
    def _record_alerts(
        cls, con, now: str, metrics: dict[str, Any], alerts: list[str]
    ) -> None:
        fingerprints: set[str] = set()
        context = {
            "running_jobs": metrics.get("running_jobs", []),
            "executor_service": metrics.get("executor_service"),
            "executor_pid": metrics.get("executor_pid"),
            "reader_timer": metrics.get("reader_timer"),
            "reader_recent": metrics.get("reader_recent"),
        }
        context_json = json.dumps(
            context, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        for message in alerts:
            fingerprint = hashlib.sha256(
                f"monitor\0{message}".encode("utf-8")
            ).hexdigest()
            fingerprints.add(fingerprint)
            con.execute(
                """INSERT INTO alerts(
                    fingerprint, source, severity, message, context_json,
                    first_seen, last_seen, resolved_at, observations
                ) VALUES(?,?,?,?,?,?,?,NULL,1)
                ON CONFLICT(fingerprint) DO UPDATE SET
                  context_json=excluded.context_json,
                  last_seen=excluded.last_seen,
                  resolved_at=NULL,
                  observations=alerts.observations+1""",
                (
                    fingerprint,
                    "monitor",
                    "warning",
                    message,
                    context_json,
                    now,
                    now,
                ),
            )
        if fingerprints:
            placeholders = ",".join("?" for _ in fingerprints)
            con.execute(
                f"UPDATE alerts SET resolved_at=? WHERE source='monitor' "
                f"AND resolved_at IS NULL AND fingerprint NOT IN ({placeholders})",
                (now, *sorted(fingerprints)),
            )
        else:
            con.execute(
                "UPDATE alerts SET resolved_at=? WHERE source='monitor' AND resolved_at IS NULL",
                (now,),
            )

    @staticmethod
    def _sample_active(
        previous,
        *,
        root_pid: int | None,
        pids: list[Any],
        cpu_ticks: int,
        io_bytes: int,
    ) -> bool:
        if previous is None:
            return bool(root_pid or pids or cpu_ticks or io_bytes)
        previous_processes = json.loads(previous["process_tree_json"] or "[]")
        previous_pids = [
            process.get("pid")
            for process in ObservabilityRepository._flatten_processes(
                previous_processes
            )
        ]
        return (
            previous["root_pid"] != root_pid
            or previous_pids != pids
            or cpu_ticks > int(previous["cpu_ticks"] or 0)
            or io_bytes > int(previous["io_bytes"] or 0)
        )

    @staticmethod
    def _flatten_processes(processes: list[Any]) -> list[dict[str, Any]]:
        flattened: list[dict[str, Any]] = []
        for process in processes:
            if not isinstance(process, dict):
                continue
            flattened.append(process)
            children = process.get("children")
            if isinstance(children, list):
                flattened.extend(ObservabilityRepository._flatten_processes(children))
        return flattened

    @staticmethod
    def _process_io_total(process: dict[str, Any]) -> int:
        io_bytes = process.get("io_bytes")
        if not isinstance(io_bytes, dict):
            return 0
        return int(io_bytes.get("read_bytes") or 0) + int(
            io_bytes.get("write_bytes") or 0
        )

    @staticmethod
    def _elapsed_seconds(start: str, end: str) -> int | None:
        try:
            started = datetime.fromisoformat(start.replace("Z", "+00:00"))
            finished = datetime.fromisoformat(end.replace("Z", "+00:00"))
        except ValueError:
            return None
        if started.tzinfo is None:
            started = started.replace(tzinfo=UTC)
        if finished.tzinfo is None:
            finished = finished.replace(tzinfo=UTC)
        return max(0, int((finished - started).total_seconds()))

    @staticmethod
    def _process_sample_from_row(row) -> ProcessSample:
        return ProcessSample(
            id=int(row["id"]),
            ts=str(row["ts"]),
            executor_pid=row["executor_pid"],
            root_pid=row["root_pid"],
            running_job_ids=json.loads(row["running_job_ids_json"] or "[]"),
            cpu_ticks=int(row["cpu_ticks"] or 0),
            io_bytes=int(row["io_bytes"] or 0),
            active_since_last_sample=bool(row["active_since_last_sample"]),
            idle_seconds=row["idle_seconds"],
        )

    @staticmethod
    def _alert_from_row(row) -> ObservabilityAlert:
        return ObservabilityAlert(
            fingerprint=str(row["fingerprint"]),
            source=str(row["source"]),
            severity=str(row["severity"]),
            message=str(row["message"]),
            context=json.loads(row["context_json"] or "{}"),
            first_seen=str(row["first_seen"]),
            last_seen=str(row["last_seen"]),
            resolved_at=row["resolved_at"],
            observations=int(row["observations"]),
        )
