from __future__ import annotations

import json
import os
import sqlite3
from collections import Counter
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .parser import extract_github_context
from .persistence import Database
from .session_events import redact_event_detail
from .session_correlation import job_session_metadata
from .sql.migrations import MigrationError, validate_migrations

JOB_LIST_ORDER_SQL = """
    CASE status
        WHEN 'running' THEN 0
        WHEN 'pending' THEN 1
        WHEN 'waiting_approval' THEN 2
        WHEN 'blocked' THEN 3
        WHEN 'denied' THEN 3
        WHEN 'done' THEN 4
        ELSE 5
    END,
    COALESCE(finished_at, started_at, updated_at, created_at) DESC,
    id DESC
"""
EXECUTOR_PAUSE_STATE_KEY = "executor_paused"
SLO_WINDOWS = {
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}
RUNTIME_WINDOWS = {
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
    "all": None,
}
SERVICE_TERMINAL_OUTCOMES = ("completed", "no_op", "blocked")
SLO_METRICS_SQL = """
WITH service_candidates AS (
    SELECT id, source_received_at, created_at, finished_at, terminal_outcome
    FROM jobs
    WHERE decision='auto_trusted'
      AND finished_at>=?
      AND finished_at<=?
),
run_stats AS (
    SELECT job_runs.job_id,
           MIN(job_runs.started_at) AS first_claimed_at,
           COUNT(*) AS run_count,
           SUM(CASE WHEN job_runs.result='requeued' THEN 1 ELSE 0 END) AS requeued_runs
    FROM job_runs
    JOIN service_candidates ON service_candidates.id=job_runs.job_id
    GROUP BY job_runs.job_id
),
progress_stats AS (
    SELECT job_progress.job_id, MIN(job_progress.ts) AS first_visible_at
    FROM job_progress
    JOIN service_candidates ON service_candidates.id=job_progress.job_id
    WHERE job_progress.kind='visible'
    GROUP BY job_progress.job_id
)
SELECT 'service' AS record_type,
       service_candidates.id AS item_id,
       service_candidates.source_received_at,
       service_candidates.created_at,
       service_candidates.finished_at,
       service_candidates.terminal_outcome,
       run_stats.first_claimed_at,
       progress_stats.first_visible_at,
       COALESCE(run_stats.run_count, 0) AS run_count,
       COALESCE(run_stats.requeued_runs, 0) AS requeued_runs,
       NULL AS event_at
FROM service_candidates
LEFT JOIN run_stats ON run_stats.job_id=service_candidates.id
LEFT JOIN progress_stats ON progress_stats.job_id=service_candidates.id
UNION ALL
SELECT 'new_job', jobs.id, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0,
       jobs.created_at
FROM jobs
WHERE jobs.created_at>=? AND jobs.created_at<=?
UNION ALL
SELECT 'coalesced', coalesced_notifications.id, NULL, NULL, NULL, NULL,
       NULL, NULL, 0, 0, coalesced_notifications.created_at
FROM coalesced_notifications
WHERE coalesced_notifications.created_at>=?
  AND coalesced_notifications.created_at<=?
"""


def readonly_connect(
    db: str | Path,
    *,
    operation: str = "dashboard.read",
) -> AbstractContextManager[sqlite3.Connection]:
    return Database(db).read(operation)


def parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    except ValueError:
        return None


def duration_seconds(start: str | None, end: str | None = None) -> int | None:
    started = parse_utc(start)
    if started is None:
        return None
    finished = parse_utc(end) or datetime.now(UTC)
    return max(0, int((finished - started).total_seconds()))


def completed_duration_seconds(start: str | None, end: str | None) -> int | None:
    started = parse_utc(start)
    finished = parse_utc(end)
    if started is None or finished is None:
        return None
    return max(0, int((finished - started).total_seconds()))


def row_get(row: sqlite3.Row, key: str, default: Any = None) -> Any:
    return row[key] if key in row.keys() else default


def executor_pause_state_from_raw(raw: str | None) -> dict[str, object]:
    if not raw:
        return {"paused": False}
    try:
        state = json.loads(raw)
    except json.JSONDecodeError:
        return {"paused": False, "error": "invalid_executor_pause_state"}
    if not isinstance(state, dict):
        return {"paused": False, "error": "invalid_executor_pause_state"}
    return {
        "paused": bool(state.get("paused")),
        **({"reason": state["reason"]} if state.get("reason") else {}),
        **({"updated_at": state["updated_at"]} if state.get("updated_at") else {}),
    }


def coerce_limit(value: int, maximum: int = 200) -> int:
    return max(1, min(value, maximum))


def parse_model_route_detail(detail: str | None) -> dict[str, Any]:
    if not detail:
        return {
            "configured": False,
            "model": None,
            "thinking": None,
            "summary": "n/a",
        }
    model = None
    thinking = None
    for part in detail.split():
        if part.startswith("model="):
            model = part.removeprefix("model=") or None
        elif part.startswith("thinking="):
            thinking = part.removeprefix("thinking=") or None
    return {
        "configured": bool(model or thinking),
        "model": model,
        "thinking": thinking,
        "summary": detail,
    }


def action_mode_for_intent(work_intent: str | None) -> str:
    return "fix_allowed" if work_intent == "work_allowed" else "review_only"


def intent_classifier_summary(metadata: dict[str, Any]) -> dict[str, Any] | None:
    classifier = metadata.get("intent_classifier")
    if not isinstance(classifier, dict):
        return None
    parser = classifier.get("parser") if isinstance(classifier.get("parser"), dict) else {}
    llm = classifier.get("llm") if isinstance(classifier.get("llm"), dict) else {}
    error = classifier.get("error")
    return {
        "enabled": bool(classifier.get("enabled")),
        "parser": parser,
        "llm": llm,
        "error": str(error) if error else None,
    }


def jobs_select_sql(_con: sqlite3.Connection | None = None) -> str:
    return """
        SELECT jobs.*,
            (
                SELECT detail
                FROM job_session_events
                WHERE job_id=jobs.id AND event_type='model_route_selected'
                ORDER BY id DESC
                LIMIT 1
            ) AS model_route_detail,
            (
                SELECT running.id
                FROM jobs AS running
                WHERE jobs.status='pending'
                  AND running.work_key=jobs.work_key
                  AND running.status='running'
                ORDER BY running.id
                LIMIT 1
            ) AS blocked_by_job_id
        FROM jobs
    """


def job_summary(row: sqlite3.Row) -> dict[str, Any]:
    context = json.loads(row["context_json"] or "{}")
    metadata = json.loads(row["metadata_json"] or "{}")
    blocked_by_job_id = row_get(row, "blocked_by_job_id")
    status = row["status"]
    return {
        "id": row["id"],
        "work_key": row["work_key"],
        "repo": row["repo"],
        "thread": row["thread"],
        "status": status,
        "runnable": status == "pending" and blocked_by_job_id is None,
        "blocked_by_job_id": blocked_by_job_id,
        "queue_state": "serialized_by_work_key" if blocked_by_job_id is not None else ("runnable" if status == "pending" else status),
        "action": row["action"],
        "decision": row["decision"],
        "intent": row["work_intent"],
        "action_mode": action_mode_for_intent(row["work_intent"]),
        "subject": row["subject"],
        "trigger_actor": row_get(row, "trigger_actor"),
        "trigger_actor_avatar_url": row_get(row, "trigger_actor_avatar_url"),
        "attempts": row["attempts"],
        "coalesced_count": row["coalesced_count"],
        "last_error": row["last_error"],
        "locked_by": row["locked_by"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "queue_wait_seconds": duration_seconds(row["created_at"], row["started_at"]) if row["started_at"] else None,
        "runtime_seconds": duration_seconds(row["started_at"], row["finished_at"]),
        "github_urls": context.get("urls", []),
        "model_route": parse_model_route_detail(row_get(row, "model_route_detail")),
        "intent_classifier": intent_classifier_summary(metadata),
    }


@dataclass(frozen=True)
class JobListFilters:
    """Allowlisted filters supported by the dashboard job read model."""

    status: str | None = None
    repo: str | None = None
    thread: int | None = None
    action: str | None = None
    intent: str | None = None
    actor: str | None = None
    since: str | None = None
    until: str | None = None

    def where_clause(self) -> tuple[str, list[Any]]:
        filters = (
            ("jobs.status", self.status),
            ("jobs.repo", self.repo),
            ("jobs.thread", self.thread),
            ("jobs.action", self.action),
            ("jobs.work_intent", self.intent),
        )
        clauses: list[str] = []
        args: list[Any] = []
        for column, value in filters:
            if value is None:
                continue
            clauses.append(f"{column}=?")
            args.append(value)
        if self.actor and self.actor.strip():
            clauses.append("lower(jobs.trigger_actor)=lower(?)")
            args.append(self.actor.strip().lstrip("@"))
        if self.since:
            clauses.append("jobs.created_at>=?")
            args.append(self.since)
        if self.until:
            clauses.append("jobs.created_at<=?")
            args.append(self.until)
        return (" WHERE " + " AND ".join(clauses), args) if clauses else ("", args)


def where_clause(filters: JobListFilters) -> tuple[str, list[Any]]:
    """Build SQL only from the fixed dashboard filter contract."""
    return filters.where_clause()


def _prefixed_row(
    row: sqlite3.Row,
    prefix: str,
    *,
    include_kind: bool = False,
) -> dict[str, Any] | None:
    if row[f"{prefix}_id"] is None:
        return None
    keys = ("ts", "phase", "summary")
    if include_kind:
        keys = ("id", "ts", "kind", "phase", "summary", "detail")
    item = {key: row[f"{prefix}_{key}"] for key in keys}
    if include_kind:
        item["age_seconds"] = duration_seconds(item.get("ts"))
    return item


def inspect_db_read_only(db: str | Path) -> dict[str, Any]:
    path = Path(db).expanduser()
    out: dict[str, Any] = {"db_path": str(path), "db_exists": path.exists(), "executor_pause": {"paused": False}}
    if not path.exists():
        return out
    with readonly_connect(path, operation="dashboard.status") as con:
        try:
            validate_migrations(con)
        except (MigrationError, sqlite3.OperationalError) as exc:
            return out | {"schema_ok": False, "schema_error": str(exc)}
        out["schema_ok"] = True
        counts = {r["status"]: int(r["count"]) for r in con.execute("SELECT status, count(*) count FROM jobs GROUP BY status")}
        out["counts"] = counts
        out.update(counts)
        pending_age = con.execute("SELECT CAST((julianday('now') - julianday(min(created_at))) * 86400 AS INTEGER) age FROM jobs WHERE status='pending'").fetchone()["age"]
        out["oldest_pending_age_seconds"] = None if pending_age is None else int(pending_age)
        state = {r["key"]: r["value"] for r in con.execute("SELECT key,value FROM state")}
        out["last_uid"] = state.get("last_uid")
        out["executor_process_tracking_id"] = state.get("executor_process_tracking_id")
        out["executor_worker_count"] = int(state.get("executor_worker_count") or 0)
        out["executor_pause"] = executor_pause_state_from_raw(state.get(EXECUTOR_PAUSE_STATE_KEY))
        heartbeats = []
        for row in con.execute(
            """SELECT worker_id, executor_id, pid, last_seen, active_job_id, loop_state,
                      recent_error_count,
                      CAST((julianday('now') - julianday(last_seen)) * 86400 AS INTEGER) age_seconds
               FROM worker_heartbeats ORDER BY worker_id"""
        ):
            item = dict(row)
            item["age_seconds"] = max(0, int(item["age_seconds"] or 0))
            heartbeats.append(item)
        out["worker_heartbeats"] = heartbeats
        knowledge_counts = {
            r["status"]: int(r["count"])
            for r in con.execute("SELECT status, count(*) count FROM feedback_rule_proposals GROUP BY status")
        }
        out["knowledge"] = {
            "proposed": knowledge_counts.get("proposed", 0),
            "approved": knowledge_counts.get("approved", 0),
            "rejected": knowledge_counts.get("rejected", 0),
            "errors": knowledge_counts.get("error", 0),
        }
        unresolved_quarantines = con.execute(
            "SELECT count(*) count FROM quarantined_notifications WHERE resolved_at IS NULL"
        ).fetchone()["count"]
        out["quarantined_notifications"] = int(unresolved_quarantines)
        latest_quarantine = con.execute(
            """
            SELECT id, uid, message_id, subject, reason, error, created_at
            FROM quarantined_notifications
            WHERE resolved_at IS NULL
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        if latest_quarantine:
            out["latest_quarantined_notification"] = dict(latest_quarantine)
        last_log = con.execute("SELECT ts, phase, summary FROM worklog ORDER BY id DESC LIMIT 1").fetchone()
        if last_log:
            out["last_worklog"] = dict(last_log)
        running_rows = con.execute(
            """
            WITH ranked_worklog AS (
                SELECT *, row_number() OVER (PARTITION BY job_id ORDER BY id DESC) AS row_number
                FROM worklog
            ), ranked_progress AS (
                SELECT *, row_number() OVER (PARTITION BY job_id, kind ORDER BY id DESC) AS row_number
                FROM job_progress
                WHERE kind IN ('semantic', 'visible')
            )
            SELECT jobs.id, jobs.work_key, jobs.work_intent, jobs.locked_by, jobs.attempts,
                   jobs.started_at, jobs.updated_at, jobs.metadata_json,
                   worklog.id AS worklog_id, worklog.ts AS worklog_ts,
                   worklog.phase AS worklog_phase, worklog.summary AS worklog_summary,
                   worklog.detail AS worklog_detail,
                   semantic.id AS semantic_id, semantic.ts AS semantic_ts,
                   semantic.kind AS semantic_kind, semantic.phase AS semantic_phase,
                   semantic.summary AS semantic_summary, semantic.detail AS semantic_detail,
                   visible.id AS visible_id, visible.ts AS visible_ts,
                   visible.kind AS visible_kind, visible.phase AS visible_phase,
                   visible.summary AS visible_summary, visible.detail AS visible_detail
            FROM jobs
            LEFT JOIN ranked_worklog AS worklog
              ON worklog.job_id=jobs.id AND worklog.row_number=1
            LEFT JOIN ranked_progress AS semantic
              ON semantic.job_id=jobs.id AND semantic.kind='semantic' AND semantic.row_number=1
            LEFT JOIN ranked_progress AS visible
              ON visible.job_id=jobs.id AND visible.kind='visible' AND visible.row_number=1
            WHERE jobs.status='running'
            ORDER BY jobs.id
            """
        ).fetchall()
        out["running_jobs"] = [
            {
                "id": row["id"],
                "work_key": row["work_key"],
                "intent": row["work_intent"],
                "work_intent": row["work_intent"],
                "locked_by": row["locked_by"],
                "attempts": row["attempts"],
                "runtime_process": json.loads(row["metadata_json"] or "{}").get("runtime_process"),
                "age_seconds": duration_seconds(row["started_at"]),
                "idle_seconds": duration_seconds(row["updated_at"]),
                "last_worklog": _prefixed_row(row, "worklog"),
                "semantic_progress": _prefixed_row(row, "semantic", include_kind=True),
                "visible_progress": _prefixed_row(row, "visible", include_kind=True),
            }
            for row in running_rows
        ]
        out["running"] = len(running_rows)
    return out


def list_jobs(
    db: str | Path,
    *,
    status_filter: str | None = None,
    repo: str | None = None,
    thread: int | None = None,
    action: str | None = None,
    intent: str | None = None,
    actor: str | None = None,
    since: str | None = None,
    until: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    path = Path(db).expanduser()
    if not path.exists():
        return []
    with readonly_connect(path, operation="dashboard.list_jobs") as con:
        where, args = where_clause(
            JobListFilters(
                status=status_filter,
                repo=repo,
                thread=thread,
                action=action,
                intent=intent,
                actor=actor,
                since=since,
                until=until,
            )
        )
        args.append(coerce_limit(limit))
        rows = con.execute(f"{jobs_select_sql(con)}{where} ORDER BY {JOB_LIST_ORDER_SQL} LIMIT ?", args).fetchall()
    return [job_summary(row) for row in rows]


def list_job_actors(db: str | Path, *, limit: int = 100) -> list[dict[str, Any]]:
    path = Path(db).expanduser()
    if not path.exists():
        return []
    with readonly_connect(path, operation="dashboard.list_job_actors") as con:
        rows = con.execute(
            """
            SELECT
                trigger_actor,
                max(trigger_actor_avatar_url) AS trigger_actor_avatar_url,
                count(*) AS job_count,
                max(updated_at) AS last_seen
            FROM jobs
            WHERE trigger_actor IS NOT NULL AND trigger_actor != ''
            GROUP BY lower(trigger_actor)
            ORDER BY job_count DESC, last_seen DESC, lower(trigger_actor)
            LIMIT ?
            """,
            (coerce_limit(limit),),
        ).fetchall()
    return [
        {
            "login": row["trigger_actor"],
            "avatar_url": row["trigger_actor_avatar_url"],
            "job_count": row["job_count"],
            "last_seen": row["last_seen"],
        }
        for row in rows
    ]


def list_all_job_actor_logins(db: str | Path) -> list[str]:
    path = Path(db).expanduser()
    if not path.exists():
        return []
    with readonly_connect(
        path,
        operation="dashboard.list_all_job_actor_logins",
    ) as con:
        rows = con.execute(
            """
            SELECT lower(trigger_actor) AS login
            FROM jobs
            WHERE trigger_actor IS NOT NULL AND trigger_actor != ''
            GROUP BY lower(trigger_actor)
            ORDER BY login
            """
        ).fetchall()
    return [str(row["login"]) for row in rows if row["login"]]


def get_job_detail(db: str | Path, job_id: int) -> dict[str, Any] | None:
    path = Path(db).expanduser()
    if not path.exists():
        return None
    with readonly_connect(path, operation="dashboard.get_job_detail") as con:
        row = con.execute(f"{jobs_select_sql(con)} WHERE jobs.id=?", (job_id,)).fetchone()
        if row is None:
            return None
        job = job_summary(row)
        job["context"] = json.loads(row["context_json"] or "{}")
        job["metadata"] = json.loads(row["metadata_json"] or "{}")
        job["worklog"] = [
            dict(log)
            for log in con.execute(
                "SELECT id, ts, phase, summary, detail FROM worklog WHERE job_id=? ORDER BY id",
                (job_id,),
            ).fetchall()
        ]
        job["progress"] = [
            dict(progress)
            for progress in con.execute(
                "SELECT id, ts, kind, phase, summary, detail FROM job_progress WHERE job_id=? ORDER BY id",
                (job_id,),
            ).fetchall()
        ]
        job["runs"] = [
            dict(run)
            for run in con.execute(
                """SELECT id, attempt, started_at, finished_at, result, worker_id, session_id, is_estimated
                FROM job_runs WHERE job_id=? ORDER BY attempt""",
                (job_id,),
            ).fetchall()
        ]
        job["coalesced_notifications"] = [
            {
                "id": row["id"],
                "uid": row["uid"],
                "message_id": row["message_id"],
                "subject": row["subject"],
                "trigger_actor": row_get(row, "trigger_actor"),
                "trigger_actor_avatar_url": row_get(row, "trigger_actor_avatar_url"),
                "context": json.loads(row["context_json"] or "{}"),
                "created_at": row["created_at"],
            }
            for row in con.execute(
                "SELECT * FROM coalesced_notifications WHERE job_id=? ORDER BY id",
                (job_id,),
            ).fetchall()
        ]
    return job


def job_logs(db: str | Path, job_id: int, limit: int = 100) -> list[dict[str, Any]]:
    path = Path(db).expanduser()
    if not path.exists():
        return []
    with readonly_connect(path, operation="dashboard.job_logs") as con:
        rows = con.execute(
            "SELECT id, ts, phase, summary, detail FROM worklog WHERE job_id=? ORDER BY id DESC LIMIT ?",
            (job_id, coerce_limit(limit, maximum=500)),
        ).fetchall()
    return [dict(row) for row in reversed(rows)]


def latest_job_progress(db: str | Path, job_id: int, kind: str | None = None) -> dict[str, Any] | None:
    path = Path(db).expanduser()
    if not path.exists():
        return None
    with readonly_connect(path, operation="dashboard.latest_job_progress") as con:
        return _latest_job_progress(con, job_id, kind)


def job_session(db: str | Path, job_id: int) -> dict[str, Any] | None:
    job = get_job_detail(db, job_id)
    if job is None:
        return None
    session = job_session_metadata(job)
    session["job_id"] = job_id
    session["work_key"] = job["work_key"]
    session["status"] = job["status"]
    session_id = str(session["id"])
    session["transcript_available"] = (
        openclaw_session_file(session_id) is not None or openclaw_trajectory_file(session_id) is not None
    )
    session["detail"] = (
        "Dispatches use this explicit OpenClaw session id. "
        "The dashboard exposes bounded, redacted bridge events and OpenClaw transcript entries for this session."
    )
    return session


def job_session_events(db: str | Path, job_id: int, *, after_id: int | None = None, limit: int = 100) -> list[dict[str, Any]]:
    path = Path(db).expanduser()
    if not path.exists():
        return []
    with readonly_connect(path, operation="dashboard.job_session_events") as con:
        where = "job_id=?"
        args: list[Any] = [job_id]
        if after_id is not None:
            where += " AND id>?"
            args.append(after_id)
        args.append(coerce_limit(limit, maximum=500))
        rows = con.execute(
            f"""
            SELECT id, ts, job_id, work_key, session_id, event_type, summary, detail
            FROM job_session_events
            WHERE {where}
            ORDER BY id
            LIMIT ?
            """,
            args,
        ).fetchall()
    return [dict(row) | {"detail": redact_event_detail(row["detail"])} for row in rows]


def job_session_transcript(db: str | Path, job_id: int, *, limit: int = 500) -> list[dict[str, Any]]:
    session = job_session(db, job_id)
    if session is None:
        return []
    max_entries = coerce_limit(limit, maximum=1000)
    session_id = str(session.get("id") or "")
    if not session_id:
        return []
    session_file = openclaw_session_file(session_id)
    entries: list[dict[str, Any]] = []
    if session_file is not None and session_file.exists():
        for line in session_file.read_text(encoding="utf-8", errors="replace").splitlines():
            if len(entries) >= max_entries:
                break
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            entry = transcript_entry(item)
            if entry is not None:
                entries.append(entry)
    if (session.get("status") == "running" or not entries) and len(entries) < max_entries:
        trajectory_file = openclaw_trajectory_file(session_id)
        if trajectory_file is not None and trajectory_file.exists():
            for line in trajectory_file.read_text(encoding="utf-8", errors="replace").splitlines():
                if len(entries) >= max_entries:
                    break
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                entry = transcript_entry_from_trajectory(item)
                if entry is not None:
                    entries.append(entry)
    if session.get("status") == "running" and len(entries) < max_entries:
        live_events = job_session_events(db, job_id, limit=max_entries)
        for event in live_events:
            entry = transcript_entry_from_session_event(event)
            if entry is None:
                continue
            entries.append(entry)
            if len(entries) >= max_entries:
                break
    return entries


def openclaw_session_file(session_id: str) -> Path | None:
    store = Path(os.getenv("GITHUB_AGENT_BRIDGE_OPENCLAW_SESSION_STORE", "~/.openclaw/agents/github/sessions/sessions.json")).expanduser()
    if not store.exists():
        fallback = store.with_name(f"{session_id}.jsonl")
        return fallback if fallback.exists() else None
    try:
        sessions = json.loads(store.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    for value in sessions.values() if isinstance(sessions, dict) else []:
        if not isinstance(value, dict) or str(value.get("sessionId") or "") != session_id:
            continue
        session_file = value.get("sessionFile")
        if not session_file:
            continue
        path = Path(str(session_file)).expanduser()
        return path if path.exists() else None
    fallback = store.with_name(f"{session_id}.jsonl")
    return fallback if fallback.exists() else None


def openclaw_trajectory_file(session_id: str) -> Path | None:
    store = Path(os.getenv("GITHUB_AGENT_BRIDGE_OPENCLAW_SESSION_STORE", "~/.openclaw/agents/github/sessions/sessions.json")).expanduser()
    fallback = store.with_name(f"{session_id}.trajectory.jsonl")
    if fallback.exists():
        return fallback
    pointer = store.with_name(f"{session_id}.trajectory-path.json")
    if not pointer.exists():
        return None
    try:
        payload = json.loads(pointer.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    path = payload.get("runtimeFile")
    if not path:
        return None
    trajectory_file = Path(str(path)).expanduser()
    return trajectory_file if trajectory_file.exists() else None


def transcript_entry(item: dict[str, Any]) -> dict[str, Any] | None:
    if item.get("type") == "session":
        return {
            "timestamp": item.get("timestamp"),
            "role": "system",
            "kind": "session",
            "title": "Session started",
            "text": f"cwd={item.get('cwd') or ''}",
        }
    if item.get("type") != "message":
        return None
    message = item.get("message")
    if not isinstance(message, dict):
        return None
    role = str(message.get("role") or "unknown")
    content = message.get("content")
    kind = role
    title = role
    text = ""
    if isinstance(content, str):
        text = content
    elif isinstance(content, list):
        parts = []
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = str(block.get("type") or "")
            if block_type == "toolCall":
                name = block.get("name") or "tool"
                kind = "tool_call"
                title = f"Tool call: {name}"
                args = block.get("arguments") or block.get("input") or {}
                parts.append(json.dumps(args, ensure_ascii=False, indent=2, sort_keys=True))
            elif block_type == "toolResult":
                name = block.get("toolName") or block.get("name") or "tool"
                kind = "tool_result"
                title = f"Tool result: {name}"
                parts.append(str(block.get("content") or block.get("text") or ""))
            elif "text" in block:
                parts.append(str(block.get("text") or ""))
            elif "content" in block:
                parts.append(str(block.get("content") or ""))
        text = "\n\n".join(part for part in parts if part)
    if not text:
        text = str(message.get("content") or "")
    text = redact_event_detail(text)
    if len(text) > 6000:
        text = text[:6000] + "\n... [truncated]"
    return {
        "timestamp": item.get("timestamp") or message.get("timestamp"),
        "role": role,
        "kind": kind,
        "title": title,
        "text": text,
    }


def transcript_entry_from_trajectory(item: dict[str, Any]) -> dict[str, Any] | None:
    event_type = str(item.get("type") or "")
    timestamp = item.get("ts")
    data = item.get("data")
    if not isinstance(data, dict):
        return None
    if event_type == "session.started":
        workspace = data.get("workspaceDir") or item.get("workspaceDir") or ""
        return {
            "timestamp": timestamp,
            "role": "system",
            "kind": "trajectory_session",
            "title": "Session started",
            "text": redact_event_detail(f"workspace={workspace}"),
        }
    if event_type == "tool.call":
        name = str(data.get("name") or "tool")
        arguments = data.get("arguments") if isinstance(data.get("arguments"), dict) else {}
        text = json.dumps(arguments, ensure_ascii=False, indent=2, sort_keys=True)
        return {
            "timestamp": timestamp,
            "role": "assistant",
            "kind": "tool_call",
            "title": f"Tool call: {name}",
            "text": _bounded_transcript_text(text),
        }
    if event_type == "tool.result":
        name = str(data.get("name") or "tool")
        output = data.get("output")
        result = data.get("result")
        if output is None and isinstance(result, dict):
            output = result.get("output") or result.get("stdout") or result.get("stderr")
        text = str(output or "")
        status = str(data.get("status") or (result.get("status") if isinstance(result, dict) else "") or "completed")
        return {
            "timestamp": timestamp,
            "role": "tool",
            "kind": "tool_result",
            "title": f"Tool result: {name}",
            "text": _bounded_transcript_text(f"status={status}\n{text}".strip()),
        }
    return None


def transcript_entry_from_session_event(event: dict[str, Any]) -> dict[str, Any] | None:
    event_type = str(event.get("event_type") or "")
    if event_type not in {"openclaw_stdout", "openclaw_stderr"}:
        return None
    detail = redact_event_detail(event.get("detail"))
    if not detail:
        return None
    title = "OpenClaw stdout" if event_type == "openclaw_stdout" else "OpenClaw stderr"
    return {
        "timestamp": event.get("ts"),
        "role": "assistant",
        "kind": event_type,
        "title": title,
        "text": detail,
    }


def _bounded_transcript_text(text: str) -> str:
    text = redact_event_detail(text)
    if len(text) > 6000:
        return text[:6000] + "\n... [truncated]"
    return text


def metrics_summary(
    db: str | Path,
    *,
    timezone_name: str = "UTC",
    window: str = "7d",
    runtime_window: str = "7d",
    now: datetime | None = None,
) -> dict[str, Any]:
    path = Path(db).expanduser()
    timezone = dashboard_timezone(timezone_name)
    window_start, window_end = slo_window_bounds(window, now=now)
    if runtime_window not in RUNTIME_WINDOWS:
        raise ValueError(
            f"unsupported runtime window {runtime_window!r}; expected one of {', '.join(RUNTIME_WINDOWS)}"
        )
    actor_run_rows: list[sqlite3.Row] = []
    if not path.exists():
        return empty_metrics_summary(
            db_exists=False,
            window=window,
            window_start=window_start,
            window_end=window_end,
            timezone=timezone,
        )
    with readonly_connect(path, operation="dashboard.metrics_summary") as con:
        try:
            validate_migrations(con)
            rows = con.execute("SELECT status, repo, action, work_intent, created_at, started_at, finished_at FROM jobs").fetchall()
            run_rows = con.execute(
                """SELECT job_runs.started_at, job_runs.finished_at,
                          jobs.work_intent, jobs.trigger_actor,
                          jobs.trigger_actor_avatar_url
                FROM job_runs
                JOIN jobs ON jobs.id=job_runs.job_id
                WHERE job_runs.finished_at IS NOT NULL"""
            ).fetchall()
            actor_run_rows = runtime_rows_in_window(
                run_rows,
                window=runtime_window,
                now=now,
            )
            slo_rows = con.execute(
                SLO_METRICS_SQL,
                (
                    format_utc(window_start),
                    format_utc(window_end),
                    format_utc(window_start),
                    format_utc(window_end),
                    format_utc(window_start),
                    format_utc(window_end),
                ),
            ).fetchall()
        except (MigrationError, sqlite3.OperationalError) as exc:
            return empty_metrics_summary(
                db_exists=True,
                window=window,
                window_start=window_start,
                window_end=window_end,
                timezone=timezone,
                schema_error=str(exc),
            )
    status_counts = Counter(row["status"] for row in rows)
    by_repo = Counter(row["repo"] or "unknown" for row in rows)
    by_action = Counter(row["action"] for row in rows)
    by_intent = Counter(row["work_intent"] for row in rows)
    by_created_day = Counter(day for day in (created_day(row["created_at"], timezone) for row in rows) if day)
    runtimes = sorted(
        seconds for seconds in (completed_duration_seconds(row["started_at"], row["finished_at"]) for row in run_rows) if seconds is not None
    )
    waits = sorted(
        seconds for seconds in (duration_seconds(row["created_at"], row["started_at"]) for row in rows if row["started_at"]) if seconds is not None
    )
    return {
        "db_exists": True,
        "schema_ok": True,
        "status_counts": dict(status_counts),
        "by_repo": dict(by_repo),
        "by_action": dict(by_action),
        "by_intent": dict(by_intent),
        "by_created_day": dict(sorted(by_created_day.items())),
        "runtime_usage": runtime_usage(run_rows, timezone),
        "runtime_usage_by_actor": runtime_usage_by_actor(actor_run_rows),
        "runtime_seconds": percentiles(runtimes),
        "queue_wait_seconds": percentiles(waits),
        "slo": slo_summary(
            slo_rows,
            window=window,
            window_start=window_start,
            window_end=window_end,
            timezone=timezone,
        ),
    }


def empty_metrics_summary(
    *,
    db_exists: bool,
    window: str,
    window_start: datetime,
    window_end: datetime,
    timezone: ZoneInfo,
    schema_error: str | None = None,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "db_exists": db_exists,
        "status_counts": {},
        "by_repo": {},
        "by_action": {},
        "by_intent": {},
        "by_created_day": {},
        "runtime_usage": {"day": [], "month": []},
        "runtime_usage_by_actor": [],
        "runtime_seconds": percentiles([]),
        "queue_wait_seconds": percentiles([]),
        "slo": slo_summary(
            [],
            window=window,
            window_start=window_start,
            window_end=window_end,
            timezone=timezone,
        ),
    }
    if schema_error is not None:
        summary.update({"schema_ok": False, "schema_error": schema_error})
    return summary


def slo_window_bounds(
    window: str,
    *,
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    duration = SLO_WINDOWS.get(window)
    if duration is None:
        raise ValueError(
            f"unsupported SLO window {window!r}; expected one of {', '.join(SLO_WINDOWS)}"
        )
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    current = current.astimezone(UTC).replace(microsecond=0)
    return current - duration, current


def runtime_rows_in_window(
    rows: list[sqlite3.Row],
    *,
    window: str,
    now: datetime | None = None,
) -> list[sqlite3.Row]:
    if window not in RUNTIME_WINDOWS:
        raise ValueError(
            f"unsupported runtime window {window!r}; expected one of {', '.join(RUNTIME_WINDOWS)}"
        )
    duration = RUNTIME_WINDOWS[window]
    if duration is None:
        return rows
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    current = current.astimezone(UTC).replace(microsecond=0)
    window_start = current - duration
    return [
        row
        for row in rows
        if (finished := parse_utc(row["finished_at"])) is not None
        and window_start <= finished <= current
    ]


def format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def slo_summary(
    rows: list[sqlite3.Row],
    *,
    window: str,
    window_start: datetime,
    window_end: datetime,
    timezone: ZoneInfo,
) -> dict[str, Any]:
    service_candidates = [row for row in rows if row["record_type"] == "service"]
    service_rows = [
        row
        for row in service_candidates
        if row["terminal_outcome"] in SERVICE_TERMINAL_OUTCOMES
    ]
    cohort_missing = sum(
        row["terminal_outcome"] is None for row in service_candidates
    )
    cohort_excluded = len(service_candidates) - len(service_rows) - cohort_missing

    latency_clocks = {
        "source_received_to_queued": ("source_received_at", "created_at"),
        "queued_to_claimed": ("created_at", "first_claimed_at"),
        "claimed_to_first_output": ("first_claimed_at", "first_visible_at"),
        "claimed_to_done": ("first_claimed_at", "finished_at"),
    }
    latencies: dict[str, dict[str, int | str | None]] = {}
    for name, (start_key, end_key) in latency_clocks.items():
        values = [
            value
            for value in (
                slo_duration_seconds(row[start_key], row[end_key])
                for row in service_rows
            )
            if value is not None
        ]
        latencies[name] = slo_latency_summary(values, len(service_rows))

    blocked = sum(row["terminal_outcome"] == "blocked" for row in service_rows)
    no_op = sum(row["terminal_outcome"] == "no_op" for row in service_rows)
    claimed = sum(int(row["run_count"]) > 0 for row in service_rows)
    retried = sum(int(row["run_count"]) > 1 for row in service_rows)
    requeued_runs = sum(int(row["requeued_runs"]) for row in service_rows)
    new_jobs = sum(row["record_type"] == "new_job" for row in rows)
    coalesced = sum(row["record_type"] == "coalesced" for row in rows)

    trends = slo_daily_trends(
        rows,
        window_start=window_start,
        window_end=window_end,
        timezone=timezone,
    )
    return {
        "window": {
            "value": window,
            "start": format_utc(window_start),
            "end": format_utc(window_end),
            "timezone": str(timezone),
        },
        "cohort": {
            "decision": "auto_trusted",
            "terminal_outcomes": list(SERVICE_TERMINAL_OUTCOMES),
            "sample_count": len(service_rows),
            "missing_count": cohort_missing,
            "excluded_count": cohort_excluded,
        },
        "latencies": latencies,
        "rates": {
            "blocked": slo_rate(blocked, len(service_rows)),
            "no_op": slo_rate(no_op, len(service_rows)),
            "retry": slo_rate(
                retried,
                claimed,
                missing_count=len(service_rows) - claimed,
                extra={"requeued_runs": requeued_runs},
            ),
            "coalescing": slo_rate(coalesced, new_jobs + coalesced),
        },
        "trends": {
            "bucket": "day",
            "timezone": str(timezone),
            "daily": trends,
        },
    }


def slo_duration_seconds(start: str | None, end: str | None) -> int | None:
    started = parse_utc(start)
    finished = parse_utc(end)
    if started is None or finished is None or finished < started:
        return None
    return int((finished - started).total_seconds())


def slo_latency_summary(
    values: list[int],
    cohort_count: int,
) -> dict[str, int | str | None]:
    values.sort()
    return {
        "unit": "seconds",
        "p50": nearest_rank(values, 0.50) if values else None,
        "p90": nearest_rank(values, 0.90) if values else None,
        "p99": nearest_rank(values, 0.99) if values else None,
        "sample_count": len(values),
        "missing_count": cohort_count - len(values),
    }


def slo_rate(
    numerator: int,
    denominator: int,
    *,
    missing_count: int = 0,
    extra: dict[str, int] | None = None,
) -> dict[str, int | float | None]:
    rate = numerator / denominator if denominator else None
    return {
        "rate": round(rate, 4) if rate is not None else None,
        "percent": round(rate * 100, 2) if rate is not None else None,
        "numerator": numerator,
        "denominator": denominator,
        "sample_count": denominator,
        "missing_count": missing_count,
        **(extra or {}),
    }


def slo_daily_trends(
    rows: list[sqlite3.Row],
    *,
    window_start: datetime,
    window_end: datetime,
    timezone: ZoneInfo,
) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, int]] = {}
    current_day = window_start.astimezone(timezone).date()
    end_day = window_end.astimezone(timezone).date()
    while current_day <= end_day:
        buckets[current_day.isoformat()] = slo_trend_counts()
        current_day += timedelta(days=1)

    for row in rows:
        record_type = row["record_type"]
        timestamp = row["finished_at"] if record_type == "service" else row["event_at"]
        bucket = created_day(timestamp, timezone)
        if bucket is None:
            continue
        counts = buckets.setdefault(bucket, slo_trend_counts())
        if record_type == "service":
            outcome = row["terminal_outcome"]
            if outcome in SERVICE_TERMINAL_OUTCOMES:
                counts["cohort"] += 1
                counts["blocked"] += outcome == "blocked"
                counts["no_op"] += outcome == "no_op"
                if int(row["run_count"]) > 0:
                    counts["claimed"] += 1
                    counts["retried"] += int(row["run_count"]) > 1
                counts["requeued_runs"] += int(row["requeued_runs"])
            elif outcome is None:
                counts["cohort_missing"] += 1
            else:
                counts["cohort_excluded"] += 1
        elif record_type == "new_job":
            counts["new_jobs"] += 1
        elif record_type == "coalesced":
            counts["coalesced"] += 1

    return [
        {
            "bucket": bucket,
            "cohort": {
                "sample_count": counts["cohort"],
                "missing_count": counts["cohort_missing"],
                "excluded_count": counts["cohort_excluded"],
            },
            "rates": {
                "blocked": slo_rate(counts["blocked"], counts["cohort"]),
                "no_op": slo_rate(counts["no_op"], counts["cohort"]),
                "retry": slo_rate(
                    counts["retried"],
                    counts["claimed"],
                    missing_count=counts["cohort"] - counts["claimed"],
                    extra={"requeued_runs": counts["requeued_runs"]},
                ),
                "coalescing": slo_rate(
                    counts["coalesced"],
                    counts["new_jobs"] + counts["coalesced"],
                ),
            },
        }
        for bucket, counts in sorted(buckets.items())
    ]


def slo_trend_counts() -> dict[str, int]:
    return {
        "cohort": 0,
        "cohort_missing": 0,
        "cohort_excluded": 0,
        "blocked": 0,
        "no_op": 0,
        "claimed": 0,
        "retried": 0,
        "requeued_runs": 0,
        "new_jobs": 0,
        "coalesced": 0,
    }


def dashboard_timezone(name: str | None) -> ZoneInfo:
    if not name:
        return ZoneInfo("UTC")
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def created_day(value: str | None, timezone: ZoneInfo = ZoneInfo("UTC")) -> str | None:
    created = parse_utc(value)
    if created is None:
        return None
    return created.astimezone(timezone).date().isoformat()


def _knowledge_json_object(raw: str | None) -> dict[str, Any]:
    try:
        value = json.loads(raw or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _knowledge_json_list(raw: str | None) -> list[str]:
    try:
        value = json.loads(raw or "[]")
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item).strip()]


def _knowledge_scope_clause(
    repo: str,
    *,
    applicable_rules: bool = False,
) -> tuple[str, list[str]]:
    normalized = repo.strip().lower()
    if not normalized:
        return "", []
    repo_scope = f"repo:{normalized}"
    scopes = [repo_scope]
    if applicable_rules and "/" in normalized:
        owner = normalized.split("/", 1)[0]
        scopes = ["global", f"org:{owner}", repo_scope]
    clauses = []
    args: list[str] = []
    for scope in scopes:
        clauses.append("(scope=? OR scope LIKE ?)")
        args.extend([scope, f"{scope}:%"])
    return " WHERE " + " OR ".join(clauses), args


def _knowledge_event_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "occurred_at": str(row["occurred_at"]),
        "captured_at": str(row["captured_at"]),
        "source": str(row["source"]),
        "scope": str(row["scope"]),
        "actor": str(row["actor"]),
        "comment": str(row["comment"]),
        "context": _knowledge_json_object(row["context_json"]),
        "classification": str(row["classification"]),
        "confidence": float(row["confidence"]),
        "memorable": bool(row["memorable"]),
    }


def _knowledge_source_from_context(context: dict[str, Any]) -> dict[str, Any]:
    github_context = (
        context.get("github_context")
        if isinstance(context.get("github_context"), dict)
        else {}
    )
    urls = context.get("github_urls")
    if not isinstance(urls, list):
        urls = github_context.get("urls")
    clean_urls = [
        str(url)
        for url in (urls if isinstance(urls, list) else [])
        if isinstance(url, str) and url.strip()
    ]
    return {
        "trigger_actor": context.get("trigger_actor"),
        "trigger_actor_avatar_url": context.get("trigger_actor_avatar_url"),
        "github_urls": clean_urls,
        "source_url": context.get("source_url")
        or (clean_urls[0] if clean_urls else None),
        "github_context": github_context,
        "source_job_id": context.get("source_job_id"),
        "source_table": context.get("source_table"),
    }


def _knowledge_source_from_row(row: sqlite3.Row) -> dict[str, Any]:
    github_context = _knowledge_json_object(row["context_json"])
    urls = github_context.get("urls")
    clean_urls = [
        str(url)
        for url in (urls if isinstance(urls, list) else [])
        if isinstance(url, str) and url.strip()
    ]
    return {
        "trigger_actor": row["trigger_actor"],
        "trigger_actor_avatar_url": row["trigger_actor_avatar_url"],
        "github_urls": clean_urls,
        "source_url": clean_urls[0] if clean_urls else None,
        "github_context": github_context,
        "source_job_id": row["source_job_id"],
        "source_table": row["source_table"],
    }


def _knowledge_fallback_source(comment: str) -> dict[str, Any]:
    context = _knowledge_json_object(extract_github_context(comment).to_json())
    urls = context.get("urls")
    clean_urls = [
        str(url)
        for url in (urls if isinstance(urls, list) else [])
        if isinstance(url, str) and url.strip()
    ]
    return {
        "github_urls": clean_urls,
        "source_url": clean_urls[0] if clean_urls else None,
        "github_context": context,
        "source_table": "feedback_comment" if clean_urls else None,
    }


def _knowledge_first(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", []):
            return value
    return None


def _enrich_knowledge_event(
    event: dict[str, Any],
    sources_by_message_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    context = event["context"] if isinstance(event.get("context"), dict) else {}
    stored = _knowledge_source_from_context(context)
    joined = sources_by_message_id.get(str(context.get("message_id") or ""), {})
    fallback = _knowledge_fallback_source(str(event.get("comment") or ""))
    urls = _knowledge_first(
        stored.get("github_urls"),
        joined.get("github_urls"),
        fallback.get("github_urls"),
    ) or []
    trigger_actor = _knowledge_first(
        stored.get("trigger_actor"),
        joined.get("trigger_actor"),
        event.get("actor") if event.get("actor") != "github" else None,
    )
    enriched = {
        **event,
        "trigger_actor": trigger_actor,
        "trigger_actor_avatar_url": _knowledge_first(
            stored.get("trigger_actor_avatar_url"),
            joined.get("trigger_actor_avatar_url"),
        ),
        "github_urls": urls,
        "source_url": _knowledge_first(
            stored.get("source_url"),
            joined.get("source_url"),
            fallback.get("source_url"),
        ),
        "github_context": _knowledge_first(
            stored.get("github_context"),
            joined.get("github_context"),
            fallback.get("github_context"),
        ) or {},
        "source_job_id": _knowledge_first(
            stored.get("source_job_id"),
            joined.get("source_job_id"),
        ),
        "source_table": _knowledge_first(
            stored.get("source_table"),
            joined.get("source_table"),
            fallback.get("source_table"),
        ),
    }
    if trigger_actor and enriched.get("actor") == "github":
        enriched["actor"] = trigger_actor
    return enriched


def _knowledge_summary(
    con: sqlite3.Connection,
    repo: str,
) -> dict[str, int]:
    event_where, event_args = _knowledge_scope_clause(repo)
    rule_where, rule_args = _knowledge_scope_clause(
        repo,
        applicable_rules=True,
    )
    proposal_where, proposal_args = _knowledge_scope_clause(repo)
    row = con.execute(
        f"""WITH event_summary AS (
            SELECT COUNT(*) AS events FROM feedback_events{event_where}
        ), rule_summary AS (
            SELECT COUNT(*) AS rules FROM feedback_rules{rule_where}
        ), proposal_summary AS (
            SELECT
                COUNT(*) AS proposals,
                COALESCE(SUM(status='proposed'), 0) AS proposed,
                COALESCE(SUM(status='approved'), 0) AS approved,
                COALESCE(SUM(status='rejected'), 0) AS rejected,
                COALESCE(SUM(status='error'), 0) AS errors
            FROM feedback_rule_proposals{proposal_where}
        )
        SELECT * FROM event_summary, rule_summary, proposal_summary""",
        [*event_args, *rule_args, *proposal_args],
    ).fetchone()
    return {key: int(row[key]) for key in row.keys()}


def _knowledge_repositories(con: sqlite3.Connection) -> list[str]:
    rows = con.execute(
        """SELECT scope FROM feedback_events
        UNION SELECT scope FROM feedback_rules
        UNION SELECT scope FROM feedback_rule_proposals"""
    ).fetchall()
    return sorted(
        {
            str(row["scope"]).removeprefix("repo:")
            for row in rows
            if str(row["scope"]).startswith("repo:")
            and str(row["scope"]).removeprefix("repo:")
        }
    )


def _knowledge_section_rows(
    con: sqlite3.Connection,
    section: str,
    *,
    repo: str,
    proposal_status: str,
    limit: int,
    offset: int,
) -> tuple[list[sqlite3.Row], int | None]:
    applicable_rules = section == "rules"
    where, args = _knowledge_scope_clause(
        repo,
        applicable_rules=applicable_rules,
    )
    clauses = [where.removeprefix(" WHERE ")] if where else []
    table = {
        "events": "feedback_events",
        "proposals": "feedback_rule_proposals",
        "rules": "feedback_rules",
    }[section]
    if section == "proposals" and proposal_status:
        clauses.append("status=?")
        args.append(proposal_status)
    order_by = {
        "events": "occurred_at DESC, id DESC",
        "proposals": "created_at DESC, id DESC",
        "rules": "last_seen DESC, created_at DESC, scope ASC, type ASC, rule ASC",
    }[section]
    sql = f"SELECT * FROM {table}"
    if clauses:
        sql += " WHERE " + " AND ".join(f"({clause})" for clause in clauses)
    sql += f" ORDER BY {order_by} LIMIT ? OFFSET ?"
    rows = con.execute(sql, [*args, limit + 1, offset]).fetchall()
    next_offset = offset + limit if len(rows) > limit else None
    return rows[:limit], next_offset


def _knowledge_source_rows(
    con: sqlite3.Connection,
    message_ids: list[str],
) -> dict[str, dict[str, Any]]:
    rows = con.execute(
        """WITH requested(message_id) AS (
            SELECT value FROM json_each(?)
        ), candidates AS (
            SELECT jobs.message_id,
                   jobs.id AS source_record_id,
                   jobs.id AS source_job_id,
                   jobs.trigger_actor,
                   jobs.trigger_actor_avatar_url,
                   jobs.context_json,
                   'job' AS source_table,
                   0 AS source_rank
            FROM jobs
            JOIN requested ON requested.message_id=jobs.message_id
            UNION ALL
            SELECT coalesced_notifications.message_id,
                   coalesced_notifications.id AS source_record_id,
                   coalesced_notifications.job_id AS source_job_id,
                   coalesced_notifications.trigger_actor,
                   coalesced_notifications.trigger_actor_avatar_url,
                   coalesced_notifications.context_json,
                   'coalesced_notification' AS source_table,
                   1 AS source_rank
            FROM coalesced_notifications
            JOIN requested
              ON requested.message_id=coalesced_notifications.message_id
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY message_id
                ORDER BY source_rank ASC, source_record_id DESC
            ) AS source_position
            FROM candidates
        )
        SELECT * FROM ranked WHERE source_position=1""",
        (json.dumps(message_ids),),
    ).fetchall()
    return {
        str(row["message_id"]): _knowledge_source_from_row(row)
        for row in rows
    }


def _knowledge_rule_dict(
    row: sqlite3.Row,
    events_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    source_events = _knowledge_json_list(row["source_events_json"])
    return {
        "id": str(row["id"]),
        "scope": str(row["scope"]),
        "type": str(row["type"]),
        "confidence": float(row["confidence"]),
        "rule": str(row["rule"]),
        "created_at": str(row["created_at"]),
        "last_seen": str(row["last_seen"]),
        "source_events": source_events,
        "source_event_details": [
            events_by_id[event_id]
            for event_id in source_events
            if event_id in events_by_id
        ],
        "observations": int(row["observations"]),
    }


def _knowledge_events_by_id(
    con: sqlite3.Connection,
    event_ids: list[str],
) -> dict[str, dict[str, Any]]:
    event_rows = con.execute(
        "SELECT * FROM feedback_events "
        "WHERE id IN (SELECT value FROM json_each(?))",
        (json.dumps(event_ids),),
    ).fetchall()
    events = [_knowledge_event_from_row(row) for row in event_rows]
    message_ids = list(
        dict.fromkeys(
            str(event["context"].get("message_id") or "")
            for event in events
            if event["context"].get("message_id")
        )
    )
    sources = _knowledge_source_rows(con, message_ids)
    return {
        str(event["id"]): _enrich_knowledge_event(event, sources)
        for event in events
    }


def knowledge_rule(
    db: str | Path,
    rule_id: str,
) -> dict[str, Any] | None:
    with readonly_connect(db, operation="dashboard.knowledge_rule") as con:
        row = con.execute(
            "SELECT * FROM feedback_rules WHERE id=?",
            (rule_id,),
        ).fetchone()
        if row is None:
            return None
        source_events = _knowledge_json_list(row["source_events_json"])
        events_by_id = _knowledge_events_by_id(con, source_events)
    return _knowledge_rule_dict(row, events_by_id)


def knowledge_page(
    db: str | Path,
    *,
    section: str | None = None,
    repo: str = "",
    proposal_status: str = "",
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    selected_sections = [section] if section else ["proposals", "rules", "events"]
    if any(item not in {"proposals", "rules", "events"} for item in selected_sections):
        raise ValueError("unsupported knowledge section")
    bounded_limit = coerce_limit(limit, 100)
    bounded_offset = max(0, offset)
    page_rows: dict[str, list[sqlite3.Row]] = {
        "proposals": [],
        "rules": [],
        "events": [],
    }
    next_offset: int | None = None
    with readonly_connect(db, operation="dashboard.knowledge") as con:
        summary = _knowledge_summary(con, repo)
        repositories = _knowledge_repositories(con)
        for selected in selected_sections:
            rows, selected_next_offset = _knowledge_section_rows(
                con,
                selected,
                repo=repo,
                proposal_status=proposal_status,
                limit=bounded_limit,
                offset=bounded_offset,
            )
            page_rows[selected] = rows
            if section == selected:
                next_offset = selected_next_offset

        event_ids: list[str] = []
        event_ids.extend(str(row["id"]) for row in page_rows["events"])
        event_ids.extend(str(row["event_id"]) for row in page_rows["proposals"])
        for row in page_rows["rules"]:
            event_ids.extend(_knowledge_json_list(row["source_events_json"]))
        event_ids = list(dict.fromkeys(event_ids))
        events_by_id = _knowledge_events_by_id(con, event_ids)
    proposals = [
        {
            "id": str(row["id"]),
            "event_id": str(row["event_id"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
            "status": str(row["status"]),
            "scope": str(row["scope"]),
            "type": str(row["type"]),
            "confidence": float(row["confidence"]),
            "rule": str(row["rule"]),
            "reason": str(row["reason"]),
            "model": str(row["model"]),
            "error": row["error"],
            "source_event": events_by_id.get(str(row["event_id"])),
        }
        for row in page_rows["proposals"]
    ]
    rules = [
        _knowledge_rule_dict(row, events_by_id)
        for row in page_rows["rules"]
    ]
    selected_events = [
        events_by_id[str(row["id"])]
        for row in page_rows["events"]
        if str(row["id"]) in events_by_id
    ]
    return {
        "repositories": repositories,
        "events": selected_events,
        "proposals": proposals,
        "rules": rules,
        "summary": summary,
        "section": section,
        "next_offset": next_offset,
    }


def runtime_usage(rows: list[sqlite3.Row], timezone: ZoneInfo) -> dict[str, list[dict[str, Any]]]:
    daily: dict[str, dict[str, int]] = {}
    monthly: dict[str, dict[str, int]] = {}
    for row in rows:
        started = parse_utc(row["started_at"])
        finished = parse_utc(row["finished_at"])
        seconds = completed_duration_seconds(row["started_at"], row["finished_at"])
        if started is None or finished is None or seconds is None:
            continue
        day = majority_runtime_day(started, finished, timezone)
        if day is None:
            continue
        month = day[:7]
        mode = runtime_usage_mode(row["work_intent"])
        add_runtime_bucket(daily, day, seconds, mode)
        add_runtime_bucket(monthly, month, seconds, mode)
    return {
        "day": runtime_bucket_rows(daily),
        "month": runtime_bucket_rows(monthly),
    }


def runtime_usage_by_actor(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    users: dict[str, dict[str, Any]] = {}
    for row in rows:
        seconds = completed_duration_seconds(row["started_at"], row["finished_at"])
        if seconds is None:
            continue
        key = (str(row["trigger_actor"] or "").strip() or "unknown").casefold()
        avatar_url = str(row["trigger_actor_avatar_url"] or "").strip() or None
        mode = runtime_usage_mode(row["work_intent"])
        current = users.setdefault(
            key,
            {
                "actor": key,
                "avatar_url": avatar_url,
                "seconds": 0,
                "runs": 0,
                "work_seconds": 0,
                "review_seconds": 0,
                "work_runs": 0,
                "review_runs": 0,
            },
        )
        if current["avatar_url"] is None and avatar_url is not None:
            current["avatar_url"] = avatar_url
        current["seconds"] += seconds
        current["runs"] += 1
        current[f"{mode}_seconds"] += seconds
        current[f"{mode}_runs"] += 1
    return [
        {
            **values,
            "minutes": round(values["seconds"] / 60, 2),
        }
        for values in sorted(
            users.values(),
            key=lambda item: (-item["seconds"], item["actor"].casefold()),
        )
    ]


def majority_runtime_day(started: datetime, finished: datetime, timezone: ZoneInfo) -> str | None:
    """Return the local day containing most elapsed runtime, preferring the start day on ties."""
    if finished < started:
        return None
    start_day = started.astimezone(timezone).date()
    end_day = finished.astimezone(timezone).date()
    current_day = start_day
    best_day = start_day
    best_seconds = -1.0
    while current_day <= end_day:
        day_start = datetime.combine(current_day, time.min, tzinfo=timezone).astimezone(UTC)
        next_day = current_day + timedelta(days=1)
        day_end = datetime.combine(next_day, time.min, tzinfo=timezone).astimezone(UTC)
        overlap_start = max(started, day_start)
        overlap_end = min(finished, day_end)
        overlap_seconds = max(0.0, (overlap_end - overlap_start).total_seconds())
        if overlap_seconds > best_seconds:
            best_day = current_day
            best_seconds = overlap_seconds
        current_day = next_day
    return best_day.isoformat()


def runtime_usage_mode(work_intent: str | None) -> str:
    return "work" if work_intent == "work_allowed" else "review"


def add_runtime_bucket(buckets: dict[str, dict[str, int]], bucket: str, seconds: int, mode: str) -> None:
    current = buckets.setdefault(bucket, {"seconds": 0, "runs": 0, "jobs": 0, "work_seconds": 0, "review_seconds": 0, "work_runs": 0, "review_runs": 0, "work_jobs": 0, "review_jobs": 0})
    current["seconds"] += seconds
    current["runs"] += 1
    current["jobs"] += 1
    current[f"{mode}_seconds"] += seconds
    current[f"{mode}_runs"] += 1
    current[f"{mode}_jobs"] += 1


def runtime_bucket_rows(buckets: dict[str, dict[str, int]]) -> list[dict[str, Any]]:
    return [
        {
            "bucket": bucket,
            "seconds": values["seconds"],
            "minutes": round(values["seconds"] / 60, 2),
            "runs": values["runs"],
            "jobs": values["jobs"],
            "work_seconds": values["work_seconds"],
            "review_seconds": values["review_seconds"],
            "work_runs": values["work_runs"],
            "review_runs": values["review_runs"],
            "work_jobs": values["work_jobs"],
            "review_jobs": values["review_jobs"],
        }
        for bucket, values in sorted(buckets.items())
    ]


def percentiles(values: list[int]) -> dict[str, int | None]:
    if not values:
        return {"median": None, "p90": None, "p99": None}
    return {
        "median": nearest_rank(values, 0.50),
        "p90": nearest_rank(values, 0.90),
        "p99": nearest_rank(values, 0.99),
    }


def nearest_rank(values: list[int], percentile: float) -> int:
    index = max(0, min(len(values) - 1, int(round(percentile * (len(values) - 1)))))
    return values[index]


def _latest_job_progress(con: sqlite3.Connection, job_id: int, kind: str | None = None) -> dict[str, Any] | None:
    where = "job_id=?"
    args: list[Any] = [job_id]
    if kind:
        where += " AND kind=?"
        args.append(kind)
    row = con.execute(
        f"SELECT id, ts, kind, phase, summary, detail FROM job_progress WHERE {where} ORDER BY id DESC LIMIT 1",
        args,
    ).fetchone()
    if row is None:
        return None
    progress = dict(row)
    progress["age_seconds"] = duration_seconds(progress.get("ts"))
    return progress


class DashboardQueries:
    """Read-only dashboard persistence boundary.

    The compatibility functions in this module remain as a public API shim, but
    HTTP handlers and services depend on this object so connection policy,
    filters, SQL, and row mapping stay outside application orchestration.
    """

    def __init__(self, db: str | Path) -> None:
        self.path = Path(db).expanduser()

    def status(self) -> dict[str, Any]:
        return inspect_db_read_only(self.path)

    def list_jobs(
        self,
        filters: JobListFilters | None = None,
        *,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        selected = filters or JobListFilters()
        return list_jobs(
            self.path,
            status_filter=selected.status,
            repo=selected.repo,
            thread=selected.thread,
            action=selected.action,
            intent=selected.intent,
            actor=selected.actor,
            since=selected.since,
            until=selected.until,
            limit=limit,
        )

    def knowledge_page(
        self,
        *,
        section: str | None = None,
        repo: str = "",
        proposal_status: str = "",
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        return knowledge_page(
            self.path,
            section=section,
            repo=repo,
            proposal_status=proposal_status,
            limit=limit,
            offset=offset,
        )

    def get_knowledge_rule(self, rule_id: str) -> dict[str, Any] | None:
        return knowledge_rule(self.path, rule_id)

    def list_job_actors(self, *, limit: int = 100) -> list[dict[str, Any]]:
        return list_job_actors(self.path, limit=limit)

    def list_all_job_actor_logins(self) -> list[str]:
        return list_all_job_actor_logins(self.path)

    def get_job_detail(self, job_id: int) -> dict[str, Any] | None:
        return get_job_detail(self.path, job_id)

    def job_logs(self, job_id: int, *, limit: int = 100) -> list[dict[str, Any]]:
        return job_logs(self.path, job_id, limit=limit)

    def latest_job_progress(
        self,
        job_id: int,
        kind: str | None = None,
    ) -> dict[str, Any] | None:
        return latest_job_progress(self.path, job_id, kind)

    def job_session(self, job_id: int) -> dict[str, Any] | None:
        return job_session(self.path, job_id)

    def job_session_events(
        self,
        job_id: int,
        *,
        after_id: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return job_session_events(
            self.path,
            job_id,
            after_id=after_id,
            limit=limit,
        )

    def job_session_transcript(
        self,
        job_id: int,
        *,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        return job_session_transcript(self.path, job_id, limit=limit)

    def metrics_summary(
        self,
        *,
        timezone_name: str = "UTC",
        window: str = "7d",
        runtime_window: str = "7d",
    ) -> dict[str, Any]:
        return metrics_summary(
            self.path,
            timezone_name=timezone_name,
            window=window,
            runtime_window=runtime_window,
        )
