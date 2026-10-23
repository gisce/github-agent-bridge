from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from importlib import metadata
from pathlib import Path
from typing import Any

from .dashboard_data import DashboardQueries
from .observability import (
    DEFAULT_PROCESS_SAMPLE_RETENTION_SECONDS,
    get_monitor_slo_state,
    recent_process_samples,
    record_monitor_observation,
)
from .process_inspection import direct_children, process_identity_matches


ALERT_RELEASE_AVAILABLE = "monitor.release_available"
ALERT_DATABASE_MISSING = "monitor.database_missing"
ALERT_SCHEMA_MISSING = "monitor.schema_missing"
ALERT_BLOCKED_JOBS = "monitor.blocked_jobs"
ALERT_PENDING_QUEUE_OLD = "monitor.pending_queue_old"
ALERT_EXECUTOR_SERVICE = "monitor.executor_service"
ALERT_RUNNING_NO_EXECUTOR_CHILD = "monitor.running_no_executor_child"
ALERT_RUNNING_PROCESS_MISMATCH = "monitor.running_process_mismatch"
ALERT_READER_TIMER = "monitor.reader_timer"
ALERT_READER_LAST_RESULT = "monitor.reader_last_result"
ALERT_READER_STALE = "monitor.reader_stale"
ALERT_RUNNING_JOB_STALLED = "monitor.running_job_stalled"
ALERT_WORKER_HEARTBEAT_MISSING = "monitor.worker_heartbeat_missing"
ALERT_WORKER_HEARTBEAT_STALE = "monitor.worker_heartbeat_stale"
ALERT_QUARANTINED_NOTIFICATIONS = "monitor.quarantined_notifications"
ALERT_SLO_QUEUED_TO_CLAIMED_P90 = "monitor.slo.queued_to_claimed_p90"
ALERT_SLO_CLAIMED_TO_FIRST_OUTPUT_P90 = "monitor.slo.claimed_to_first_output_p90"
ALERT_SLO_CLAIMED_TO_DONE_P90 = "monitor.slo.claimed_to_done_p90"
ALERT_SLO_BLOCKED_RATE = "monitor.slo.blocked_rate"
ALERT_SLO_RETRY_RATE = "monitor.slo.retry_rate"


SLO_WINDOWS = {"24h", "7d", "30d"}
SLO_THRESHOLD_FIELDS = (
    "slo_queued_to_claimed_p90_seconds",
    "slo_claimed_to_first_output_p90_seconds",
    "slo_claimed_to_done_p90_seconds",
    "slo_blocked_rate_percent",
    "slo_retry_rate_percent",
)


@dataclass(frozen=True)
class MonitorThresholds:
    pending_warn_seconds: int = 300
    review_running_warn_seconds: int = 1200
    work_running_warn_seconds: int = 4200
    progress_warn_seconds: int = 600
    reader_recent_seconds: int = 180
    worker_heartbeat_warn_seconds: int = 20
    slo_window: str = "7d"
    slo_min_samples: int = 20
    slo_breach_observations: int = 3
    slo_recovery_observations: int = 2
    slo_recovery_ratio: float = 0.9
    slo_queued_to_claimed_p90_seconds: float | None = None
    slo_claimed_to_first_output_p90_seconds: float | None = None
    slo_claimed_to_done_p90_seconds: float | None = None
    slo_blocked_rate_percent: float | None = None
    slo_retry_rate_percent: float | None = None

    def __post_init__(self) -> None:
        if self.slo_window not in SLO_WINDOWS:
            raise ValueError(f"unsupported SLO window {self.slo_window!r}")
        if self.slo_min_samples < 1:
            raise ValueError("slo_min_samples must be at least 1")
        if self.slo_breach_observations < 1:
            raise ValueError("slo_breach_observations must be at least 1")
        if self.slo_recovery_observations < 1:
            raise ValueError("slo_recovery_observations must be at least 1")
        if not 0 < self.slo_recovery_ratio <= 1:
            raise ValueError("slo_recovery_ratio must be greater than 0 and at most 1")
        for field_name in SLO_THRESHOLD_FIELDS:
            value = getattr(self, field_name)
            if value is not None and value < 0:
                raise ValueError(f"{field_name} must be non-negative")
        for field_name in ("slo_blocked_rate_percent", "slo_retry_rate_percent"):
            value = getattr(self, field_name)
            if value is not None and value > 100:
                raise ValueError(f"{field_name} must be at most 100")

    @property
    def slo_enabled(self) -> bool:
        return any(getattr(self, field_name) is not None for field_name in SLO_THRESHOLD_FIELDS)

    def slo_config(self) -> dict[str, Any]:
        return {
            "window": self.slo_window,
            "min_samples": self.slo_min_samples,
            "breach_observations": self.slo_breach_observations,
            "recovery_observations": self.slo_recovery_observations,
            "recovery_ratio": self.slo_recovery_ratio,
            "thresholds": {
                field_name: getattr(self, field_name)
                for field_name in SLO_THRESHOLD_FIELDS
                if getattr(self, field_name) is not None
            },
        }


@dataclass
class MonitorReport:
    ok: bool
    alerts: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "alerts": self.alerts, "metrics": self.metrics}

    def text(self) -> str:
        status = "OK" if self.ok else "ALERT"
        parts = [f"{status} github-agent-bridge"]
        metrics = self.metrics
        compact = [
            f"executor={metrics.get('executor_service', 'unknown')}",
            f"reader_timer={metrics.get('reader_timer', 'unknown')}",
            f"reader_recent={metrics.get('reader_recent', 'unknown')}",
            f"pending={metrics.get('pending', 0)}",
            f"blocked={metrics.get('blocked', 0)}",
            f"running={metrics.get('running', 0)}",
            f"workers={metrics.get('worker_heartbeats_live', 0)}/{metrics.get('executor_worker_count', 0)}",
            f"workers_busy={metrics.get('executor_worker_busy_count', 0)}",
            f"workers_draining={metrics.get('executor_worker_draining_count', 0)}",
            f"quarantined={metrics.get('quarantined_notifications', 0)}",
            f"oldest_pending={metrics.get('oldest_pending_age_seconds') if metrics.get('oldest_pending_age_seconds') is not None else '-'}",
            f"last_uid={metrics.get('last_uid', '-')}",
        ]
        parts.append(" ".join(compact))
        if self.alerts:
            alert_codes = metrics.get("alert_codes") or []
            for index, alert in enumerate(self.alerts):
                code = alert_codes[index] if index < len(alert_codes) else ""
                prefix = f"[{code}] " if code else ""
                parts.append(f"- {prefix}{alert}")
        for job in metrics.get("running_jobs", []):
            last = job.get("last_worklog") or {}
            semantic = job.get("semantic_progress") or {}
            visible = job.get("visible_progress") or {}
            parts.append(
                "- running detail: "
                f"job={job.get('id')} key={job.get('work_key')} "
                f"intent={job.get('work_intent')} attempts={job.get('attempts')} "
                f"worker={job.get('locked_by')} age={job.get('age_seconds')}s "
                f"idle={job.get('idle_seconds')}s "
                f"last={last.get('phase', '-')}/{last.get('summary', '-')} "
                f"semantic={semantic.get('phase', '-')}/{semantic.get('age_seconds', '-')}s "
                f"visible={visible.get('phase', '-')}/{visible.get('age_seconds', '-')}s"
            )
            runtime = job.get("runtime_process") or {}
            if runtime:
                parts.append(
                    "- runtime detail: "
                    f"job={job.get('id')} state={runtime.get('state', '-')} "
                    f"pid={runtime.get('pid', '-')} ppid={runtime.get('ppid', '-')} "
                    f"pgid={runtime.get('pgid', '-')} sid={runtime.get('sid', '-')}"
                )
        children = metrics.get("executor_children") or []
        if children:
            child_text = ", ".join(f"{child.get('pid')}:{child.get('cmd') or '-'}" for child in children[:5])
            parts.append(f"- executor children: {child_text}")
        return "\n".join(parts)


def _run_systemctl(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["systemctl", "--user", *args], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def _is_active(unit: str) -> str:
    proc = _run_systemctl(["is-active", unit])
    return proc.stdout.strip() or "unknown"


def _last_service_result(unit: str) -> tuple[str, str | None, int | None]:
    proc = _run_systemctl([
        "show",
        unit,
        "--property=Result,ExecMainStatus,InactiveEnterTimestampMonotonic,ActiveEnterTimestampMonotonic",
    ])
    props: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key] = value.strip()

    result = props.get("Result") or "unknown"
    exit_status = props.get("ExecMainStatus") or None
    finished_raw = props.get("InactiveEnterTimestampMonotonic") or props.get("ActiveEnterTimestampMonotonic")
    age_seconds: int | None = None
    try:
        finished_us = int(finished_raw or "0")
    except ValueError:
        finished_us = 0
    if finished_us > 0:
        now_us = time.monotonic_ns() // 1000
        age_seconds = max(0, int((now_us - finished_us) / 1_000_000))
    return result, exit_status, age_seconds


def _main_pid(unit: str) -> int | None:
    proc = _run_systemctl(["show", unit, "--property=MainPID", "--value"])
    value = (proc.stdout or "").strip()
    try:
        pid = int(value)
    except ValueError:
        return None
    return pid or None


def _direct_children(pid: int) -> list[dict[str, Any]]:
    return direct_children(pid)


def inspect_db(path: str | Path) -> dict[str, Any]:
    return DashboardQueries(path).status()


def _package_version() -> str:
    try:
        return metadata.version("github-agent-bridge")
    except metadata.PackageNotFoundError:
        return "unknown"


def _normalize_version(version: str) -> str:
    return version.strip().lstrip("v")


def _latest_github_release(repo: str, timeout_seconds: float = 5.0) -> dict[str, str] | None:
    repo = repo.strip().strip("/")
    if not repo:
        return None
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/releases/latest",
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "github-agent-bridge-monitor",
        },
    )
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, RuntimeError, urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    tag = str(payload.get("tag_name") or "").strip()
    if not tag:
        return None
    return {
        "tag_name": tag,
        "name": str(payload.get("name") or tag),
        "html_url": str(payload.get("html_url") or ""),
        "published_at": str(payload.get("published_at") or ""),
        "body": str(payload.get("body") or ""),
    }


def _release_summary(release: dict[str, str]) -> str:
    body = release.get("body") or ""
    for line in body.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped[:180]
    return release.get("name") or release.get("tag_name") or ""


def _add_alert(
    metrics: dict[str, Any],
    alerts: list[str],
    code: str,
    message: str,
    *,
    fingerprint: str | None = None,
    context: dict[str, Any] | None = None,
) -> None:
    alerts.append(message)
    metrics.setdefault("alert_codes", []).append(code)
    detail: dict[str, Any] = {"code": code, "message": message}
    if fingerprint:
        detail["fingerprint"] = fingerprint
    if context:
        detail["context"] = context
    metrics.setdefault("alert_details", []).append(detail)


def monitor(
    db: str | Path,
    executor_unit: str = "github-agent-bridge.service",
    reader_timer_unit: str = "github-agent-bridge-reader.timer",
    reader_service_unit: str = "github-agent-bridge-reader.service",
    thresholds: MonitorThresholds | None = None,
    check_systemd: bool = True,
    persist_observability: bool = False,
    process_sample_retention_seconds: int = DEFAULT_PROCESS_SAMPLE_RETENTION_SECONDS,
) -> MonitorReport:
    thresholds = thresholds or MonitorThresholds()
    metrics = inspect_db(db)
    alerts: list[str] = []
    package_version = _package_version()
    release_repo = os.getenv("GITHUB_AGENT_BRIDGE_RELEASE_REPO", "").strip()
    metrics["package_version"] = package_version
    if release_repo:
        metrics["release_repo"] = release_repo
        latest_release = _latest_github_release(release_repo)
        if latest_release:
            metrics["latest_release"] = {
                key: latest_release[key]
                for key in ("tag_name", "name", "html_url", "published_at")
                if latest_release.get(key)
            }
            if package_version != "unknown" and _normalize_version(package_version) != _normalize_version(latest_release["tag_name"]):
                summary = _release_summary(latest_release)
                detail = f": {summary}" if summary else ""
                url = f" ({latest_release['html_url']})" if latest_release.get("html_url") else ""
                _add_alert(
                    metrics,
                    alerts,
                    ALERT_RELEASE_AVAILABLE,
                    f"new github-agent-bridge release {latest_release['tag_name']} available; "
                    f"installed package version is {package_version}{url}{detail}",
                )
        else:
            metrics["latest_release_error"] = f"could not fetch latest release for {release_repo}"

    if not metrics.get("db_exists"):
        _add_alert(metrics, alerts, ALERT_DATABASE_MISSING, f"database missing: {metrics.get('db_path')}")
    elif not metrics.get("schema_ok", True):
        _add_alert(metrics, alerts, ALERT_SCHEMA_MISSING, "database schema missing jobs table")

    pending = int(metrics.get("pending", 0) or 0)
    blocked = int(metrics.get("blocked", 0) or 0)
    waiting = int(metrics.get("waiting_approval", 0) or 0)
    quarantined = int(metrics.get("quarantined_notifications", 0) or 0)
    pending_age = metrics.get("oldest_pending_age_seconds")
    if blocked:
        _add_alert(metrics, alerts, ALERT_BLOCKED_JOBS, f"blocked jobs: {blocked}")
    if quarantined:
        latest = metrics.get("latest_quarantined_notification") or {}
        detail = f"; latest uid={latest.get('uid')} reason={latest.get('reason')}" if latest else ""
        _add_alert(metrics, alerts, ALERT_QUARANTINED_NOTIFICATIONS, f"quarantined GitHub notifications: {quarantined}{detail}")
    if pending and pending_age is not None and pending_age > thresholds.pending_warn_seconds:
        _add_alert(
            metrics,
            alerts,
            ALERT_PENDING_QUEUE_OLD,
            f"pending queue oldest age {pending_age}s > {thresholds.pending_warn_seconds}s",
        )
    if waiting:
        metrics["waiting_approval"] = waiting

    if check_systemd:
        executor_state = _is_active(executor_unit)
        executor_pid = _main_pid(executor_unit)
        children = _direct_children(executor_pid) if executor_pid else []
        timer_state = _is_active(reader_timer_unit)
        reader_result, reader_exit, reader_age = _last_service_result(reader_service_unit)
        metrics.update({
            "executor_service": executor_state,
            "executor_pid": executor_pid,
            "executor_children": children,
            "reader_timer": timer_state,
            "reader_last_result": reader_result,
            "reader_last_exit_status": reader_exit,
            "reader_last_age_seconds": reader_age,
        })
        if executor_state != "active":
            _add_alert(metrics, alerts, ALERT_EXECUTOR_SERVICE, f"executor service is {executor_state}")
        tracking_id = metrics.get("executor_process_tracking_id")
        expected_workers = int(metrics.get("executor_worker_count") or 0)
        current_heartbeats = [
            heartbeat for heartbeat in metrics.get("worker_heartbeats", [])
            if heartbeat.get("executor_id") == tracking_id
        ]
        live_heartbeats = [
            heartbeat for heartbeat in current_heartbeats
            if int(heartbeat.get("age_seconds") or 0) <= thresholds.worker_heartbeat_warn_seconds
        ]
        metrics["worker_heartbeats_live"] = len(live_heartbeats)
        draining_heartbeats = [
            heartbeat
            for heartbeat in live_heartbeats
            if heartbeat.get("loop_state") == "draining"
        ]
        active_heartbeats = [
            heartbeat
            for heartbeat in live_heartbeats
            if heartbeat.get("loop_state") != "draining"
        ]
        metrics["executor_worker_live_count"] = len(live_heartbeats)
        metrics["executor_worker_busy_count"] = sum(
            heartbeat.get("active_job_id") is not None
            for heartbeat in live_heartbeats
        )
        metrics["executor_worker_draining_count"] = len(draining_heartbeats)
        if executor_state == "active" and expected_workers and len(active_heartbeats) < expected_workers:
            _add_alert(
                metrics, alerts, ALERT_WORKER_HEARTBEAT_MISSING,
                f"executor has {len(active_heartbeats)}/{expected_workers} active worker heartbeats",
            )
        stale_heartbeats = [
            heartbeat for heartbeat in current_heartbeats
            if int(heartbeat.get("age_seconds") or 0) > thresholds.worker_heartbeat_warn_seconds
        ]
        if executor_state == "active" and stale_heartbeats:
            detail = ", ".join(
                f"{heartbeat['worker_id']}={heartbeat['age_seconds']}s" for heartbeat in stale_heartbeats
            )
            _add_alert(
                metrics, alerts, ALERT_WORKER_HEARTBEAT_STALE,
                f"stale worker heartbeats: {detail}",
            )
        if metrics.get("running_jobs") and executor_state == "active" and not children:
            tracking_id = metrics.get("executor_process_tracking_id")
            expects_live_process = not tracking_id or any(
                (job.get("runtime_process") or {}).get("state") == "running"
                for job in metrics.get("running_jobs", [])
            )
            if expects_live_process:
                _add_alert(
                    metrics,
                    alerts,
                    ALERT_RUNNING_NO_EXECUTOR_CHILD,
                    "running jobs exist but executor has no child process",
                )
        if timer_state != "active":
            _add_alert(metrics, alerts, ALERT_READER_TIMER, f"reader timer is {timer_state}")
        if reader_result not in ("success", "unknown"):
            _add_alert(
                metrics,
                alerts,
                ALERT_READER_LAST_RESULT,
                f"reader last result is {reader_result} exit={reader_exit}",
            )
        if reader_age is None:
            metrics["reader_recent"] = "unknown"
        else:
            reader_recent = reader_age <= thresholds.reader_recent_seconds
            metrics["reader_recent"] = reader_recent
            if not reader_recent:
                _add_alert(
                    metrics,
                    alerts,
                    ALERT_READER_STALE,
                    f"reader last run age {reader_age}s > {thresholds.reader_recent_seconds}s",
                )

        tracking_id = metrics.get("executor_process_tracking_id")
        if tracking_id and executor_state == "active" and executor_pid:
            for job in metrics.get("running_jobs", []):
                problem = _runtime_process_problem(job, executor_pid, str(tracking_id))
                if problem:
                    _add_alert(
                        metrics,
                        alerts,
                        ALERT_RUNNING_PROCESS_MISMATCH,
                        f"running job {job.get('id')} process ownership mismatch: {problem}",
                    )

    latest_sample = recent_process_samples(db, limit=1)
    metrics["latest_process_sample"] = latest_sample[-1] if latest_sample else None
    for job in metrics.get("running_jobs", []):
        age = job.get("age_seconds")
        if age is None:
            continue
        limit = thresholds.review_running_warn_seconds if job.get("work_intent") == "review_only" else thresholds.work_running_warn_seconds
        if age > limit and _running_job_looks_stalled(job, metrics, thresholds):
            _add_alert(
                metrics,
                alerts,
                ALERT_RUNNING_JOB_STALLED,
                f"running job {job.get('id')} {job.get('work_key')} age {age}s > {limit}s without recent progress",
            )

    slo_state_update: dict[str, Any] | None = None
    metrics["slo_monitor"] = {
        "enabled": thresholds.slo_enabled,
        "window": thresholds.slo_window,
        "evaluations": [],
    }
    if thresholds.slo_enabled and metrics.get("db_exists") and metrics.get("schema_ok", True):
        slo_monitor, slo_alerts, slo_state_update = _evaluate_slo_thresholds(
            db,
            thresholds,
            persist_state=persist_observability,
        )
        metrics["slo_monitor"] = slo_monitor
        for alert in slo_alerts:
            _add_alert(
                metrics,
                alerts,
                alert["code"],
                alert["message"],
                fingerprint=alert["fingerprint"],
                context=alert["context"],
            )
    elif thresholds.slo_enabled:
        metrics["slo_monitor"].update({
            "schema_ok": False,
            "schema_error": metrics.get("schema_error") or "database schema is unavailable",
        })
    elif persist_observability:
        slo_state_update = {
            "version": 1,
            "config": thresholds.slo_config(),
            "metrics": {},
        }

    if persist_observability and metrics.get("db_exists") and metrics.get("schema_ok", True):
        record_monitor_observation(
            db,
            metrics,
            alerts,
            process_sample_retention_seconds=process_sample_retention_seconds,
            monitor_slo_state=slo_state_update,
        )

    return MonitorReport(ok=not alerts, alerts=alerts, metrics=metrics)


def _evaluate_slo_thresholds(
    db: str | Path,
    thresholds: MonitorThresholds,
    *,
    persist_state: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any] | None]:
    previous_state = get_monitor_slo_state(db) if persist_state else {}
    config = thresholds.slo_config()
    if previous_state.get("version") != 1 or previous_state.get("config") != config:
        previous_state = {"version": 1, "config": config, "metrics": {}}

    summary = DashboardQueries(db).metrics_summary(window=thresholds.slo_window)
    if summary.get("schema_ok") is False:
        error = str(summary.get("schema_error") or "SLO schema is unavailable")
        alerts = _preserved_slo_alerts(previous_state, error)
        return (
            {
                "enabled": True,
                "window": thresholds.slo_window,
                "schema_ok": False,
                "schema_error": error,
                "evaluations": [],
                "alerts_active": len(alerts),
            },
            alerts,
            None,
        )

    slo = summary.get("slo") if isinstance(summary.get("slo"), dict) else {}
    previous_metrics = previous_state.get("metrics")
    if not isinstance(previous_metrics, dict):
        previous_metrics = {}
    state: dict[str, Any] = {"version": 1, "config": config, "metrics": {}}
    evaluations: list[dict[str, Any]] = []
    alerts: list[dict[str, Any]] = []

    for spec in _slo_specs(thresholds):
        metric = spec["metric"]
        aggregate = _slo_aggregate(slo, spec["group"], spec["aggregate"])
        raw_value = aggregate.get(spec["value"])
        try:
            observed = float(raw_value) if raw_value is not None else None
        except (TypeError, ValueError):
            observed = None
        try:
            sample_count = int(aggregate.get("sample_count") or 0)
        except (TypeError, ValueError):
            sample_count = 0

        old = previous_metrics.get(metric)
        if not isinstance(old, dict):
            old = {}
        active = bool(old.get("active"))
        breach_count = int(old.get("breach_observations") or 0)
        recovery_count = int(old.get("recovery_observations") or 0)
        threshold = float(spec["threshold"])
        recovery_threshold = threshold * thresholds.slo_recovery_ratio

        if observed is None or sample_count < thresholds.slo_min_samples:
            breach_count = breach_count if active else 0
            recovery_count = 0
            status = "insufficient_samples"
        elif active:
            if observed > threshold:
                recovery_count = 0
                status = "alerting"
            elif observed <= recovery_threshold:
                recovery_count += 1
                if recovery_count >= thresholds.slo_recovery_observations:
                    active = False
                    breach_count = 0
                    recovery_count = 0
                    status = "recovered"
                else:
                    status = "recovering"
            else:
                recovery_count = 0
                status = "hysteresis"
        elif observed > threshold:
            breach_count += 1
            recovery_count = 0
            if breach_count >= thresholds.slo_breach_observations:
                active = True
                status = "alerting"
            else:
                status = "breach_pending"
        else:
            breach_count = 0
            recovery_count = 0
            status = "healthy"

        fingerprint = _slo_fingerprint(spec["code"], metric, thresholds.slo_window)
        record = {
            "code": spec["code"],
            "metric": metric,
            "unit": spec["unit"],
            "threshold": threshold,
            "recovery_threshold": recovery_threshold,
            "observed": observed,
            "sample_count": sample_count,
            "active": active,
            "status": status,
            "breach_observations": breach_count,
            "recovery_observations": recovery_count,
            "fingerprint": fingerprint,
        }
        state["metrics"][metric] = record
        evaluations.append(record)
        if active:
            alert = _slo_alert(record, thresholds)
            alerts.append(alert)
            record["message"] = alert["message"]
            record["context"] = alert["context"]

    return (
        {
            "enabled": True,
            "window": thresholds.slo_window,
            "schema_ok": True,
            "min_samples": thresholds.slo_min_samples,
            "evaluations": evaluations,
            "alerts_active": len(alerts),
        },
        alerts,
        state if persist_state else None,
    )


def _slo_specs(thresholds: MonitorThresholds) -> list[dict[str, Any]]:
    candidates = (
        (
            "queued_to_claimed.p90",
            ALERT_SLO_QUEUED_TO_CLAIMED_P90,
            "latencies",
            "queued_to_claimed",
            "p90",
            "seconds",
            thresholds.slo_queued_to_claimed_p90_seconds,
        ),
        (
            "claimed_to_first_output.p90",
            ALERT_SLO_CLAIMED_TO_FIRST_OUTPUT_P90,
            "latencies",
            "claimed_to_first_output",
            "p90",
            "seconds",
            thresholds.slo_claimed_to_first_output_p90_seconds,
        ),
        (
            "claimed_to_done.p90",
            ALERT_SLO_CLAIMED_TO_DONE_P90,
            "latencies",
            "claimed_to_done",
            "p90",
            "seconds",
            thresholds.slo_claimed_to_done_p90_seconds,
        ),
        (
            "blocked.percent",
            ALERT_SLO_BLOCKED_RATE,
            "rates",
            "blocked",
            "percent",
            "percent",
            thresholds.slo_blocked_rate_percent,
        ),
        (
            "retry.percent",
            ALERT_SLO_RETRY_RATE,
            "rates",
            "retry",
            "percent",
            "percent",
            thresholds.slo_retry_rate_percent,
        ),
    )
    return [
        {
            "metric": metric,
            "code": code,
            "group": group,
            "aggregate": aggregate,
            "value": value,
            "unit": unit,
            "threshold": threshold,
        }
        for metric, code, group, aggregate, value, unit, threshold in candidates
        if threshold is not None
    ]


def _slo_aggregate(slo: dict[str, Any], group: str, aggregate: str) -> dict[str, Any]:
    aggregates = slo.get(group)
    if not isinstance(aggregates, dict):
        return {}
    value = aggregates.get(aggregate)
    return value if isinstance(value, dict) else {}


def _slo_fingerprint(code: str, metric: str, window: str) -> str:
    return hashlib.sha256(
        f"monitor\0{code}\0{metric}\0{window}".encode("utf-8")
    ).hexdigest()


def _slo_alert(record: dict[str, Any], thresholds: MonitorThresholds) -> dict[str, Any]:
    suffix = "s" if record["unit"] == "seconds" else "%"
    observed = _format_measurement(record["observed"], suffix)
    limit = _format_measurement(record["threshold"], suffix)
    message = (
        f"SLO {record['metric']} observed {observed} with limit {limit} "
        f"over {thresholds.slo_window} ({record['sample_count']} samples; {record['status']})"
    )
    context = {
        "metric": record["metric"],
        "window": thresholds.slo_window,
        "observed": record["observed"],
        "threshold": record["threshold"],
        "recovery_threshold": record["recovery_threshold"],
        "unit": record["unit"],
        "sample_count": record["sample_count"],
        "min_samples": thresholds.slo_min_samples,
        "breach_observations": record["breach_observations"],
        "required_breach_observations": thresholds.slo_breach_observations,
        "recovery_observations": record["recovery_observations"],
        "required_recovery_observations": thresholds.slo_recovery_observations,
        "status": record["status"],
    }
    return {
        "code": record["code"],
        "message": message,
        "fingerprint": record["fingerprint"],
        "context": context,
    }


def _preserved_slo_alerts(state: dict[str, Any], error: str) -> list[dict[str, Any]]:
    records = state.get("metrics")
    if not isinstance(records, dict):
        return []
    alerts: list[dict[str, Any]] = []
    for record in records.values():
        if not isinstance(record, dict) or not record.get("active"):
            continue
        context = dict(record.get("context") or {})
        context["evaluation_error"] = error
        alerts.append({
            "code": str(record.get("code") or "monitor.slo"),
            "message": str(record.get("message") or f"SLO evaluation unavailable: {error}"),
            "fingerprint": str(record.get("fingerprint") or ""),
            "context": context,
        })
    return alerts


def _format_threshold(value: Any) -> str:
    numeric = float(value)
    return str(int(numeric)) if numeric.is_integer() else str(round(numeric, 2))


def _format_measurement(value: Any, suffix: str) -> str:
    return "n/a" if value is None else f"{_format_threshold(value)}{suffix}"


def _runtime_process_problem(
    job: dict[str, Any],
    executor_pid: int,
    tracking_id: str,
    *,
    launch_grace_seconds: int = 60,
    exit_grace_seconds: int = 120,
) -> str | None:
    worker_id = str(job.get("locked_by") or "")
    if not worker_id.startswith(f"{tracking_id}/worker-"):
        return f"worker {worker_id or '-'} is not owned by active executor {tracking_id}"
    runtime = job.get("runtime_process")
    if not isinstance(runtime, dict):
        age = int(job.get("age_seconds") or 0)
        return None if age <= launch_grace_seconds else "runtime PID was not registered"
    if runtime.get("executor_id") != tracking_id or runtime.get("worker_id") != worker_id:
        return "runtime metadata does not match the job worker"
    if runtime.get("state") == "exited":
        idle = int(job.get("idle_seconds") or 0)
        return None if idle <= exit_grace_seconds else "runtime exited but job did not reach a terminal state"
    try:
        pid = int(runtime["pid"])
        ppid = int(runtime["ppid"])
        start_time_ticks = int(runtime["start_time_ticks"])
    except (KeyError, TypeError, ValueError):
        return "runtime process identity is incomplete"
    if ppid != executor_pid:
        return f"runtime parent {ppid} does not match executor PID {executor_pid}"
    if not process_identity_matches(pid, start_time_ticks, expected_ppid=executor_pid):
        return f"PID {pid} is dead, reparented, zombie, or reused"
    return None


def _running_job_looks_stalled(job: dict[str, Any], metrics: dict[str, Any], thresholds: MonitorThresholds) -> bool:
    progress_ages = [
        progress.get("age_seconds")
        for progress in (job.get("semantic_progress"), job.get("visible_progress"))
        if isinstance(progress, dict) and progress.get("age_seconds") is not None
    ]
    progress_stale = not progress_ages or min(int(age) for age in progress_ages) > thresholds.progress_warn_seconds
    latest_sample = metrics.get("latest_process_sample")
    if isinstance(latest_sample, dict):
        sample_job_ids = latest_sample.get("running_job_ids") or []
        sample_applies = job.get("id") in sample_job_ids or not sample_job_ids
        process_quiet = sample_applies and not latest_sample.get("active_since_last_sample") and (
            latest_sample.get("idle_seconds") is None or int(latest_sample.get("idle_seconds") or 0) > thresholds.progress_warn_seconds
        )
    else:
        process_quiet = not bool(metrics.get("executor_children"))
    return progress_stale and process_quiet


def report_json(report: MonitorReport) -> str:
    return json.dumps(report.to_dict(), ensure_ascii=False, indent=2)
