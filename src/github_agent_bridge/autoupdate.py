from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import __version__
from .models import utc_now
from .persistence import (
    ACTIVE_JOB_STATUSES,
    Database,
    StateRepository,
    active_job_counts,
    backup_sqlite_database,
    restore_sqlite_database,
)
from .policy import Policy
from .queue import JobQueue

CommandRunner = Callable[[Sequence[str], Path | None], subprocess.CompletedProcess[str]]

UPDATE_STATE_KEY = "autoupdate"
RISKY_PATH_PREFIXES = (
    "src/github_agent_bridge/cli.py",
    "src/github_agent_bridge/dispatch.py",
    "src/github_agent_bridge/executor.py",
    "src/github_agent_bridge/monitor.py",
    "src/github_agent_bridge/parser.py",
    "src/github_agent_bridge/policy.py",
    "src/github_agent_bridge/queue.py",
    "src/github_agent_bridge/reader.py",
    "src/github_agent_bridge/reader_run.py",
    "src/github_agent_bridge/sql/",
)
DASHBOARD_PATH_PREFIXES = (
    "dashboard/",
    "src/github_agent_bridge/backend.py",
    "src/github_agent_bridge/dashboard_data.py",
    "src/github_agent_bridge/dashboard_static/",
)
WEBHOOK_PATH_PREFIXES = (
    "src/github_agent_bridge/backend.py",
    "src/github_agent_bridge/models.py",
    "src/github_agent_bridge/parser.py",
    "src/github_agent_bridge/policy.py",
    "src/github_agent_bridge/queue.py",
    "src/github_agent_bridge/sql/",
    "src/github_agent_bridge/webhook.py",
)
SYSTEMD_PATH_PREFIXES = ("systemd/",)
DEFAULT_SYSTEMD_UNITS = {
    "executor": "github-agent-bridge.service",
    "dashboard": "github-agent-bridge-dashboard.service",
    "webhook": "github-agent-bridge-webhook.service",
    "reader": "github-agent-bridge-reader.timer",
    "monitor": "github-agent-bridge-monitor.timer",
    "feedback": "github-agent-bridge-feedback.timer",
    "autoupdate_service": "github-agent-bridge-autoupdate.service",
    "autoupdate_timer": "github-agent-bridge-autoupdate.timer",
}
DEFAULT_BACKUP_DIR = Path("~/.local/state/github-agent-bridge/backups").expanduser()


@dataclass(frozen=True)
class ReleaseInfo:
    tag_name: str
    name: str = ""
    url: str = ""
    body: str = ""
    published_at: str = ""
    source: str = "github_release"

    def to_json(self) -> dict[str, str]:
        return {
            "tag_name": self.tag_name,
            "name": self.name,
            "url": self.url,
            "body": self.body,
            "published_at": self.published_at,
            "source": self.source,
        }


def _default_runner(args: Sequence[str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    command = list(args)
    try:
        return subprocess.run(command, cwd=cwd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except FileNotFoundError as exc:
        return subprocess.CompletedProcess(command, 127, "", str(exc))


def _run_json(args: Sequence[str], cwd: Path | None, runner: CommandRunner) -> dict[str, Any]:
    proc = runner(args, cwd)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or f"{args[0]} failed with exit code {proc.returncode}")
    try:
        return json.loads(proc.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{args[0]} returned invalid JSON") from exc


def latest_release(repo: str, *, gh_bin: str = "gh", runner: CommandRunner = _default_runner) -> ReleaseInfo:
    data = _run_json(
        [
            gh_bin,
            "release",
            "view",
            "--repo",
            repo,
            "--json",
            "tagName,name,url,body,publishedAt,isDraft,isPrerelease",
        ],
        None,
        runner,
    )
    tag_name = str(data.get("tagName") or "")
    if not tag_name:
        raise RuntimeError("latest release did not include a tagName")
    return ReleaseInfo(
        tag_name=tag_name,
        name=str(data.get("name") or ""),
        url=str(data.get("url") or ""),
        body=str(data.get("body") or ""),
        published_at=str(data.get("publishedAt") or ""),
    )


def _git_output(args: Sequence[str], repo_dir: Path, runner: CommandRunner) -> str:
    proc = runner(["git", *args], repo_dir)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "git failed")
    return proc.stdout.strip()


def changed_files_between(repo_dir: Path, base_ref: str, target_ref: str, *, runner: CommandRunner = _default_runner) -> list[str]:
    output = _git_output(["diff", "--name-only", f"{base_ref}..{target_ref}"], repo_dir, runner)
    return [line.strip() for line in output.splitlines() if line.strip()]


def classify_changed_files(files: Sequence[str]) -> dict[str, Any]:
    risky_files = [path for path in files if path.startswith(RISKY_PATH_PREFIXES)]
    migration_files = [path for path in files if path.startswith("src/github_agent_bridge/sql/") or "/migrations/" in path]
    dashboard_files = [path for path in files if path.startswith(DASHBOARD_PATH_PREFIXES)]
    webhook_files = [path for path in files if path.startswith(WEBHOOK_PATH_PREFIXES)]
    systemd_files = [path for path in files if path.startswith(SYSTEMD_PATH_PREFIXES)]
    dashboard_only = bool(files) and len(dashboard_files) == len(files) and not webhook_files
    webhook_only = bool(files) and len(webhook_files) == len(files) and not dashboard_files and not risky_files
    api_only = bool(files) and all(
        path.startswith(DASHBOARD_PATH_PREFIXES) or path.startswith(WEBHOOK_PATH_PREFIXES)
        for path in files
    ) and not risky_files
    risk = "dashboard_only" if dashboard_only else "webhook_only" if webhook_only else "api_only" if api_only else "executor_or_shared"
    if migration_files:
        risk = "migration_required"
    elif risky_files:
        risk = "executor_or_queue"
    elif systemd_files:
        risk = "service_topology"
    elif not files:
        risk = "none"
    return {
        "risk": risk,
        "dashboard_only": dashboard_only,
        "webhook_only": webhook_only,
        "api_only": api_only,
        "webhook_files": webhook_files,
        "risky_files": risky_files,
        "migration_files": migration_files,
        "systemd_files": systemd_files,
        "changed_files": list(files),
    }


def plan_systemd_actions(decision: str, classification: dict[str, Any], *, units: dict[str, str] | None = None) -> dict[str, Any]:
    unit_names = {**DEFAULT_SYSTEMD_UNITS, **(units or {})}
    systemd_files = list(classification.get("systemd_files") or [])
    daemon_reload = bool(systemd_files)

    immediate: list[dict[str, str]] = []
    deferred: list[dict[str, str]] = []
    notes: list[str] = []

    def action(command: str, unit_key: str, reason: str) -> dict[str, str]:
        unit = unit_names.get(unit_key, "")
        return {"command": command, "unit": unit, "reason": reason}

    if daemon_reload:
        immediate.append({"command": "daemon-reload", "unit": "--user", "reason": "systemd unit files changed"})

    if decision == "stage_dashboard_reload":
        immediate.append(action("try-restart", "dashboard", "dashboard-only update can reload independently"))
    elif decision == "stage_webhook_reload":
        immediate.append(action("try-restart", "webhook", "webhook ingress update can reload independently"))
    elif decision == "stage_api_reload":
        immediate.append(action("try-restart", "dashboard", "dashboard API update can reload independently"))
        immediate.append(action("try-restart", "webhook", "webhook ingress update can reload independently"))
    elif decision == "stage_defer_executor_reload":
        immediate.append(action("try-restart", "dashboard", "dashboard can refresh while executor jobs finish"))
        if classification.get("webhook_files"):
            immediate.append(action("try-restart", "webhook", "webhook ingress can refresh independently"))
        deferred.append(action("restart", "executor", "executor/shared update waits for active queue to drain"))
    elif decision == "stage_full_reload":
        immediate.append(action("try-restart", "dashboard", "refresh dashboard after package update"))
        if classification.get("webhook_files"):
            immediate.append(action("try-restart", "webhook", "refresh webhook ingress after package update"))
        immediate.append(action("restart", "executor", "queue is quiet, executor reload is allowed"))
    elif decision == "defer_migration":
        deferred.append(action("restart", "executor", "schema migration must wait for active queue to drain"))
        deferred.append(action("try-restart", "dashboard", "dashboard refresh waits for migration window"))
        deferred.append(action("try-restart", "webhook", "webhook ingress refresh waits for migration window"))

    if daemon_reload:
        affected_units = sorted(
            {
                unit_names[key]
                for path in systemd_files
                for key, filename in (
                    ("executor", "github-agent-bridge.service"),
                    ("dashboard", "github-agent-bridge-dashboard.service"),
                    ("webhook", "github-agent-bridge-webhook.service"),
                    ("reader", "github-agent-bridge-reader.timer"),
                    ("monitor", "github-agent-bridge-monitor.timer"),
                    ("feedback", "github-agent-bridge-feedback.timer"),
                    ("autoupdate_service", "github-agent-bridge-autoupdate.service"),
                    ("autoupdate_timer", "github-agent-bridge-autoupdate.timer"),
                )
                if path.endswith(filename)
            }
        )
        if affected_units:
            notes.append("Unit changes detected: " + ", ".join(affected_units))
        notes.append("Run systemctl --user daemon-reload before restarting changed units.")

    return {
        "manager": "systemd",
        "scope": "user",
        "units": unit_names,
        "daemon_reload_required": daemon_reload,
        "immediate": immediate,
        "deferred": deferred,
        "notes": notes,
    }


def active_queue_counts(queue: JobQueue) -> dict[str, int]:
    return dict(update_queue_state(queue)["active_counts"])


def update_queue_state(queue: JobQueue) -> dict[str, Any]:
    stats = queue.stats()
    executor_paused = queue.executor_paused()
    active_statuses = ("running",) if executor_paused else ACTIVE_JOB_STATUSES
    active_counts = {status: int(stats.get(status, 0)) for status in active_statuses}
    return {
        "active_counts": active_counts,
        "active_total": sum(active_counts.values()),
        "executor_paused": executor_paused,
    }


def database_queue_state(
    db: str | Path,
    *,
    migration_preflight: bool = False,
) -> dict[str, Any]:
    """Inspect queue state without triggering schema initialization or migration."""
    database = Database(db)
    counts = active_job_counts(database)
    executor_paused = StateRepository(database).executor_paused()
    active_statuses = (
        ACTIVE_JOB_STATUSES
        if migration_preflight or not executor_paused
        else ("running",)
    )
    active_counts = {status: int(counts.get(status, 0)) for status in active_statuses}
    return {
        "active_counts": active_counts,
        "active_total": sum(active_counts.values()),
        "executor_paused": executor_paused,
    }


def _service_plan_restarts_executor(service_plan: dict[str, Any]) -> bool:
    units = service_plan.get("units") if isinstance(service_plan.get("units"), dict) else {}
    executor_unit = str(units.get("executor") or DEFAULT_SYSTEMD_UNITS["executor"])
    return any(
        action.get("unit") == executor_unit and action.get("command") in {"restart", "try-restart"}
        for action in service_plan.get("immediate") or []
        if isinstance(action, dict)
    )


def load_update_state(queue: JobQueue) -> dict[str, Any]:
    raw = queue.get_state(UPDATE_STATE_KEY, "")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {"state_error": "invalid_autoupdate_state"}
    return data if isinstance(data, dict) else {}


def save_update_state(queue: JobQueue, state: dict[str, Any]) -> None:
    queue.set_state(UPDATE_STATE_KEY, json.dumps(state, sort_keys=True))


def default_install_command(repo: str, target_tag: str, *, python_bin: str = sys.executable) -> list[str]:
    repo_ref = repo
    if not repo_ref.startswith(("http://", "https://", "git@")):
        repo_ref = f"https://github.com/{repo_ref}.git"
    return [python_bin, "-m", "pip", "install", f"git+{repo_ref}@{target_tag}"]


def _command_result(
    kind: str,
    args: Sequence[str],
    proc: subprocess.CompletedProcess[str],
    *,
    unit: str = "",
    reason: str = "",
) -> dict[str, Any]:
    return {
        "kind": kind,
        "command": list(args),
        "unit": unit,
        "reason": reason,
        "returncode": proc.returncode,
        "status": "succeeded" if proc.returncode == 0 else "failed",
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _backup_sqlite_db(db: str | Path, backup_dir: str | Path | None = None) -> dict[str, Any]:
    backup_root = Path(backup_dir).expanduser() if backup_dir else DEFAULT_BACKUP_DIR
    return backup_sqlite_database(db, backup_root)


def _restore_sqlite_db(db: str | Path, backup_path: str | Path) -> dict[str, Any]:
    return restore_sqlite_database(db, backup_path)


def default_migration_command(db: str | Path, *, python_bin: str = sys.executable) -> list[str]:
    return [
        python_bin,
        "-m",
        "github_agent_bridge.cli",
        "--db",
        str(Path(db).expanduser()),
        "migrate-db",
    ]


def default_version_check_command(*, python_bin: str = sys.executable) -> list[str]:
    return [python_bin, "-c", "from github_agent_bridge import __version__; print(__version__)"]


def _installed_version_from_command(command: Sequence[str], runner: CommandRunner) -> tuple[dict[str, Any], str]:
    proc = runner(list(command), None)
    return _command_result("postcheck", command, proc, reason="verify installed package version"), proc.stdout.strip()


def _model_smoke_postcheck(
    policy_path: str | Path,
    *,
    openclaw_bin: str,
    runner: CommandRunner,
) -> list[dict[str, Any]]:
    policy = Policy.from_file(policy_path)
    agent = policy.route_for("gisce/erp").agent
    erp_route = policy.model_route_for(
        "gisce/erp",
        "reply_comment",
        "work_allowed",
        "substantive",
    )
    specs = [
        ("intent_classifier", policy.intent_classifier.model, policy.intent_classifier.thinking),
        ("feedback_learning", policy.feedback_learning.model, policy.feedback_learning.thinking),
        ("erp_substantive", erp_route.model, erp_route.thinking),
    ]
    checks: list[dict[str, Any]] = []
    timestamp = utc_now().replace(":", "").replace("-", "")
    for name, model, thinking in specs:
        marker = f"GAB_MODEL_SMOKE_{name.upper()}_OK"
        command = [
            openclaw_bin,
            "agent",
            "--json",
            "--session-id",
            f"github-agent-bridge-model-smoke-{name}-{timestamp}",
            "--session-key",
            f"github-agent-bridge:model-smoke:{name}:{timestamp}",
            "--timeout",
            "180",
            "--thinking",
            thinking or "low",
            "--message",
            f"Return exactly this text and nothing else: {marker}",
        ]
        if agent:
            command.extend(["--agent", agent])
        if model:
            command.extend(["--model", model])
        proc = runner(command, None)
        check = _command_result(
            "postcheck",
            command,
            proc,
            reason=f"verify {name} model route",
        )
        check.update(
            {
                "name": "model_smoke",
                "route": name,
                "model": model or "",
                "thinking": thinking or "",
                "ok": proc.returncode == 0 and marker in proc.stdout,
            }
        )
        checks.append(check)
    return checks


def _postcheck_update(
    db: str | Path | None,
    plan: dict[str, Any],
    *,
    systemctl_bin: str,
    run_systemd: bool,
    runner: CommandRunner,
    policy_path: str | Path | None = None,
    openclaw_bin: str = "openclaw",
) -> dict[str, Any]:
    checks: dict[str, Any] = {"ok": False, "checks": [], "errors": []}
    target = plan.get("target") if isinstance(plan.get("target"), dict) else {}
    target_tag = str(target.get("tag_name") or "")
    expected_version = target_tag.lstrip("v")

    command = default_version_check_command()
    version_check, installed_version = _installed_version_from_command(command, runner)
    version_check["name"] = "installed_version"
    version_check["expected"] = expected_version
    version_check["actual"] = installed_version
    version_check["ok"] = version_check["returncode"] == 0 and (not expected_version or installed_version == expected_version)
    checks["checks"].append(version_check)
    if not version_check["ok"]:
        checks["errors"].append("installed_version_mismatch")

    if db is not None:
        queue_counts = active_queue_counts(JobQueue(db))
        queue_check = {
            "kind": "postcheck",
            "name": "queue_state",
            "status": "succeeded",
            "active_counts": queue_counts,
            "active_total": sum(queue_counts.values()),
            "ok": True,
        }
        checks["checks"].append(queue_check)

    if run_systemd:
        service_plan = plan.get("service_plan") if isinstance(plan.get("service_plan"), dict) else {}
        units = [
            str(action.get("unit") or "")
            for action in service_plan.get("immediate") or []
            if action.get("unit") and action.get("unit") != "--user"
        ]
        for unit in dict.fromkeys(units):
            command = [systemctl_bin, "--user", "is-active", unit]
            proc = runner(command, None)
            check = _command_result("postcheck", command, proc, unit=unit, reason="verify service is active")
            check["name"] = "systemd_active"
            check["ok"] = proc.returncode == 0 and proc.stdout.strip() == "active"
            checks["checks"].append(check)
            if not check["ok"]:
                checks["errors"].append(f"service_not_active:{unit}")

    if policy_path is not None:
        try:
            model_checks = _model_smoke_postcheck(
                policy_path,
                openclaw_bin=openclaw_bin,
                runner=runner,
            )
        except Exception as exc:
            checks["errors"].append(f"model_smoke_setup_failed:{exc}")
        else:
            checks["checks"].extend(model_checks)
            checks["errors"].extend(
                f"model_smoke_failed:{check['route']}"
                for check in model_checks
                if not check["ok"]
            )

    checks["ok"] = not checks["errors"]
    return checks


def _record_degraded_update_state(
    db: str | Path | None,
    plan: dict[str, Any],
    *,
    migration: dict[str, Any] | None,
    execution: dict[str, Any],
    degraded: bool = True,
) -> None:
    if db is None:
        return
    state = record_update_plan(db, plan)
    now = utc_now()
    live_queue = execution.get("queue") if isinstance(execution.get("queue"), dict) else None
    state.update(
        {
            "updated_at": now,
            "degraded": degraded,
            "blocked_reason": execution["blocked"][-1] if execution["blocked"] else "autoupdate_apply_failed",
            "migration": migration or {},
            "execution": {
                "commands": execution.get("commands", []),
                "blocked": execution.get("blocked", []),
                "updated_at": now,
            },
        }
    )
    if live_queue is not None:
        state["queue"] = live_queue
    save_update_state(JobQueue(db), state)


def apply_update_plan(
    plan: dict[str, Any],
    *,
    db: str | Path | None = None,
    repo: str = "gisce/github-agent-bridge",
    backup_dir: str | Path | None = None,
    install_command: Sequence[str] | None = None,
    migration_command: Sequence[str] | None = None,
    systemctl_bin: str = "systemctl",
    run_install: bool = True,
    run_migrations: bool = True,
    run_systemd: bool = True,
    run_postchecks: bool = True,
    policy_path: str | Path | None = None,
    openclaw_bin: str = "openclaw",
    runner: CommandRunner = _default_runner,
) -> dict[str, Any]:
    """Execute the safe, immediate subset described by an update plan."""

    target = plan.get("target") if isinstance(plan.get("target"), dict) else {}
    target_tag = str(target.get("tag_name") or "")
    classification = plan.get("classification") if isinstance(plan.get("classification"), dict) else {}
    result: dict[str, Any] = {
        "applied": False,
        "blocked": [],
        "commands": [],
        "migration": {},
        "postcheck": {},
    }

    if plan.get("decision") == "noop" or plan.get("up_to_date"):
        result["applied"] = True
        return result
    if not target_tag:
        result["blocked"].append("missing_target_tag")
        return result
    migration_files = list(classification.get("migration_files") or [])
    migration_state: dict[str, Any] = {}
    service_plan = plan.get("service_plan") if isinstance(plan.get("service_plan"), dict) else {}
    queue_info = (
        database_queue_state(db, migration_preflight=bool(migration_files))
        if db is not None
        else plan.get("queue") if isinstance(plan.get("queue"), dict) else {}
    )
    result["queue"] = queue_info
    active_total = int(queue_info.get("active_total") or 0)
    if migration_files and active_total:
        result["blocked"].append("active_jobs_block_migration")
        _record_degraded_update_state(db, plan, migration={"required": True, "files": migration_files, "status": "deferred"}, execution=result, degraded=False)
        return result
    if active_total and _service_plan_restarts_executor(service_plan):
        result["blocked"].append("active_jobs_block_executor_reload")
        _record_degraded_update_state(db, plan, migration={}, execution=result, degraded=False)
        return result
    if migration_files and db is None:
        result["blocked"].append("missing_db_for_migration")
        return result

    if migration_files:
        migration_state = {
            "required": True,
            "files": migration_files,
            "status": "backup_pending",
            "started_at": utc_now(),
        }
        result["migration"] = migration_state
        try:
            migration_state["backup"] = _backup_sqlite_db(db, backup_dir)
        except sqlite3.Error as exc:
            migration_state["status"] = "backup_failed"
            migration_state["error"] = str(exc)
            result["blocked"].append("migration_backup_failed")
            _record_degraded_update_state(db, plan, migration=migration_state, execution=result)
            return result
        migration_state["status"] = "backup_complete"

    if run_install:
        command = list(install_command or default_install_command(repo, target_tag))
        proc = runner(command, None)
        result["commands"].append(_command_result("install", command, proc, reason=f"install {target_tag}"))
        if proc.returncode != 0:
            if migration_state:
                migration_state["status"] = "install_failed_after_backup"
                _record_degraded_update_state(db, plan, migration=migration_state, execution=result)
            return result

    if migration_files and run_migrations:
        queue_info = database_queue_state(db, migration_preflight=True)
        result["queue"] = queue_info
        if int(queue_info.get("active_total") or 0):
            migration_state["status"] = "deferred"
            result["blocked"].append("active_jobs_block_migration")
            _record_degraded_update_state(db, plan, migration=migration_state, execution=result, degraded=False)
            return result
        migration_state["status"] = "applying"
        command = list(migration_command or default_migration_command(db))
        proc = runner(command, None)
        result["commands"].append(_command_result("migration", command, proc, reason="apply versioned SQLite migrations"))
        if proc.returncode != 0:
            migration_state["status"] = "failed"
            migration_state["error"] = proc.stderr.strip() or proc.stdout.strip()
            backup = migration_state.get("backup") if isinstance(migration_state.get("backup"), dict) else {}
            backup_path = str(backup.get("path") or "")
            if backup_path:
                try:
                    migration_state["rollback"] = _restore_sqlite_db(db, backup_path)
                    migration_state["status"] = "rolled_back"
                except sqlite3.Error as exc:
                    migration_state["rollback_error"] = str(exc)
            result["blocked"].append("migration_apply_failed")
            _record_degraded_update_state(db, plan, migration=migration_state, execution=result)
            return result
        migration_state["status"] = "applied"
        migration_state["applied_at"] = utc_now()

    if run_systemd:
        if db is not None and _service_plan_restarts_executor(service_plan):
            queue_info = update_queue_state(JobQueue(db))
            result["queue"] = queue_info
            if int(queue_info.get("active_total") or 0):
                result["blocked"].append("active_jobs_block_executor_reload")
                _record_degraded_update_state(
                    db,
                    plan,
                    migration=migration_state,
                    execution=result,
                    degraded=bool(migration_state.get("status") == "applied"),
                )
                return result
        if not _run_systemd_actions(service_plan.get("immediate") or [], result, systemctl_bin=systemctl_bin, runner=runner):
            _record_degraded_update_state(db, plan, migration=migration_state, execution=result)
            return result

    if run_postchecks:
        result["postcheck"] = _postcheck_update(
            db,
            plan,
            systemctl_bin=systemctl_bin,
            run_systemd=run_systemd,
            runner=runner,
            policy_path=policy_path,
            openclaw_bin=openclaw_bin,
        )
        if not result["postcheck"].get("ok"):
            result["blocked"].append("postcheck_failed")
            _record_degraded_update_state(db, plan, migration=migration_state, execution=result)
            return result

    if migration_state:
        migration_state["status"] = "complete"
        migration_state["completed_at"] = utc_now()
    result["applied"] = True
    return result


def _run_systemd_actions(
    actions: Sequence[dict[str, Any]],
    result: dict[str, Any],
    *,
    systemctl_bin: str,
    runner: CommandRunner,
) -> bool:
    for action in actions:
        command = str(action.get("command") or "")
        unit = str(action.get("unit") or "")
        if not command:
            continue
        if command == "daemon-reload":
            args = [systemctl_bin, "--user", "daemon-reload"]
        elif unit:
            args = [systemctl_bin, "--user", command, unit]
        else:
            result["blocked"].append(f"missing_unit_for_{command}")
            return False
        proc = runner(args, None)
        result["commands"].append(
            _command_result("systemd", args, proc, unit=unit, reason=str(action.get("reason") or ""))
        )
        if proc.returncode != 0:
            return False
    return True


def complete_pending_reload(
    db: str | Path,
    *,
    systemctl_bin: str = "systemctl",
    runner: CommandRunner = _default_runner,
) -> dict[str, Any]:
    """Run a recorded deferred executor reload once the queue is quiet."""

    queue = JobQueue(db)
    state = load_update_state(queue)
    queue_state = update_queue_state(queue)
    active_total = int(queue_state["active_total"])
    result: dict[str, Any] = {
        "completed": False,
        "blocked": [],
        "commands": [],
        "queue": queue_state,
        "state": state,
    }

    if not state:
        result["blocked"].append("no_recorded_update")
        return result
    if state.get("state_error"):
        result["blocked"].append(str(state["state_error"]))
        return result
    if not state.get("executor_reload_pending"):
        result["completed"] = True
        result["message"] = "no_pending_executor_reload"
        return result

    classification = state.get("classification") if isinstance(state.get("classification"), dict) else {}
    if classification.get("migration_files"):
        result["blocked"].append("migration_completion_not_supported")
        return result
    if active_total:
        result["blocked"].append("active_jobs_block_executor_reload")
        updated_state = {
            **state,
            "updated_at": utc_now(),
            "blocked_reason": "active_jobs_block_executor_reload",
            "queue": result["queue"],
        }
        save_update_state(queue, updated_state)
        result["state"] = updated_state
        return result

    service_plan = state.get("service_plan") if isinstance(state.get("service_plan"), dict) else {}
    deferred_actions = service_plan.get("deferred") or []
    if not deferred_actions:
        result["blocked"].append("no_deferred_actions")
        return result

    if not _run_systemd_actions(deferred_actions, result, systemctl_bin=systemctl_bin, runner=runner):
        return result

    completed_at = utc_now()
    completed_state = {
        **state,
        "updated_at": completed_at,
        "completed_at": completed_at,
        "decision": "noop",
        "executor_reload_pending": False,
        "blocked_reason": "",
        "queue": result["queue"],
        "service_plan": {**service_plan, "deferred": []},
        "completion": {
            "commands": result["commands"],
            "completed_at": completed_at,
        },
    }
    save_update_state(queue, completed_state)
    result["completed"] = True
    result["state"] = completed_state
    return result


def plan_update(
    db: str | Path,
    *,
    repo: str = "gisce/github-agent-bridge",
    repo_dir: str | Path = ".",
    target_tag: str | None = None,
    gh_bin: str = "gh",
    installed_version: str = __version__,
    systemd_units: dict[str, str] | None = None,
    runner: CommandRunner = _default_runner,
) -> dict[str, Any]:
    repo_path = Path(repo_dir).expanduser().resolve()
    current_tag = f"v{installed_version.lstrip('v')}"
    release = ReleaseInfo(tag_name=target_tag, source="explicit_target") if target_tag else latest_release(repo, gh_bin=gh_bin, runner=runner)

    warnings: list[str] = []
    try:
        files = [] if release.tag_name == current_tag else changed_files_between(repo_path, current_tag, release.tag_name, runner=runner)
    except RuntimeError as exc:
        files = []
        warnings.append(f"changed_files_unavailable: {exc}")
    classification = classify_changed_files(files)
    up_to_date = release.tag_name == current_tag
    migration_required = bool(classification["migration_files"])
    queue_state = database_queue_state(
        db,
        migration_preflight=migration_required,
    )
    active_total = int(queue_state["active_total"])
    executor_reload_pending = False
    dashboard_restart_allowed = False
    executor_restart_allowed = False
    blocked_reason = ""

    if up_to_date:
        decision = "noop"
    elif migration_required and active_total:
        decision = "defer_migration"
        blocked_reason = "active_jobs_block_migration"
    elif classification["dashboard_only"]:
        decision = "stage_dashboard_reload"
        dashboard_restart_allowed = True
    elif classification["webhook_only"]:
        decision = "stage_webhook_reload"
    elif classification["api_only"]:
        decision = "stage_api_reload"
        dashboard_restart_allowed = True
    elif active_total:
        decision = "stage_defer_executor_reload"
        executor_reload_pending = True
        dashboard_restart_allowed = True
        blocked_reason = "active_jobs_block_executor_reload"
    else:
        decision = "stage_full_reload"
        dashboard_restart_allowed = True
        executor_restart_allowed = True

    service_plan = plan_systemd_actions(decision, classification, units=systemd_units)

    return {
        "checked_at": utc_now(),
        "installed_version": installed_version,
        "installed_tag": current_tag,
        "target": release.to_json(),
        "up_to_date": up_to_date,
        "queue": queue_state,
        "classification": classification,
        "decision": decision,
        "dashboard_restart_allowed": dashboard_restart_allowed,
        "executor_restart_allowed": executor_restart_allowed,
        "executor_reload_pending": executor_reload_pending,
        "blocked_reason": blocked_reason,
        "service_plan": service_plan,
        "warnings": warnings,
    }


def record_update_plan(db: str | Path, plan: dict[str, Any]) -> dict[str, Any]:
    queue = JobQueue(db)
    state = {
        "updated_at": utc_now(),
        "installed_version": plan["installed_version"],
        "installed_tag": plan["installed_tag"],
        "target": plan["target"],
        "decision": plan["decision"],
        "executor_reload_pending": bool(plan.get("executor_reload_pending")),
        "blocked_reason": plan.get("blocked_reason", ""),
        "queue": plan.get("queue", {}),
        "classification": {
            "risk": plan["classification"]["risk"],
            "migration_files": plan["classification"]["migration_files"],
            "risky_files": plan["classification"]["risky_files"],
            "systemd_files": plan["classification"].get("systemd_files", []),
        },
        "service_plan": plan.get("service_plan", {}),
        "warnings": plan.get("warnings", []),
    }
    save_update_state(queue, state)
    return state
