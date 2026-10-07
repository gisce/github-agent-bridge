from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from .database import Database, TransactionMode

WEBHOOK_COVERAGE_EVENT_GLOBS = (
    "issue_comment:created:*",
    "pull_request_review_comment:created:*",
    "pull_request_review:created:*",
    "commit_comment:created:*",
    "workflow_run:workflow_run_failed:*",
)


def _coverage_predicate(column: str) -> str:
    if column not in {"event_key", "i.event_key", "w.event_key"}:
        raise ValueError("unsupported webhook coverage column")
    return "(" + " OR ".join(
        f"{column} GLOB '{pattern}'" for pattern in WEBHOOK_COVERAGE_EVENT_GLOBS
    ) + ")"


@dataclass(frozen=True)
class WebhookReceipt:
    delivery_id: str
    event_name: str
    action: str | None
    event_key: str | None
    repository: str | None
    status: str
    enqueue_status: str | None
    job_id: int | None


class WebhookRepository:
    """Persist webhook deliveries and expose dashboard-oriented webhook queries."""

    HOOK_COLUMNS = (
        "hook_id,target,target_type,name,active,events_json,content_type,insecure_ssl,delivery_url,"
        "github_api_url,ping_url,deliveries_url,github_created_at,github_updated_at,last_ping_at,last_event_at,"
        "last_delivery_id,last_event_name,last_action,last_repository,last_result,updated_at"
    )
    DELIVERY_COLUMNS = (
        "r.delivery_id,r.hook_id,r.event_name,r.action,r.event_key,r.repository,r.status,"
        "r.enqueue_status,r.job_id,r.duplicate_count,r.created_at,"
        "h.target hook_target,h.target_type hook_target_type"
    )

    def __init__(self, database: Database) -> None:
        self.database = database

    def persist_delivery(
        self,
        *,
        delivery_id: str,
        hook_id: str | None,
        event_name: str,
        action: str | None,
        event_key: str | None,
        repository: str | None,
        payload_hash: str,
        payload_json: str,
        payload: dict[str, Any],
        status: str,
        enqueue_status: str | None,
        job_id: int | None,
        retention_days: int,
        created_at: str,
        hook_target: tuple[str, str] | None,
    ) -> WebhookReceipt:
        duplicate = False
        try:
            with self.database.transaction(TransactionMode.IMMEDIATE) as con:
                con.execute(
                    "DELETE FROM webhook_shadow_receipts WHERE julianday(created_at) < julianday('now', ?)",
                    (f"-{retention_days} days",),
                )
                con.execute(
                    "INSERT INTO webhook_shadow_receipts(delivery_id,hook_id,event_name,action,event_key,repository,payload_hash,payload_json,status,enqueue_status,job_id,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        delivery_id,
                        hook_id,
                        event_name,
                        action,
                        event_key,
                        repository,
                        payload_hash,
                        payload_json,
                        status,
                        enqueue_status,
                        job_id,
                        created_at,
                    ),
                )
                self._upsert_hook(
                    con,
                    hook_id=hook_id,
                    event_name=event_name,
                    action=action,
                    repository=repository,
                    delivery_id=delivery_id,
                    status=status,
                    payload=payload,
                    created_at=created_at,
                    hook_target=hook_target,
                )
        except sqlite3.IntegrityError:
            duplicate = True
            status = "duplicate"
            with self.database.transaction(TransactionMode.IMMEDIATE) as con:
                con.execute(
                    "UPDATE webhook_shadow_receipts SET duplicate_count=duplicate_count+1,"
                    "enqueue_status=COALESCE(enqueue_status,?),job_id=COALESCE(job_id,?) WHERE delivery_id=?",
                    (enqueue_status, job_id, delivery_id),
                )
                self._upsert_hook(
                    con,
                    hook_id=hook_id,
                    event_name=event_name,
                    action=action,
                    repository=repository,
                    delivery_id=delivery_id,
                    status=status,
                    payload=payload,
                    created_at=created_at,
                    hook_target=hook_target,
                )
        return WebhookReceipt(
            delivery_id=delivery_id,
            event_name=event_name,
            action=action,
            event_key=event_key,
            repository=repository,
            status="duplicate" if duplicate else status,
            enqueue_status=enqueue_status,
            job_id=job_id,
        )

    def first_receipt_created_at(self) -> str | None:
        with self.database.read_only() as con:
            row = con.execute(
                "SELECT MIN(created_at) first_created_at FROM webhook_shadow_receipts"
            ).fetchone()
        return str(row["first_created_at"]) if row and row["first_created_at"] else None

    def summary(self, window_start: str, window_end: str) -> dict[str, Any]:
        with self.database.read_only() as con:
            receipt = con.execute(
                "SELECT COALESCE(SUM(CASE WHEN status='observed' THEN 1 ELSE 0 END),0) observed,"
                "COALESCE(SUM(CASE WHEN status='unsupported' THEN 1 ELSE 0 END),0) unsupported,"
                "COALESCE(SUM(duplicate_count),0) duplicate_deliveries FROM webhook_shadow_receipts"
            ).fetchone()
            coverage = con.execute(
                "WITH email_events AS ("
                " SELECT event_key,MIN(created_at) created_at FROM ingest_receipts"
                f" WHERE source='email' AND julianday(created_at)>=julianday(?) AND julianday(created_at)<=julianday(?) AND {_coverage_predicate('event_key')}"
                " GROUP BY event_key"
                "), webhook_events AS ("
                " SELECT event_key,MIN(created_at) created_at FROM webhook_shadow_receipts"
                f" WHERE julianday(created_at)>=julianday(?) AND julianday(created_at)<=julianday(?) AND {_coverage_predicate('event_key')}"
                " GROUP BY event_key"
                ") SELECT"
                " (SELECT COUNT(*) FROM email_events) imap_events,"
                " (SELECT COUNT(*) FROM webhook_events) webhook_events,"
                " (SELECT COUNT(*) FROM email_events JOIN webhook_events USING(event_key)) both_events,"
                " (SELECT AVG(ABS((julianday(w.created_at)-julianday(i.created_at))*86400000.0))"
                "  FROM email_events i JOIN webhook_events w USING(event_key)) match_delay_ms",
                (window_start, window_end, window_start, window_end),
            ).fetchone()
            inventory = con.execute(
                "SELECT (SELECT COUNT(*) FROM webhook_hooks) hooks,"
                "(SELECT COUNT(*) FROM webhook_shadow_receipts) deliveries"
            ).fetchone()
            enqueue = {
                str(row["enqueue_status"]): int(row["count"])
                for row in con.execute(
                    "SELECT enqueue_status,COUNT(*) count FROM webhook_shadow_receipts "
                    "WHERE enqueue_status IS NOT NULL GROUP BY enqueue_status"
                )
            }
        return {
            "observed": int(receipt["observed"]),
            "unsupported": int(receipt["unsupported"]),
            "duplicate_deliveries": int(receipt["duplicate_deliveries"]),
            "imap_events": int(coverage["imap_events"]),
            "webhook_events": int(coverage["webhook_events"]),
            "both_events": int(coverage["both_events"]),
            "match_delay_ms": coverage["match_delay_ms"],
            "hooks": int(inventory["hooks"]),
            "deliveries": int(inventory["deliveries"]),
            "enqueue": enqueue,
        }

    def exceptions(
        self, window_start: str, window_end: str, limit: int
    ) -> list[dict[str, Any]]:
        with self.database.read_only() as con:
            rows = con.execute(
                "WITH email_events AS ("
                " SELECT event_key,source_key,created_at FROM ingest_receipts"
                f" WHERE source='email' AND julianday(created_at)>=julianday(?) AND julianday(created_at)<=julianday(?) AND {_coverage_predicate('event_key')}"
                "), webhook_events AS ("
                " SELECT event_key,delivery_id,created_at,repository FROM webhook_shadow_receipts"
                f" WHERE julianday(created_at)>=julianday(?) AND julianday(created_at)<=julianday(?) AND {_coverage_predicate('event_key')}"
                "), candidates AS ("
                " SELECT 'imap_only' kind,i.event_key,i.source_key reference,i.created_at,NULL repository "
                " FROM email_events i WHERE NOT EXISTS ("
                "  SELECT 1 FROM webhook_events w WHERE w.event_key=i.event_key"
                " ) UNION ALL "
                " SELECT 'webhook_only',w.event_key,w.delivery_id,w.created_at,w.repository "
                " FROM webhook_events w WHERE NOT EXISTS ("
                "  SELECT 1 FROM email_events i WHERE i.event_key=w.event_key"
                " ) UNION ALL "
                " SELECT 'unmatchable',NULL,w.delivery_id,w.created_at,w.repository "
                " FROM webhook_shadow_receipts w WHERE w.event_key IS NULL"
                " AND julianday(w.created_at)>=julianday(?) AND julianday(w.created_at)<=julianday(?)"
                ") SELECT kind,event_key,reference,created_at,repository FROM candidates "
                "ORDER BY created_at DESC LIMIT ?",
                (
                    window_start,
                    window_end,
                    window_start,
                    window_end,
                    window_start,
                    window_end,
                    limit,
                ),
            ).fetchall()
        return [dict(row) for row in rows]

    def timeseries(
        self, start: str, end: str, bucket: str
    ) -> list[dict[str, Any]]:
        expressions = {
            "hour": "substr(created_at,1,13) || ':00:00Z'",
            "day": "substr(created_at,1,10)",
        }
        try:
            bucket_expression = expressions[bucket]
        except KeyError as exc:
            raise ValueError("unsupported webhook timeseries bucket") from exc
        with self.database.read_only() as con:
            rows = con.execute(
                f"SELECT {bucket_expression} bucket, "
                "SUM(CASE WHEN status='observed' THEN 1 ELSE 0 END) observed, "
                "SUM(duplicate_count) duplicate, "
                "SUM(CASE WHEN status='unsupported' THEN 1 ELSE 0 END) unsupported "
                "FROM webhook_shadow_receipts WHERE created_at>=? AND created_at<? "
                "GROUP BY 1 ORDER BY 1",
                (start, end),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_hooks(
        self, limit: int, cursor: tuple[str, str] | None = None
    ) -> list[dict[str, Any]]:
        where = ""
        parameters: list[Any] = []
        if cursor:
            where = "WHERE updated_at<? OR (updated_at=? AND hook_id<?)"
            parameters.extend((cursor[0], cursor[0], cursor[1]))
        parameters.append(limit + 1)
        with self.database.read_only() as con:
            rows = con.execute(
                f"SELECT {self.HOOK_COLUMNS} FROM webhook_hooks {where} "
                "ORDER BY updated_at DESC,hook_id DESC LIMIT ?",
                parameters,
            ).fetchall()
        return [dict(row) for row in rows]

    def hook_detail(self, hook_id: str) -> dict[str, Any] | None:
        with self.database.read_only() as con:
            hook = con.execute(
                f"SELECT {self.HOOK_COLUMNS} FROM webhook_hooks WHERE hook_id=?",
                (hook_id,),
            ).fetchone()
            if hook is None:
                return None
            stats = con.execute(
                "SELECT COUNT(*) deliveries,COALESCE(SUM(duplicate_count),0) duplicates,"
                "SUM(CASE WHEN status='unsupported' THEN 1 ELSE 0 END) unsupported "
                "FROM webhook_shadow_receipts WHERE hook_id=?",
                (hook_id,),
            ).fetchone()
            recent = con.execute(
                f"SELECT {self.DELIVERY_COLUMNS} FROM webhook_shadow_receipts r "
                "LEFT JOIN webhook_hooks h ON h.hook_id=r.hook_id "
                "WHERE r.hook_id=? ORDER BY r.created_at DESC,r.delivery_id DESC LIMIT 20",
                (hook_id,),
            ).fetchall()
            actions = con.execute(
                "SELECT id,action,actor,status,detail,created_at,completed_at "
                "FROM webhook_hook_actions WHERE hook_id=? ORDER BY created_at DESC,id DESC LIMIT 10",
                (hook_id,),
            ).fetchall()
        return {
            "hook": dict(hook),
            "stats": dict(stats),
            "recent_deliveries": [dict(row) for row in recent],
            "recent_actions": [dict(row) for row in actions],
        }

    def ping_target(self, hook_id: str) -> dict[str, Any] | None:
        with self.database.read_only() as con:
            row = con.execute(
                "SELECT hook_id,target,target_type,ping_url FROM webhook_hooks WHERE hook_id=?",
                (hook_id,),
            ).fetchone()
        return dict(row) if row else None

    def create_hook_action(
        self, hook_id: str, action: str, actor: str, created_at: str
    ) -> int:
        with self.database.transaction() as con:
            cursor = con.execute(
                "INSERT INTO webhook_hook_actions(hook_id,action,actor,status,created_at) VALUES(?,?,?,?,?)",
                (hook_id, action, actor, "requested", created_at),
            )
            return int(cursor.lastrowid)

    def finish_hook_action(
        self, action_id: int, status: str, detail: str, completed_at: str
    ) -> None:
        with self.database.transaction() as con:
            con.execute(
                "UPDATE webhook_hook_actions SET status=?,detail=?,completed_at=? WHERE id=?",
                (status, detail[:1000], completed_at, action_id),
            )

    def list_deliveries(
        self,
        limit: int,
        *,
        cursor: tuple[str, str] | None = None,
        hook_id: str | None = None,
        event_name: str | None = None,
        repository: str | None = None,
        result: str | None = None,
        enqueue_status: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if cursor:
            clauses.append("(r.created_at<? OR (r.created_at=? AND r.delivery_id<?))")
            parameters.extend((cursor[0], cursor[0], cursor[1]))
        filters = (
            ("r.hook_id", hook_id),
            ("r.event_name", event_name),
            ("r.repository", repository),
            ("r.status", result),
            ("r.enqueue_status", enqueue_status),
        )
        for column, value in filters:
            if value:
                clauses.append(f"{column}=?")
                parameters.append(value)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        parameters.append(limit + 1)
        with self.database.read_only() as con:
            rows = con.execute(
                f"SELECT {self.DELIVERY_COLUMNS} FROM webhook_shadow_receipts r "
                f"LEFT JOIN webhook_hooks h ON h.hook_id=r.hook_id {where} "
                "ORDER BY r.created_at DESC,r.delivery_id DESC LIMIT ?",
                parameters,
            ).fetchall()
        return [dict(row) for row in rows]

    def delivery_detail(self, delivery_id: str) -> dict[str, Any] | None:
        with self.database.read_only() as con:
            row = con.execute(
                "SELECT r.delivery_id,r.hook_id,r.event_name,r.action,r.event_key,r.repository,r.status,"
                "r.enqueue_status,r.duplicate_count,r.created_at,r.payload_hash,r.payload_json,"
                "h.target hook_target,h.target_type hook_target_type,"
                "j.id job_id,j.work_key job_work_key,j.status job_status,j.action job_action,"
                "j.decision job_decision,j.work_intent job_work_intent,j.updated_at job_updated_at "
                "FROM webhook_shadow_receipts r "
                "LEFT JOIN webhook_hooks h ON h.hook_id=r.hook_id "
                "LEFT JOIN ingest_receipts i ON i.source='webhook' AND i.source_key=r.delivery_id "
                "LEFT JOIN jobs j ON j.id=COALESCE(r.job_id,i.job_id) WHERE r.delivery_id=?",
                (delivery_id,),
            ).fetchone()
        return dict(row) if row else None

    @staticmethod
    def _upsert_hook(
        con,
        *,
        hook_id: str | None,
        event_name: str,
        action: str | None,
        repository: str | None,
        delivery_id: str,
        status: str,
        payload: dict[str, Any],
        created_at: str,
        hook_target: tuple[str, str] | None,
    ) -> None:
        if not hook_id or hook_target is None:
            return
        target, target_type = hook_target
        hook = payload.get("hook") if isinstance(payload, dict) else None
        if event_name == "ping" and isinstance(hook, dict):
            config = hook.get("config") if isinstance(hook.get("config"), dict) else {}
            events = hook.get("events") if isinstance(hook.get("events"), list) else []
            insecure_ssl = config.get("insecure_ssl")
            insecure_ssl_value = (
                None if insecure_ssl is None else int(str(insecure_ssl) == "1")
            )
            con.execute(
                "INSERT INTO webhook_hooks("
                "hook_id,target,target_type,name,active,events_json,content_type,insecure_ssl,delivery_url,"
                "github_api_url,ping_url,deliveries_url,github_created_at,github_updated_at,last_ping_at,updated_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(hook_id) DO UPDATE SET "
                "target=excluded.target,target_type=excluded.target_type,name=excluded.name,active=excluded.active,"
                "events_json=excluded.events_json,content_type=excluded.content_type,insecure_ssl=excluded.insecure_ssl,"
                "delivery_url=excluded.delivery_url,github_api_url=excluded.github_api_url,ping_url=excluded.ping_url,"
                "deliveries_url=excluded.deliveries_url,github_created_at=excluded.github_created_at,"
                "github_updated_at=excluded.github_updated_at,last_ping_at=excluded.last_ping_at,updated_at=excluded.updated_at",
                (
                    hook_id,
                    target,
                    target_type,
                    str(hook.get("name") or "") or None,
                    int(bool(hook.get("active", True))),
                    json.dumps(events),
                    str(config.get("content_type") or "") or None,
                    insecure_ssl_value,
                    str(config.get("url") or "") or None,
                    str(hook.get("url") or "") or None,
                    str(hook.get("ping_url") or "") or None,
                    str(hook.get("deliveries_url") or "") or None,
                    str(hook.get("created_at") or "") or None,
                    str(hook.get("updated_at") or "") or None,
                    created_at,
                    created_at,
                ),
            )
        else:
            con.execute(
                "INSERT INTO webhook_hooks("
                "hook_id,target,target_type,active,events_json,last_event_at,last_delivery_id,last_event_name,"
                "last_action,last_repository,last_result,updated_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(hook_id) DO UPDATE SET "
                "last_event_at=excluded.last_event_at,last_delivery_id=excluded.last_delivery_id,"
                "last_event_name=excluded.last_event_name,last_action=excluded.last_action,"
                "last_repository=excluded.last_repository,last_result=excluded.last_result,updated_at=excluded.updated_at",
                (
                    hook_id,
                    target,
                    target_type,
                    1,
                    "[]",
                    created_at,
                    delivery_id,
                    event_name,
                    action,
                    repository,
                    status,
                    created_at,
                ),
            )
