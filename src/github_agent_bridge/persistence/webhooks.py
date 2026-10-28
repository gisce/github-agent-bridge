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
SPLIT_IDENTITY_MAX_DELAY_SECONDS = 600
WEBHOOK_OBSERVATION_STARTED_AT_KEY = "webhook_observation_started_at"


def _coverage_predicate(column: str) -> str:
    if column not in {"event_key", "i.event_key", "w.event_key"}:
        raise ValueError("unsupported webhook coverage column")
    return "(" + " OR ".join(
        f"{column} GLOB '{pattern}'" for pattern in WEBHOOK_COVERAGE_EVENT_GLOBS
    ) + ")"


def _email_coverage_predicate(event_key: str, job_alias: str) -> str:
    """Include comparable keys plus actionable email events lacking one.

    A source-specific event key is evidence of a parity gap when IMAP created
    executable work. Passive subscription mail remains outside the promotion
    gate so routine archive traffic cannot make webhook coverage look worse.
    """
    if job_alias != "j":
        raise ValueError("unsupported email coverage job alias")
    return (
        f"({_coverage_predicate(event_key)} OR "
        f"({job_alias}.decision='auto_trusted' "
        f"AND {job_alias}.action<>'archive_notification'))"
    )


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
            with self.database.transaction(
                TransactionMode.IMMEDIATE,
                operation="webhooks.persist_delivery",
            ) as con:
                con.execute(
                    "INSERT OR IGNORE INTO state(key,value) "
                    "SELECT ?,COALESCE(MIN(created_at),?) FROM webhook_shadow_receipts",
                    (WEBHOOK_OBSERVATION_STARTED_AT_KEY, created_at),
                )
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
            with self.database.transaction(
                TransactionMode.IMMEDIATE,
                operation="webhooks.persist_duplicate_delivery",
            ) as con:
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

    def observation_started_at(self) -> str | None:
        with self.database.read("webhooks.observation_started_at") as con:
            row = con.execute(
                "SELECT COALESCE("
                " (SELECT value FROM state WHERE key=?),"
                " (SELECT MIN(created_at) FROM webhook_shadow_receipts)"
                ") observation_started_at",
                (WEBHOOK_OBSERVATION_STARTED_AT_KEY,),
            ).fetchone()
        return (
            str(row["observation_started_at"])
            if row and row["observation_started_at"]
            else None
        )

    def summary(self, window_start: str, window_end: str) -> dict[str, Any]:
        with self.database.read("webhooks.summary") as con:
            receipt = con.execute(
                "SELECT COALESCE(SUM(CASE WHEN status='observed' THEN 1 ELSE 0 END),0) observed,"
                "COALESCE(SUM(CASE WHEN status='unsupported' THEN 1 ELSE 0 END),0) unsupported,"
                "COALESCE(SUM(duplicate_count),0) duplicate_deliveries "
                "FROM webhook_shadow_receipts INDEXED BY idx_webhook_shadow_summary"
            ).fetchone()
            coverage = con.execute(
                "WITH email_events AS ("
                " SELECT i.event_key,MIN(i.created_at) created_at,MIN(i.work_key) work_key,"
                " MIN(i.event_type) event_type,MIN(i.event_action) event_action FROM ingest_receipts i"
                " LEFT JOIN jobs j ON j.id=i.job_id"
                f" WHERE i.source='email' AND julianday(i.created_at)>=julianday(?) AND julianday(i.created_at)<=julianday(?) AND {_email_coverage_predicate('i.event_key', 'j')}"
                " GROUP BY i.event_key"
                "), webhook_events AS ("
                " SELECT w.event_key,MIN(w.created_at) created_at,"
                " MIN(COALESCE(j.work_key,w.repository || '#' || COALESCE("
                " json_extract(w.payload_json,'$.issue.number'),"
                " json_extract(w.payload_json,'$.pull_request.number')))) work_key,"
                " MIN(w.event_name) event_type,MIN(CASE WHEN w.action='submitted' THEN 'created' ELSE w.action END) event_action"
                " FROM webhook_shadow_receipts w INDEXED BY idx_webhook_shadow_coverage"
                " LEFT JOIN jobs j ON j.id=w.job_id"
                f" WHERE julianday(w.created_at)>=julianday(?) AND julianday(w.created_at)<=julianday(?) AND {_coverage_predicate('w.event_key')}"
                " GROUP BY w.event_key"
                "), decision_traces AS ("
                " SELECT e.event_key,"
                " MAX(CASE WHEN e.bridge_action IS NOT NULL AND w.bridge_action IS NOT NULL"
                " AND e.work_intent IS NOT NULL AND w.work_intent IS NOT NULL"
                " AND e.work_key IS NOT NULL AND w.work_key IS NOT NULL THEN 1 ELSE 0 END) comparable,"
                " MAX(CASE WHEN e.bridge_action IS NOT NULL AND w.bridge_action IS NOT NULL"
                " AND e.work_intent IS NOT NULL AND w.work_intent IS NOT NULL"
                " AND e.work_key IS NOT NULL AND w.work_key IS NOT NULL"
                " AND (e.bridge_action<>w.bridge_action OR e.work_intent<>w.work_intent"
                " OR e.work_key<>w.work_key) THEN 1 ELSE 0 END) mismatch"
                " FROM ingest_receipts e JOIN ingest_receipts w USING(event_key)"
                " WHERE e.source='email' AND w.source='webhook'"
                " AND julianday(e.created_at)>=julianday(?) AND julianday(e.created_at)<=julianday(?)"
                " AND julianday(w.created_at)>=julianday(?) AND julianday(w.created_at)<=julianday(?)"
                " GROUP BY e.event_key"
                "), matched_events AS ("
                " SELECT e.event_key,COALESCE(d.comparable,0) decision_comparable"
                " FROM email_events e JOIN webhook_events w USING(event_key)"
                " LEFT JOIN decision_traces d USING(event_key)"
                "), split_identities AS ("
                " SELECT e.event_key,w.event_key FROM ingest_receipts e JOIN ingest_receipts w"
                " ON e.source='email' AND w.source='webhook'"
                " AND e.event_key GLOB 'email:*' AND w.event_key GLOB 'webhook:*'"
                " AND e.work_key=w.work_key AND e.event_type=w.event_type"
                " AND e.event_action=w.event_action"
                " WHERE e.work_key IS NOT NULL AND e.event_type IS NOT NULL"
                " AND e.event_action IS NOT NULL"
                " AND julianday(e.created_at)>=julianday(?) AND julianday(e.created_at)<=julianday(?)"
                " AND julianday(w.created_at)>=julianday(?) AND julianday(w.created_at)<=julianday(?)"
                " AND ABS((julianday(w.created_at)-julianday(e.created_at))*86400.0)<=?"
                " GROUP BY e.event_key,w.event_key"
                ") SELECT"
                " (SELECT COUNT(*) FROM email_events) imap_events,"
                " (SELECT COUNT(*) FROM webhook_events) webhook_events,"
                " (SELECT COUNT(*) FROM matched_events) both_events,"
                " (SELECT COALESCE(SUM(mismatch),0) FROM decision_traces) decision_mismatches,"
                " (SELECT COUNT(*) FROM matched_events WHERE decision_comparable=0) incomplete_decision_traces,"
                " (SELECT COUNT(*) FROM split_identities) split_identities,"
                " (SELECT AVG(ABS((julianday(w.created_at)-julianday(i.created_at))*86400000.0))"
                "  FROM email_events i JOIN webhook_events w USING(event_key)) match_delay_ms",
                (
                    window_start,
                    window_end,
                    window_start,
                    window_end,
                    window_start,
                    window_end,
                    window_start,
                    window_end,
                    window_start,
                    window_end,
                    window_start,
                    window_end,
                    SPLIT_IDENTITY_MAX_DELAY_SECONDS,
                ),
            ).fetchone()
            inventory = con.execute(
                "SELECT (SELECT COUNT(*) FROM webhook_hooks) hooks,"
                "(SELECT COUNT(*) FROM webhook_shadow_receipts) deliveries"
            ).fetchone()
            enqueue = {
                str(row["enqueue_status"]): int(row["count"])
                for row in con.execute(
                    "SELECT enqueue_status,COUNT(*) count FROM webhook_shadow_receipts "
                    "INDEXED BY idx_webhook_shadow_summary "
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
            "decision_mismatches": int(coverage["decision_mismatches"]),
            "incomplete_decision_traces": int(coverage["incomplete_decision_traces"]),
            "split_identities": int(coverage["split_identities"]),
            "match_delay_ms": coverage["match_delay_ms"],
            "hooks": int(inventory["hooks"]),
            "deliveries": int(inventory["deliveries"]),
            "enqueue": enqueue,
        }

    def exceptions(
        self, window_start: str, window_end: str, limit: int
    ) -> list[dict[str, Any]]:
        with self.database.read("webhooks.exceptions") as con:
            rows = con.execute(
                "WITH email_events AS MATERIALIZED ("
                " SELECT i.event_key,MIN(i.source_key) reference,MIN(i.created_at) created_at,"
                " MIN(COALESCE(i.repository,j.repo)) repository,"
                " MIN(i.event_type) event_type,MIN(i.event_action) event_action"
                " FROM ingest_receipts i"
                " LEFT JOIN jobs j ON j.id=i.job_id"
                f" WHERE i.source='email' AND julianday(i.created_at)>=julianday(?) AND julianday(i.created_at)<=julianday(?) AND {_email_coverage_predicate('i.event_key', 'j')}"
                " GROUP BY i.event_key"
                "), webhook_events AS MATERIALIZED ("
                " SELECT event_key,MIN(delivery_id) reference,MIN(created_at) created_at,"
                " MIN(repository) repository,MIN(event_name) event_type,"
                " MIN(CASE WHEN action='submitted' THEN 'created' ELSE action END) event_action"
                " FROM webhook_shadow_receipts INDEXED BY idx_webhook_shadow_coverage"
                f" WHERE julianday(created_at)>=julianday(?) AND julianday(created_at)<=julianday(?) AND {_coverage_predicate('event_key')}"
                " GROUP BY event_key"
                "), decision_traces AS ("
                " SELECT e.event_key,MIN(e.source_key) reference,"
                " MIN(w.source_key) related_reference,MIN(e.created_at) created_at,"
                " MIN(COALESCE(e.repository,w.repository)) repository,"
                " MIN(COALESCE(e.event_type,w.event_type)) event_type,"
                " MIN(COALESCE(e.event_action,w.event_action)) event_action,"
                " MAX(CASE WHEN e.bridge_action IS NOT NULL AND w.bridge_action IS NOT NULL"
                " AND e.work_intent IS NOT NULL AND w.work_intent IS NOT NULL"
                " AND e.work_key IS NOT NULL AND w.work_key IS NOT NULL THEN 1 ELSE 0 END) comparable,"
                " MAX(CASE WHEN e.bridge_action IS NOT NULL AND w.bridge_action IS NOT NULL"
                " AND e.work_intent IS NOT NULL AND w.work_intent IS NOT NULL"
                " AND e.work_key IS NOT NULL AND w.work_key IS NOT NULL"
                " AND (e.bridge_action<>w.bridge_action OR e.work_intent<>w.work_intent"
                " OR e.work_key<>w.work_key) THEN 1 ELSE 0 END) mismatch"
                " FROM ingest_receipts e JOIN ingest_receipts w USING(event_key)"
                " WHERE e.source='email' AND w.source='webhook'"
                " AND julianday(e.created_at)>=julianday(?) AND julianday(e.created_at)<=julianday(?)"
                " AND julianday(w.created_at)>=julianday(?) AND julianday(w.created_at)<=julianday(?)"
                " GROUP BY e.event_key"
                "), incomplete_traces AS ("
                " SELECT e.event_key,e.reference,w.reference related_reference,"
                " CASE WHEN e.created_at>w.created_at THEN e.created_at ELSE w.created_at END created_at,"
                " COALESCE(e.repository,w.repository) repository,"
                " COALESCE(e.event_type,w.event_type) event_type,"
                " COALESCE(e.event_action,w.event_action) event_action"
                " FROM email_events e JOIN webhook_events w USING(event_key)"
                " LEFT JOIN decision_traces d USING(event_key)"
                " WHERE COALESCE(d.comparable,0)=0"
                "), split_pairs AS ("
                " SELECT e.event_key,w.event_key related_event_key,e.source_key reference,"
                " w.source_key related_reference,"
                " CASE WHEN e.created_at>w.created_at THEN e.created_at ELSE w.created_at END created_at,"
                " COALESCE(e.repository,w.repository) repository,"
                " COALESCE(e.event_type,w.event_type) event_type,"
                " COALESCE(e.event_action,w.event_action) event_action"
                " FROM ingest_receipts e JOIN ingest_receipts w"
                " ON e.source='email' AND w.source='webhook'"
                " AND e.event_key GLOB 'email:*' AND w.event_key GLOB 'webhook:*'"
                " AND e.work_key=w.work_key AND e.event_type=w.event_type"
                " AND e.event_action=w.event_action"
                " WHERE e.work_key IS NOT NULL AND e.event_type IS NOT NULL"
                " AND e.event_action IS NOT NULL"
                " AND julianday(e.created_at)>=julianday(?) AND julianday(e.created_at)<=julianday(?)"
                " AND julianday(w.created_at)>=julianday(?) AND julianday(w.created_at)<=julianday(?)"
                " AND ABS((julianday(w.created_at)-julianday(e.created_at))*86400.0)<=?"
                " GROUP BY e.event_key,w.event_key"
                "), candidates AS ("
                " SELECT 'imap_only' kind,i.event_key,NULL related_event_key,i.reference,"
                " NULL related_reference,i.created_at,i.repository,i.event_type,i.event_action"
                " FROM email_events i LEFT JOIN webhook_events w USING(event_key)"
                " WHERE w.event_key IS NULL UNION ALL "
                " SELECT 'webhook_only',w.event_key,NULL,w.reference,NULL,w.created_at,"
                " w.repository,w.event_type,w.event_action"
                " FROM webhook_events w LEFT JOIN email_events i USING(event_key)"
                " WHERE i.event_key IS NULL UNION ALL "
                " SELECT 'unmatchable',NULL,NULL,w.delivery_id,NULL,w.created_at,"
                " w.repository,w.event_name,w.action"
                " FROM webhook_shadow_receipts w INDEXED BY idx_webhook_shadow_coverage WHERE w.event_key IS NULL"
                " AND julianday(w.created_at)>=julianday(?) AND julianday(w.created_at)<=julianday(?)"
                " UNION ALL SELECT 'decision_mismatch',event_key,NULL,reference,"
                " related_reference,created_at,repository,event_type,event_action"
                " FROM decision_traces WHERE mismatch=1 UNION ALL "
                " SELECT 'incomplete_trace',event_key,NULL,reference,"
                " related_reference,created_at,repository,event_type,event_action"
                " FROM incomplete_traces UNION ALL "
                " SELECT 'split_identity',event_key,related_event_key,reference,"
                " related_reference,created_at,repository,event_type,event_action"
                " FROM split_pairs"
                ") SELECT kind,event_key,related_event_key,reference,related_reference,"
                "created_at,repository,event_type,event_action FROM candidates "
                "ORDER BY created_at DESC LIMIT ?",
                (
                    window_start,
                    window_end,
                    window_start,
                    window_end,
                    window_start,
                    window_end,
                    window_start,
                    window_end,
                    window_start,
                    window_end,
                    window_start,
                    window_end,
                    SPLIT_IDENTITY_MAX_DELAY_SECONDS,
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
        with self.database.read("webhooks.timeseries") as con:
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
        with self.database.read("webhooks.list_hooks") as con:
            rows = con.execute(
                f"SELECT {self.HOOK_COLUMNS} FROM webhook_hooks {where} "
                "ORDER BY updated_at DESC,hook_id DESC LIMIT ?",
                parameters,
            ).fetchall()
        return [dict(row) for row in rows]

    def hook_detail(self, hook_id: str) -> dict[str, Any] | None:
        with self.database.read("webhooks.hook_detail") as con:
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
        with self.database.read("webhooks.ping_target") as con:
            row = con.execute(
                "SELECT hook_id,target,target_type,ping_url FROM webhook_hooks WHERE hook_id=?",
                (hook_id,),
            ).fetchone()
        return dict(row) if row else None

    def create_hook_action(
        self, hook_id: str, action: str, actor: str, created_at: str
    ) -> int:
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="webhooks.create_hook_action",
        ) as con:
            cursor = con.execute(
                "INSERT INTO webhook_hook_actions(hook_id,action,actor,status,created_at) VALUES(?,?,?,?,?)",
                (hook_id, action, actor, "requested", created_at),
            )
            return int(cursor.lastrowid)

    def finish_hook_action(
        self, action_id: int, status: str, detail: str, completed_at: str
    ) -> None:
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="webhooks.finish_hook_action",
        ) as con:
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
        with self.database.read("webhooks.list_deliveries") as con:
            rows = con.execute(
                f"SELECT {self.DELIVERY_COLUMNS} FROM webhook_shadow_receipts r "
                f"LEFT JOIN webhook_hooks h ON h.hook_id=r.hook_id {where} "
                "ORDER BY r.created_at DESC,r.delivery_id DESC LIMIT ?",
                parameters,
            ).fetchall()
        return [dict(row) for row in rows]

    def delivery_detail(self, delivery_id: str) -> dict[str, Any] | None:
        with self.database.read("webhooks.delivery_detail") as con:
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
