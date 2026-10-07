from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .database import Database, TransactionMode


@dataclass(frozen=True)
class FeedbackEvent:
    id: str
    occurred_at: str
    captured_at: str
    source: str
    scope: str
    actor: str
    comment: str
    context: dict[str, Any]
    classification: str
    confidence: float
    memorable: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "occurred_at": self.occurred_at,
            "captured_at": self.captured_at,
            "source": self.source,
            "scope": self.scope,
            "actor": self.actor,
            "comment": self.comment,
            "context": self.context,
            "classification": self.classification,
            "confidence": self.confidence,
            "memorable": self.memorable,
        }


@dataclass(frozen=True)
class FeedbackProposal:
    id: str
    event_id: str
    created_at: str
    updated_at: str
    status: str
    scope: str
    type: str
    confidence: float
    rule: str
    reason: str
    model: str
    error: str | None

    def to_dict(
        self, source_event: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return {
            "id": self.id,
            "event_id": self.event_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "status": self.status,
            "scope": self.scope,
            "type": self.type,
            "confidence": self.confidence,
            "rule": self.rule,
            "reason": self.reason,
            "model": self.model,
            "error": self.error,
            "source_event": source_event,
        }


@dataclass(frozen=True)
class FeedbackRule:
    id: str
    scope: str
    type: str
    confidence: float
    rule: str
    created_at: str
    last_seen: str
    source_events: list[str]
    observations: int

    def to_dict(
        self, source_event_details: list[dict[str, Any]] | None = None
    ) -> dict[str, Any]:
        return {
            "id": self.id,
            "scope": self.scope,
            "type": self.type,
            "confidence": self.confidence,
            "rule": self.rule,
            "created_at": self.created_at,
            "last_seen": self.last_seen,
            "source_events": self.source_events,
            "source_event_details": source_event_details or [],
            "observations": self.observations,
        }


class FeedbackRepository:
    """Persist feedback evidence, proposals and curated rules."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def capture(self, event: FeedbackEvent) -> bool:
        with self.database.transaction() as con:
            cursor = con.execute(
                """INSERT OR IGNORE INTO feedback_events(
                    id, occurred_at, captured_at, source, scope, actor, comment,
                    context_json, classification, confidence, memorable
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    event.id,
                    event.occurred_at,
                    event.captured_at,
                    event.source,
                    event.scope,
                    event.actor,
                    event.comment,
                    json.dumps(
                        event.context, ensure_ascii=False, sort_keys=True
                    ),
                    event.classification,
                    event.confidence,
                    int(event.memorable),
                ),
            )
            return bool(cursor.rowcount)

    def get_event(self, event_id: str) -> FeedbackEvent | None:
        with self.database.read_only() as con:
            row = con.execute(
                "SELECT * FROM feedback_events WHERE id=?", (event_id,)
            ).fetchone()
        return self._event_from_row(row) if row else None

    def pending_events(
        self, scope: str = "", limit: int = 10
    ) -> list[FeedbackEvent]:
        clauses = [
            """NOT EXISTS (
                SELECT 1 FROM feedback_rule_proposals p
                WHERE p.event_id=feedback_events.id AND p.status != 'error'
            )"""
        ]
        args: list[Any] = []
        if scope:
            clauses.append("(scope=? OR scope LIKE ?)")
            args.extend([scope, f"{scope}:%"])
        sql = (
            "SELECT * FROM feedback_events WHERE "
            + " AND ".join(clauses)
            + " ORDER BY occurred_at ASC, id ASC LIMIT ?"
        )
        args.append(limit)
        with self.database.read_only() as con:
            rows = con.execute(sql, args).fetchall()
        return [self._event_from_row(row) for row in rows]

    def list_events(
        self, scope: str = "", limit: int = 20
    ) -> list[FeedbackEvent]:
        args: list[Any] = []
        sql = "SELECT * FROM feedback_events"
        if scope:
            sql += " WHERE scope=? OR scope LIKE ?"
            args.extend([scope, f"{scope}:%"])
        sql += " ORDER BY occurred_at DESC, id DESC LIMIT ?"
        args.append(limit)
        with self.database.read_only() as con:
            rows = con.execute(sql, args).fetchall()
        return [self._event_from_row(row) for row in rows]

    def update_event_context(
        self, event_id: str, context: dict[str, Any]
    ) -> bool:
        with self.database.transaction() as con:
            cursor = con.execute(
                "UPDATE feedback_events SET context_json=? WHERE id=?",
                (
                    json.dumps(context, ensure_ascii=False, sort_keys=True),
                    event_id,
                ),
            )
            return bool(cursor.rowcount)

    def source_for_message_id(self, message_id: str | None) -> dict[str, Any]:
        if not message_id:
            return {}
        with self.database.read_only() as con:
            row = con.execute(
                "SELECT id, trigger_actor, trigger_actor_avatar_url, context_json "
                "FROM jobs WHERE message_id=? ORDER BY id DESC LIMIT 1",
                (message_id,),
            ).fetchone()
            if row:
                return {**dict(row), "source_table": "jobs"}
            row = con.execute(
                "SELECT id, job_id, trigger_actor, trigger_actor_avatar_url, context_json "
                "FROM coalesced_notifications WHERE message_id=? ORDER BY id DESC LIMIT 1",
                (message_id,),
            ).fetchone()
        return (
            {**dict(row), "source_table": "coalesced_notifications"}
            if row
            else {}
        )

    def upsert_rule(
        self,
        *,
        rule_id: str,
        scope: str,
        rule_type: str,
        rule: str,
        confidence: float,
        source_events: list[str],
        now: str,
    ) -> FeedbackRule:
        with self.database.transaction(TransactionMode.IMMEDIATE) as con:
            row = con.execute(
                "SELECT * FROM feedback_rules WHERE id=?", (rule_id,)
            ).fetchone()
            if row:
                events = sorted(
                    set(
                        json.loads(row["source_events_json"] or "[]")
                        + source_events
                    )
                )
                confidence = max(float(row["confidence"]), confidence)
                observations = int(row["observations"]) + 1
                con.execute(
                    """UPDATE feedback_rules
                    SET confidence=?, last_seen=?, source_events_json=?, observations=?
                    WHERE id=?""",
                    (
                        confidence,
                        now,
                        json.dumps(
                            events, ensure_ascii=False, sort_keys=True
                        ),
                        observations,
                        rule_id,
                    ),
                )
            else:
                con.execute(
                    """INSERT INTO feedback_rules(
                        id, scope, type, confidence, rule, created_at, last_seen,
                        source_events_json, observations
                    ) VALUES(?,?,?,?,?,?,?,?,?)""",
                    (
                        rule_id,
                        scope,
                        rule_type,
                        confidence,
                        rule,
                        now,
                        now,
                        json.dumps(
                            source_events, ensure_ascii=False, sort_keys=True
                        ),
                        1,
                    ),
                )
            stored = con.execute(
                "SELECT * FROM feedback_rules WHERE id=?", (rule_id,)
            ).fetchone()
            return self._rule_from_row(stored)

    def get_rule(self, rule_id: str) -> FeedbackRule | None:
        with self.database.read_only() as con:
            row = con.execute(
                "SELECT * FROM feedback_rules WHERE id=?", (rule_id,)
            ).fetchone()
        return self._rule_from_row(row) if row else None

    def delete_rule(self, rule_id: str) -> bool:
        with self.database.transaction() as con:
            cursor = con.execute(
                "DELETE FROM feedback_rules WHERE id=?", (rule_id,)
            )
            return cursor.rowcount > 0

    def move_rule(
        self, rule_id: str, new_id: str, new_scope: str
    ) -> FeedbackRule | None:
        with self.database.transaction(TransactionMode.IMMEDIATE) as con:
            row = con.execute(
                "SELECT * FROM feedback_rules WHERE id=?", (rule_id,)
            ).fetchone()
            if not row:
                return None
            if row["scope"] == new_scope:
                return self._rule_from_row(row)
            existing = con.execute(
                "SELECT * FROM feedback_rules WHERE id=?", (new_id,)
            ).fetchone()
            if existing and existing["id"] != rule_id:
                source_events = json.loads(row["source_events_json"] or "[]")
                merged_events = sorted(
                    set(
                        json.loads(existing["source_events_json"] or "[]")
                        + source_events
                    )
                )
                con.execute(
                    """UPDATE feedback_rules
                    SET confidence=?, created_at=?, last_seen=?,
                        source_events_json=?, observations=? WHERE id=?""",
                    (
                        max(
                            float(existing["confidence"]),
                            float(row["confidence"]),
                        ),
                        min(str(existing["created_at"]), str(row["created_at"])),
                        max(str(existing["last_seen"]), str(row["last_seen"])),
                        json.dumps(
                            merged_events, ensure_ascii=False, sort_keys=True
                        ),
                        int(existing["observations"])
                        + int(row["observations"]),
                        existing["id"],
                    ),
                )
                con.execute(
                    "DELETE FROM feedback_rules WHERE id=?", (rule_id,)
                )
            else:
                con.execute(
                    "UPDATE feedback_rules SET id=?, scope=? WHERE id=?",
                    (new_id, new_scope, rule_id),
                )
            stored = con.execute(
                "SELECT * FROM feedback_rules WHERE id=?", (new_id,)
            ).fetchone()
            return self._rule_from_row(stored) if stored else None

    def list_rules(
        self,
        *,
        scopes: list[str] | None = None,
        min_confidence: float | None = None,
    ) -> list[FeedbackRule]:
        clauses: list[str] = []
        args: list[Any] = []
        if scopes:
            scope_clauses = []
            for scope in scopes:
                scope_clauses.append("(scope=? OR scope LIKE ?)")
                args.extend([scope, f"{scope}:%"])
            clauses.append("(" + " OR ".join(scope_clauses) + ")")
        if min_confidence is not None:
            clauses.append("confidence>=?")
            args.append(min_confidence)
        sql = "SELECT * FROM feedback_rules"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += (
            " ORDER BY last_seen DESC, created_at DESC, scope ASC, "
            "type ASC, rule ASC"
        )
        with self.database.read_only() as con:
            rows = con.execute(sql, args).fetchall()
        return [self._rule_from_row(row) for row in rows]

    def store_proposal(self, proposal: FeedbackProposal) -> None:
        with self.database.transaction() as con:
            con.execute(
                """INSERT OR REPLACE INTO feedback_rule_proposals(
                    id, event_id, created_at, updated_at, status, scope, type,
                    confidence, rule, reason, model, error
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    proposal.id,
                    proposal.event_id,
                    proposal.created_at,
                    proposal.updated_at,
                    proposal.status,
                    proposal.scope,
                    proposal.type,
                    proposal.confidence,
                    proposal.rule,
                    proposal.reason,
                    proposal.model,
                    proposal.error,
                ),
            )

    def get_proposal(self, proposal_id: str) -> FeedbackProposal | None:
        with self.database.read_only() as con:
            row = con.execute(
                "SELECT * FROM feedback_rule_proposals WHERE id=?",
                (proposal_id,),
            ).fetchone()
        return self._proposal_from_row(row) if row else None

    def set_proposal_status(
        self, proposal_id: str, status: str, now: str
    ) -> FeedbackProposal | None:
        with self.database.transaction() as con:
            cursor = con.execute(
                "UPDATE feedback_rule_proposals SET status=?, updated_at=?, "
                "error=CASE WHEN ?='approved' THEN NULL ELSE error END WHERE id=?",
                (status, now, status, proposal_id),
            )
            if not cursor.rowcount:
                return None
            row = con.execute(
                "SELECT * FROM feedback_rule_proposals WHERE id=?",
                (proposal_id,),
            ).fetchone()
            return self._proposal_from_row(row)

    def list_proposals(
        self, status: str = "", limit: int = 20
    ) -> list[FeedbackProposal]:
        args: list[Any] = []
        sql = "SELECT * FROM feedback_rule_proposals"
        if status:
            sql += " WHERE status=?"
            args.append(status)
        sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
        args.append(limit)
        with self.database.read_only() as con:
            rows = con.execute(sql, args).fetchall()
        return [self._proposal_from_row(row) for row in rows]

    def list_repositories(self) -> list[str]:
        with self.database.read_only() as con:
            rows = con.execute(
                """SELECT scope FROM feedback_events
                UNION SELECT scope FROM feedback_rules
                UNION SELECT scope FROM feedback_rule_proposals"""
            ).fetchall()
        repos = {
            str(row["scope"]).removeprefix("repo:")
            for row in rows
            if str(row["scope"]).startswith("repo:")
            and str(row["scope"]).removeprefix("repo:")
        }
        return sorted(repos)

    @staticmethod
    def _event_from_row(row) -> FeedbackEvent:
        return FeedbackEvent(
            id=str(row["id"]),
            occurred_at=str(row["occurred_at"]),
            captured_at=str(row["captured_at"]),
            source=str(row["source"]),
            scope=str(row["scope"]),
            actor=str(row["actor"]),
            comment=str(row["comment"]),
            context=json.loads(row["context_json"] or "{}"),
            classification=str(row["classification"]),
            confidence=float(row["confidence"]),
            memorable=bool(row["memorable"]),
        )

    @staticmethod
    def _proposal_from_row(row) -> FeedbackProposal:
        return FeedbackProposal(
            id=str(row["id"]),
            event_id=str(row["event_id"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            status=str(row["status"]),
            scope=str(row["scope"]),
            type=str(row["type"]),
            confidence=float(row["confidence"]),
            rule=str(row["rule"]),
            reason=str(row["reason"]),
            model=str(row["model"]),
            error=row["error"],
        )

    @staticmethod
    def _rule_from_row(row) -> FeedbackRule:
        return FeedbackRule(
            id=str(row["id"]),
            scope=str(row["scope"]),
            type=str(row["type"]),
            confidence=float(row["confidence"]),
            rule=str(row["rule"]),
            created_at=str(row["created_at"]),
            last_seen=str(row["last_seen"]),
            source_events=json.loads(row["source_events_json"] or "[]"),
            observations=int(row["observations"]),
        )
