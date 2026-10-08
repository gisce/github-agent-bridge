from __future__ import annotations

from dataclasses import dataclass

from ..sql.migrations import validate_migrations
from .database import Database


@dataclass(frozen=True)
class ActorBackfillCandidate:
    job_id: int
    context_json: str
    trigger_actor: str | None
    trigger_actor_avatar_url: str | None


@dataclass(frozen=True)
class ActorBackfillUpdate:
    job_id: int
    trigger_actor: str
    trigger_actor_avatar_url: str | None


class ActorBackfillRepository:
    """Read and update persisted trigger-actor data for the operator backfill."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def validate_current_schema(self) -> None:
        with self.database.read_only() as con:
            validate_migrations(con)

    def list_candidates(self, limit: int | None = None) -> list[ActorBackfillCandidate]:
        with self.database.read_only() as con:
            jobs_table = con.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='jobs'"
            ).fetchone()
            if jobs_table is None:
                return []
            columns = {
                str(row["name"])
                for row in con.execute("PRAGMA table_info(jobs)")
            }
            has_actor = "trigger_actor" in columns
            has_avatar = "trigger_actor_avatar_url" in columns
            conditions = []
            if has_actor:
                conditions.append("(trigger_actor IS NULL OR trigger_actor='')")
            if has_avatar:
                conditions.append(
                    "(trigger_actor_avatar_url IS NULL OR trigger_actor_avatar_url='')"
                )
            where = f"WHERE {' OR '.join(conditions)}" if conditions else ""
            actor_column = "trigger_actor" if has_actor else "NULL AS trigger_actor"
            avatar_column = (
                "trigger_actor_avatar_url"
                if has_avatar
                else "NULL AS trigger_actor_avatar_url"
            )
            rows = con.execute(
                f"""SELECT id, context_json, {actor_column}, {avatar_column}
                FROM jobs
                {where}
                ORDER BY id
                LIMIT ?""",
                (max(1, limit or 1000000),),
            ).fetchall()
        return [
            ActorBackfillCandidate(
                job_id=int(row["id"]),
                context_json=str(row["context_json"]),
                trigger_actor=(
                    str(row["trigger_actor"]) if row["trigger_actor"] else None
                ),
                trigger_actor_avatar_url=(
                    str(row["trigger_actor_avatar_url"])
                    if row["trigger_actor_avatar_url"]
                    else None
                ),
            )
            for row in rows
        ]

    def apply_updates(self, updates: list[ActorBackfillUpdate]) -> None:
        if not updates:
            return
        with self.database.transaction() as con:
            con.executemany(
                "UPDATE jobs SET trigger_actor=?, trigger_actor_avatar_url=? WHERE id=?",
                [
                    (
                        update.trigger_actor,
                        update.trigger_actor_avatar_url,
                        update.job_id,
                    )
                    for update in updates
                ],
            )
