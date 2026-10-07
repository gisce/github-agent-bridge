from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from ..models import utc_now
from .database import Database


EXECUTOR_PAUSE_STATE_KEY = "executor_paused"


@dataclass(frozen=True)
class ExecutorPauseState:
    """Typed representation of the executor pause state stored in SQLite."""

    paused: bool
    reason: str | None = None
    updated_at: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        state: dict[str, object] = {"paused": self.paused}
        if self.reason:
            state["reason"] = self.reason
        if self.updated_at:
            state["updated_at"] = self.updated_at
        if self.error:
            state["error"] = self.error
        return state


class StateRepository:
    """Persist bridge and executor state without owning orchestration policy."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def set(self, key: str, value: str) -> None:
        with self.database.transaction() as con:
            con.execute(
                "INSERT INTO state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def get(
        self,
        key: str,
        default: str = "",
        *,
        connection: sqlite3.Connection | None = None,
    ) -> str:
        if connection is not None:
            return self._get(connection, key, default)
        with self.database.read_only() as con:
            return self._get(con, key, default)

    def pause_executor(self, reason: str = "") -> None:
        self.set(
            EXECUTOR_PAUSE_STATE_KEY,
            json.dumps(
                {"paused": True, "reason": reason, "updated_at": utc_now()},
                sort_keys=True,
            ),
        )

    def resume_executor(self) -> None:
        self.set(
            EXECUTOR_PAUSE_STATE_KEY,
            json.dumps({"paused": False, "updated_at": utc_now()}, sort_keys=True),
        )

    def executor_pause_state(
        self,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> ExecutorPauseState:
        raw = self.get(EXECUTOR_PAUSE_STATE_KEY, "", connection=connection)
        if not raw:
            return ExecutorPauseState(paused=False)
        try:
            state = json.loads(raw)
        except json.JSONDecodeError:
            return ExecutorPauseState(paused=False, error="invalid_executor_pause_state")
        if not isinstance(state, dict):
            return ExecutorPauseState(paused=False, error="invalid_executor_pause_state")
        return ExecutorPauseState(
            paused=bool(state.get("paused")),
            reason=str(state["reason"]) if state.get("reason") else None,
            updated_at=str(state["updated_at"]) if state.get("updated_at") else None,
        )

    def executor_paused(
        self,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> bool:
        return self.executor_pause_state(connection=connection).paused

    @staticmethod
    def _get(con: sqlite3.Connection, key: str, default: str) -> str:
        row = con.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default
