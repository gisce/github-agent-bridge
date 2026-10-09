from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .database import Database, TransactionMode


@dataclass(frozen=True)
class WebPushSubscription:
    id: int
    user_login: str
    endpoint: str
    subscription: dict[str, Any]
    updated_at: str
    last_success_at: str | None
    last_error: str | None
    disabled_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "user_login": self.user_login,
            "endpoint": self.endpoint,
            "updated_at": self.updated_at,
            "last_success_at": self.last_success_at,
            "last_error": self.last_error,
            "disabled_at": self.disabled_at,
        }


class WebPushRepository:
    """Persist web-push subscriptions and delivery outcomes."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def save(
        self,
        user_login: str,
        endpoint: str,
        subscription: dict[str, Any],
        now: str,
    ) -> WebPushSubscription:
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="web_push.save",
        ) as con:
            con.execute(
                """INSERT INTO web_push_subscriptions(
                    user_login, endpoint, subscription_json, created_at, updated_at,
                    disabled_at, last_error
                ) VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(endpoint) DO UPDATE SET
                  user_login=excluded.user_login,
                  subscription_json=excluded.subscription_json,
                  updated_at=excluded.updated_at,
                  disabled_at=NULL,
                  last_error=NULL""",
                (
                    user_login,
                    endpoint,
                    json.dumps(subscription, sort_keys=True),
                    now,
                    now,
                    None,
                    None,
                ),
            )
            row = con.execute(
                "SELECT * FROM web_push_subscriptions WHERE endpoint=?",
                (endpoint,),
            ).fetchone()
            return self._from_row(row)

    def disable(self, user_login: str, endpoint: str, now: str) -> bool:
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="web_push.disable",
        ) as con:
            cursor = con.execute(
                "UPDATE web_push_subscriptions SET disabled_at=?, updated_at=? "
                "WHERE user_login=? AND endpoint=? AND disabled_at IS NULL",
                (now, now, user_login, endpoint),
            )
            return bool(cursor.rowcount)

    def active_for_user(self, user_login: str) -> list[WebPushSubscription]:
        with self.database.read("web_push.active_for_user") as con:
            rows = con.execute(
                """SELECT * FROM web_push_subscriptions
                WHERE user_login=? AND disabled_at IS NULL
                ORDER BY updated_at DESC""",
                (user_login,),
            ).fetchall()
        return [self._from_row(row) for row in rows]

    def active_for_recipients(
        self, recipients: list[str]
    ) -> list[WebPushSubscription]:
        if not recipients:
            return []
        placeholders = ",".join("?" for _ in recipients)
        with self.database.read("web_push.active_for_recipients") as con:
            rows = con.execute(
                f"""SELECT * FROM web_push_subscriptions
                WHERE disabled_at IS NULL AND lower(user_login) IN ({placeholders})
                ORDER BY updated_at DESC""",
                tuple(recipients),
            ).fetchall()
        return [self._from_row(row) for row in rows]

    def mark_delivery(
        self, subscription_id: int, now: str, error: str | None = None
    ) -> None:
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="web_push.mark_delivery",
        ) as con:
            if error:
                con.execute(
                    "UPDATE web_push_subscriptions SET updated_at=?, last_error=? WHERE id=?",
                    (now, error, subscription_id),
                )
            else:
                con.execute(
                    "UPDATE web_push_subscriptions SET updated_at=?, last_success_at=?, last_error=NULL WHERE id=?",
                    (now, now, subscription_id),
                )

    @staticmethod
    def _from_row(row) -> WebPushSubscription:
        return WebPushSubscription(
            id=int(row["id"]),
            user_login=str(row["user_login"]),
            endpoint=str(row["endpoint"]),
            subscription=json.loads(row["subscription_json"]),
            updated_at=str(row["updated_at"]),
            last_success_at=row["last_success_at"],
            last_error=row["last_error"],
            disabled_at=row["disabled_at"],
        )
