from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .database import Database, TransactionMode


@dataclass(frozen=True)
class McpToken:
    id: str
    name: str
    user_login: str | None
    created_by: str | None
    created_at: str
    last_used_at: str | None
    revoked_at: str | None
    expires_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "user_login": self.user_login,
            "created_by": self.created_by,
            "created_at": self.created_at,
            "last_used_at": self.last_used_at,
            "revoked_at": self.revoked_at,
            "expires_at": self.expires_at,
        }


@dataclass(frozen=True)
class McpTokenCredential:
    record: McpToken
    token_hash: str


class McpTokenRepository:
    """Persist MCP token records while keeping authentication policy outside."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def create(self, record: McpToken, token_hash: str) -> None:
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="mcp_tokens.create",
        ) as con:
            con.execute(
                """INSERT INTO mcp_tokens(
                    id, name, token_hash, user_login, created_by, created_at, expires_at
                ) VALUES(?,?,?,?,?,?,?)""",
                (
                    record.id,
                    record.name,
                    token_hash,
                    record.user_login,
                    record.created_by,
                    record.created_at,
                    record.expires_at,
                ),
            )

    def update_owner(self, token_id: str, user_login: str | None) -> McpToken | None:
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="mcp_tokens.update_owner",
        ) as con:
            cursor = con.execute(
                "UPDATE mcp_tokens SET user_login=? WHERE id=? AND revoked_at IS NULL",
                (user_login, token_id),
            )
            if cursor.rowcount == 0:
                return None
            return self._get(con, token_id)

    def list(
        self,
        *,
        include_revoked: bool = False,
        user_login: str | None = None,
    ) -> list[McpToken]:
        clauses = []
        args: list[Any] = []
        if not include_revoked:
            clauses.append("revoked_at IS NULL")
        if user_login:
            clauses.append("lower(user_login)=?")
            args.append(user_login)
        sql = self._select_sql()
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC, id DESC"
        with self.database.read("mcp_tokens.list") as con:
            return [self._from_row(row) for row in con.execute(sql, args)]

    def revoke(
        self,
        token_id: str,
        revoked_at: str,
        *,
        user_login: str | None = None,
    ) -> bool:
        args: list[Any] = [revoked_at, token_id]
        owner_clause = ""
        if user_login:
            owner_clause = " AND lower(user_login)=?"
            args.append(user_login)
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="mcp_tokens.revoke",
        ) as con:
            cursor = con.execute(
                f"UPDATE mcp_tokens SET revoked_at=? WHERE id=? AND revoked_at IS NULL{owner_clause}",
                args,
            )
            return cursor.rowcount > 0

    def active_credentials(self, now: str) -> list[McpTokenCredential]:
        with self.database.read("mcp_tokens.active_credentials") as con:
            rows = con.execute(
                self._select_sql(include_hash=True)
                + " WHERE revoked_at IS NULL AND (expires_at IS NULL OR expires_at > ?)",
                (now,),
            ).fetchall()
        return [
            McpTokenCredential(self._from_row(row), str(row["token_hash"]))
            for row in rows
        ]

    def mark_used(self, token_id: str, used_at: str) -> None:
        with self.database.transaction(
            TransactionMode.DEFERRED,
            operation="mcp_tokens.mark_used",
        ) as con:
            con.execute(
                "UPDATE mcp_tokens SET last_used_at=? WHERE id=?",
                (used_at, token_id),
            )

    @staticmethod
    def _select_sql(*, include_hash: bool = False) -> str:
        columns = (
            "id, name, user_login, created_by, created_at, last_used_at, "
            "revoked_at, expires_at"
        )
        if include_hash:
            columns += ", token_hash"
        return f"SELECT {columns} FROM mcp_tokens"

    @classmethod
    def _get(cls, con, token_id: str) -> McpToken | None:
        row = con.execute(
            cls._select_sql() + " WHERE id=?", (token_id,)
        ).fetchone()
        return cls._from_row(row) if row else None

    @staticmethod
    def _from_row(row) -> McpToken:
        return McpToken(
            id=str(row["id"]),
            name=str(row["name"]),
            user_login=row["user_login"],
            created_by=row["created_by"],
            created_at=str(row["created_at"]),
            last_used_at=row["last_used_at"],
            revoked_at=row["revoked_at"],
            expires_at=row["expires_at"],
        )
