from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterator


class TransactionMode(str, Enum):
    """SQLite transaction modes supported by the persistence boundary."""

    DEFERRED = "deferred"
    IMMEDIATE = "immediate"


class ClosingConnection(sqlite3.Connection):
    """Commit or roll back a context-managed connection, then close it."""

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


class Database:
    """Create consistently configured SQLite connections and transactions."""

    def __init__(
        self,
        path: str | Path,
        *,
        timeout_seconds: float = 30,
        busy_timeout_ms: int | None = None,
    ) -> None:
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must be non-negative")
        configured_busy_timeout = int(
            round(timeout_seconds * 1000) if busy_timeout_ms is None else busy_timeout_ms
        )
        if configured_busy_timeout < 0:
            raise ValueError("busy_timeout_ms must be non-negative")
        self.path = Path(path).expanduser()
        self.timeout_seconds = timeout_seconds
        self.busy_timeout_ms = configured_busy_timeout

    def read_write(self) -> sqlite3.Connection:
        """Open a read-write connection with the bridge's SQLite policy."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(
            self.path,
            timeout=self.timeout_seconds,
            isolation_level=None,
            factory=ClosingConnection,
        )
        try:
            self._configure(con, read_only=False)
        except Exception:
            con.close()
            raise
        return con

    def read_only(self) -> sqlite3.Connection:
        """Open a URI read-only connection and enforce query-only behavior."""
        uri = self.path.absolute().as_uri() + "?mode=ro"
        con = sqlite3.connect(
            uri,
            uri=True,
            timeout=self.timeout_seconds,
            isolation_level=None,
            factory=ClosingConnection,
        )
        try:
            self._configure(con, read_only=True)
        except Exception:
            con.close()
            raise
        return con

    @contextmanager
    def transaction(
        self,
        mode: TransactionMode = TransactionMode.DEFERRED,
    ) -> Iterator[sqlite3.Connection]:
        """Run one unit of work in an explicit deferred or immediate transaction."""
        if not isinstance(mode, TransactionMode):
            raise ValueError(f"unsupported transaction mode: {mode}")
        con = self.read_write()
        try:
            statement = "BEGIN" if mode is TransactionMode.DEFERRED else "BEGIN IMMEDIATE"
            con.execute(statement)
            yield con
        except BaseException:
            con.rollback()
            raise
        else:
            con.commit()
        finally:
            con.close()

    def _configure(self, con: sqlite3.Connection, *, read_only: bool) -> None:
        con.row_factory = sqlite3.Row
        con.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        con.execute("PRAGMA foreign_keys=ON")
        if read_only:
            con.execute("PRAGMA query_only=ON")
        else:
            journal_mode = str(con.execute("PRAGMA journal_mode").fetchone()[0]).lower()
            if journal_mode != "wal":
                con.execute("PRAGMA journal_mode=WAL")


def backup_sqlite_database(
    path: str | Path,
    backup_dir: str | Path,
) -> dict[str, Any]:
    """Create a consistent online SQLite backup for an operator action."""
    source_path = Path(path).expanduser()
    backup_root = Path(backup_dir).expanduser()
    backup_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    backup_path = backup_root / f"{source_path.stem}-{timestamp}.sqlite3"
    with sqlite3.connect(source_path) as source, sqlite3.connect(backup_path) as target:
        source.backup(target)
    return {
        "path": str(backup_path),
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": str(source_path),
        "size_bytes": backup_path.stat().st_size,
    }


def restore_sqlite_database(path: str | Path, backup_path: str | Path) -> dict[str, Any]:
    """Restore a SQLite database from an operator-created backup."""
    target_path = Path(path).expanduser()
    source_path = Path(backup_path).expanduser()
    with sqlite3.connect(source_path) as source, sqlite3.connect(target_path) as target:
        source.backup(target)
    return {
        "restored_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": str(source_path),
        "target": str(target_path),
        "size_bytes": target_path.stat().st_size,
    }
