from __future__ import annotations

import hashlib
import importlib
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import resources
from typing import Callable, Sequence


MIGRATION_FILENAME = re.compile(r"^v(?P<version>[0-9]{4})_[a-z0-9_]+\.py$")


class MigrationError(RuntimeError):
    """Raised when migration history is inconsistent or a step cannot run."""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    checksum: str
    upgrade: Callable[[sqlite3.Connection], None]


def load_migrations() -> tuple[Migration, ...]:
    """Load immutable migration modules in filename/version order."""
    loaded: list[Migration] = []
    package = resources.files(__package__)
    for path in sorted(package.iterdir(), key=lambda item: item.name):
        match = MIGRATION_FILENAME.match(path.name)
        if match is None or not path.is_file():
            continue
        version = int(match.group("version"))
        module_name = path.name[:-3]
        module = importlib.import_module(f"{__package__}.{module_name}")
        declared_version = int(getattr(module, "VERSION", -1))
        if declared_version != version:
            raise MigrationError(
                f"migration {path.name} declares version {declared_version}, expected {version}"
            )
        name = str(getattr(module, "NAME", "")).strip()
        upgrade = getattr(module, "upgrade", None)
        if not name or not callable(upgrade):
            raise MigrationError(f"migration {path.name} must define NAME and upgrade(connection)")
        loaded.append(
            Migration(
                version=version,
                name=name,
                checksum=hashlib.sha256(path.read_bytes()).hexdigest(),
                upgrade=upgrade,
            )
        )

    versions = [migration.version for migration in loaded]
    if versions != sorted(set(versions)):
        raise MigrationError("migration versions must be unique and ordered")
    return tuple(loaded)


def _ensure_history_table(con: sqlite3.Connection) -> None:
    con.execute(
        """CREATE TABLE IF NOT EXISTS schema_migrations (
          version INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          checksum TEXT NOT NULL,
          applied_at TEXT NOT NULL
        )"""
    )


def apply_migrations(
    con: sqlite3.Connection,
    migrations: Sequence[Migration] | None = None,
) -> tuple[int, ...]:
    """Apply each pending migration atomically and return applied versions."""
    if con.in_transaction:
        raise MigrationError("cannot apply migrations inside an existing transaction")
    _ensure_history_table(con)
    steps = tuple(migrations if migrations is not None else load_migrations())
    versions = [migration.version for migration in steps]
    if versions != sorted(set(versions)):
        raise MigrationError("migration versions must be unique and ordered")

    known_versions = set(versions)
    completed: list[int] = []
    while True:
        # Read migration history only after taking the writer lock so a
        # concurrent migrator cannot make this decision stale.
        con.execute("BEGIN IMMEDIATE")
        pending: Migration | None = None
        try:
            rows = con.execute(
                "SELECT version,name,checksum FROM schema_migrations ORDER BY version"
            ).fetchall()
            applied = {int(row[0]): (str(row[1]), str(row[2])) for row in rows}
            unknown_versions = sorted(set(applied) - known_versions)
            if unknown_versions:
                raise MigrationError(
                    "database contains migrations newer than this package: "
                    + ", ".join(str(version) for version in unknown_versions)
                )

            for migration in steps:
                recorded = applied.get(migration.version)
                if recorded is not None:
                    if recorded != (migration.name, migration.checksum):
                        raise MigrationError(
                            f"migration {migration.version} does not match recorded name/checksum"
                        )
                elif pending is None:
                    pending = migration

            if pending is not None:
                pending.upgrade(con)
                applied_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                con.execute(
                    "INSERT INTO schema_migrations(version,name,checksum,applied_at) VALUES(?,?,?,?)",
                    (pending.version, pending.name, pending.checksum, applied_at),
                )
        except Exception:
            con.rollback()
            raise
        else:
            con.commit()
            if pending is None:
                return tuple(completed)
            completed.append(pending.version)
