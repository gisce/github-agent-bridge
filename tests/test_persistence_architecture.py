import ast
from pathlib import Path


PACKAGE_ROOT = Path(__file__).parents[1] / "src" / "github_agent_bridge"
SQL_METHODS = {"execute", "executemany", "executescript"}


def _allows_sql(path: Path) -> bool:
    relative = path.relative_to(PACKAGE_ROOT)
    return (
        relative == Path("dashboard_data.py")
        or relative.parts[0] == "persistence"
        or relative.parts[:2] == ("sql", "migrations")
    )


def _sqlite_connect_calls(tree: ast.AST) -> list[int]:
    module_aliases = {"sqlite3"}
    connect_aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            module_aliases.update(
                alias.asname or alias.name
                for alias in node.names
                if alias.name == "sqlite3"
            )
        elif isinstance(node, ast.ImportFrom) and node.module == "sqlite3":
            connect_aliases.update(
                alias.asname or alias.name
                for alias in node.names
                if alias.name == "connect"
            )
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and (
            (
                isinstance(node.func, ast.Name)
                and node.func.id in connect_aliases
            )
            or (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "connect"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id in module_aliases
            )
        )
    ]


def _sql_method_calls(tree: ast.AST) -> list[tuple[int, str]]:
    return [
        (node.lineno, node.func.attr)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in SQL_METHODS
    ]


def test_sqlite_connections_and_sql_stay_inside_persistence_boundaries():
    violations: list[str] = []
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        relative = path.relative_to(PACKAGE_ROOT)
        connect_lines = _sqlite_connect_calls(tree)
        if relative != Path("persistence/database.py"):
            violations.extend(
                f"{relative}:{line}: direct sqlite3.connect"
                for line in connect_lines
            )
        if not _allows_sql(path):
            violations.extend(
                f"{relative}:{line}: direct .{method}()"
                for line, method in _sql_method_calls(tree)
            )

    assert violations == []


def test_repository_operations_declare_transaction_mode_and_safe_name():
    violations: list[str] = []
    persistence_root = PACKAGE_ROOT / "persistence"
    for path in sorted(persistence_root.glob("*.py")):
        if path.name in {"database.py", "__init__.py"}:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(
                node.func, ast.Attribute
            ):
                continue
            if node.func.attr == "read_only":
                violations.append(
                    f"{path.name}:{node.lineno}: use named Database.read()"
                )
                continue
            if node.func.attr not in {"read", "transaction"}:
                continue
            operation = next(
                (keyword.value for keyword in node.keywords if keyword.arg == "operation"),
                node.args[0] if node.func.attr == "read" and node.args else None,
            )
            if not isinstance(operation, ast.Constant) or not isinstance(
                operation.value, str
            ):
                violations.append(
                    f"{path.name}:{node.lineno}: missing literal operation name"
                )
            if node.func.attr == "transaction":
                mode = node.args[0] if node.args else None
                if not (
                    isinstance(mode, ast.Attribute)
                    and isinstance(mode.value, ast.Name)
                    and mode.value.id == "TransactionMode"
                    and mode.attr in {"DEFERRED", "IMMEDIATE"}
                ):
                    violations.append(
                        f"{path.name}:{node.lineno}: missing explicit TransactionMode"
                    )

    assert violations == []
