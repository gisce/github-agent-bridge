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
