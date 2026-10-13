from __future__ import annotations

import os
from collections.abc import Mapping


def configured_node_bin(explicit: str | None = None) -> str | None:
    """Resolve the Node executable shared by bridge subprocesses."""
    return (
        explicit
        or os.getenv("GITHUB_AGENT_BRIDGE_NODE_BIN")
        or os.getenv("NODE_BIN")
        or None
    )


def prepare_subprocess_env(
    *executables: str | None,
    base: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Prepend executable directories to PATH without duplicating entries."""
    env = dict(base) if base is not None else os.environ.copy()
    entries = [entry for entry in env.get("PATH", "").split(os.pathsep) if entry]
    prefixes: list[str] = []
    for executable in executables:
        if not executable:
            continue
        directory = os.path.dirname(os.path.expanduser(executable))
        if directory and directory not in prefixes:
            prefixes.append(directory)
    env["PATH"] = os.pathsep.join(
        [*prefixes, *(entry for entry in entries if entry not in prefixes)]
    )
    return env
