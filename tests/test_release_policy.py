import tomllib
from pathlib import Path


PYPROJECT = Path(__file__).parents[1] / "pyproject.toml"


def test_refactor_commits_trigger_patch_releases():
    config = tomllib.loads(PYPROJECT.read_text())
    parser_options = config["tool"]["semantic_release"]["commit_parser_options"]

    assert {"fix", "perf", "refactor"} <= set(parser_options["patch_tags"])
