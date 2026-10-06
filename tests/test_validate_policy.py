import json

from github_agent_bridge.cli import main
from github_agent_bridge.policy import validate_policy_file


def test_example_policy_matches_published_schema():
    validate_policy_file("policy.example.json")


def test_validate_policy_cli_reports_unknown_field(tmp_path, capsys):
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"feedbackLearning": {"surprise": True}}))

    result = main(["validate-policy", "--policy", str(policy)])

    assert result == 1
    error = capsys.readouterr().err
    assert "feedbackLearning" in error
    assert "surprise" in error


def test_validate_policy_cli_accepts_valid_policy(tmp_path, capsys):
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"trustedOrgs": ["gisce"]}))

    result = main(["validate-policy", "--policy", str(policy)])

    assert result == 0
    assert "valid policy" in capsys.readouterr().out
