from pathlib import Path


WORKFLOW = Path(__file__).parents[1] / ".github" / "workflows" / "release.yml"


def release_steps():
    sections = WORKFLOW.read_text().split("\n      - name: ")[1:]
    return {
        name: body
        for section in sections
        for name, _, body in [section.partition("\n")]
    }


def test_release_checkout_can_pin_an_older_workflow_sha():
    steps = release_steps()

    checkout = steps["Checkout workflow SHA"]
    assert "ref: ${{ github.sha }}" in checkout
    assert "fetch-depth: 0" in checkout
    assert 'git checkout -B "$GITHUB_REF_NAME" "$GITHUB_SHA"' in steps[
        "Restore release branch at workflow SHA"
    ]


def test_stale_release_runs_skip_validation_unless_a_partial_release_can_resume():
    steps = release_steps()
    initial_guard = steps["Check workflow SHA is still current"]

    assert "id: release_head" in initial_guard
    assert 'refs/heads/${GITHUB_REF_NAME}' in initial_guard
    assert 'echo "current=false" >> "$GITHUB_OUTPUT"' in initial_guard
    assert 'echo "resume=true" >> "$GITHUB_OUTPUT"' in initial_guard
    assert "gh release view" in initial_guard
    assert "github_agent_bridge-${release_version}-py3-none-any.whl" in initial_guard

    guarded_steps = [
        "Set up Python",
        "Set up Node",
        "Install package with test dependencies",
        "Install dashboard dependencies",
        "Run dashboard tests",
        "Build dashboard",
        "Run tests before release",
    ]
    for name in guarded_steps:
        assert "steps.release_head.outputs.current == 'true'" in steps[name]
        assert "steps.release_head.outputs.resume == 'true'" in steps[name]

    final_guard = steps["Recheck workflow SHA before release"]
    assert "id: pre_release_head" in final_guard
    assert 'refs/heads/${GITHUB_REF_NAME}' in final_guard
    assert "if: steps.release_head.outputs.current == 'true'" in final_guard
    assert "if: steps.pre_release_head.outputs.current == 'true'" in steps[
        "Semantic version release"
    ]


def test_failed_semantic_release_recovers_the_partial_commit_and_tag():
    steps = release_steps()

    semantic_release = steps["Semantic version release"]
    recovery = steps["Recover partial semantic release"]

    assert "continue-on-error: true" in semantic_release
    assert "steps.release.outcome == 'failure'" in recovery
    assert 'release_parent="$(git rev-parse "${remote_sha}^")"' in recovery
    assert "python -m build" in recovery
    assert "for attempt in 1 2 3" in recovery
    assert 'git push origin "refs/tags/${release_tag}"' in recovery


def test_release_is_only_green_after_github_assets_are_verified():
    steps = release_steps()

    for name in [
        "Upload distributions to GitHub Release",
        "Upload distribution artifacts",
        "Verify published release",
    ]:
        assert "steps.partial_release.outputs.recovered == 'true'" in steps[name]

    verify = steps["Verify published release"]
    assert "gh release view" in verify
    assert "github_agent_bridge-${release_version}-py3-none-any.whl" in verify
    assert "github_agent_bridge-${release_version}.tar.gz" in verify
