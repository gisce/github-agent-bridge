# Agent development guide

This repository is the GitHub notification → OpenClaw agent bridge used by Pilipilis.

## Safety first

- Default to `--mode shadow` for local runs. Use `--mode live` only when the operator explicitly asks.
- Never enable `--mark-seen` in development fixtures or shadow tests. It mutates the mailbox.
- Never commit secrets, app passwords, tokens, local DBs, logs, or `~/.config/*` files.
- Treat `enabledRepos` as the canary guardrail. If it is set, jobs outside that repo set must be denied.
- Do not remove the per-`work_key` lock/coalescing behavior without replacing it with an equivalent concurrency guard.
- Prefer adding tests around parser/policy/queue/dispatch behavior before changing production flow.

## Fast start

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[test]'
pytest -q
```

## Useful local commands

```bash
# Initialize an isolated DB
DB=/tmp/github-agent-bridge-dev.sqlite3
gab --db "$DB" init-db

# Replay a saved .eml without external side effects
gab --db "$DB" --policy ./policy.example.json replay ./fixtures/example.eml --verbose

# Enqueue a real GitHub issue/PR comment URL as a synthetic trusted notification
gab --db "$DB" --policy ./policy.example.json enqueue-comment-url \
  'https://github.com/gisce/erp/pull/27675#issuecomment-4419572864'

# Process queued jobs without touching GitHub/OpenClaw
gab --db "$DB" --policy ./policy.example.json run --mode shadow --once

# Inspect health

gab --db "$DB" --policy ./policy.example.json status
gab --db "$DB" --policy ./policy.example.json monitor --no-systemd
```

## Architecture contract

Pipeline:

```text
GitHub webhook ─┐
IMAP/eml ───────┼─> transport-neutral Notification -> Policy decision -> SQLite jobs -> executor -> GitHub 👀 + OpenClaw agent
manual URL ─────┘
```

Important invariants:

1. Each input must durably persist or explicitly handle an event before advancing its cursor or acknowledging enqueue.
2. Transport receipt ids (`Message-ID` or `X-GitHub-Delivery`) are idempotent, and matching cross-source receipts must not create duplicate jobs.
3. Active jobs with the same `work_key` (`owner/repo#number`) must coalesce instead of running concurrently.
4. Different `work_key`s may run in parallel.
5. `shadow` and `dry-run` modes must not call GitHub or OpenClaw.
6. `review_only` work uses a shorter timeout and must not ask agents to modify code.
7. `enabledRepos` is a hard allowlist when non-empty.

## Where to change things

- `parser.py`: classify GitHub notification text and extract repo/thread/comment context.
- `policy.py`: trust, canary scope, actions and routing decisions.
- `queue.py`: durable jobs, coalescing, locking and retry state.
- `sql/schema.sql`: packaged SQLite schema loaded by `queue.py`.
- `dispatch.py`: GitHub reactions and OpenClaw agent command construction/execution.
- `prompt_rules/*.md`: packaged Markdown rules appended to agent prompts. Keep these readable; they are loaded with `importlib.resources` so they work from wheels/sdists.
- `prompt_rules/roles/*.md`: packaged repository-role postures selected by `policy.json` `repoRoles`/`orgRoles`.
- `reader.py`: IMAP polling and mailbox mutation.
- `webhook.py`: webhook signature verification, structured event normalization and canonical event identity.
- `backend.py`: dedicated webhook ingress and operator monitoring endpoints.
- `monitor.py`: operational health checks.
- `cli.py`: operational entrypoints and developer tooling.
- `autoupdate.py`: safe release update planning. When changing runtime structure, reload boundaries, schema layout, dashboard packaging, queue semantics, or process/service topology, update the autoupdate classification and tests so the planner still knows whether a release can reload the dashboard, must defer executor work, or needs a migration window.

## Prompt resource contract

- Do not put substantial LLM prompt text inline in Python. Prompt text belongs in `src/github_agent_bridge/prompt_rules/*.md` or `src/github_agent_bridge/prompt_rules/roles/*.md`.
- If a new prompt can reasonably vary by deployment, add a `policy.json` `promptOverrides` key for it instead of hardcoding a resource path as the only source.
- Packaged prompt resources are defaults. Operator override files are first-class configuration and must be honored wherever prompts are built, including background learning commands such as `feedback-learn`.
- Add or update tests for both the packaged default and the override path whenever prompt loading changes.

## Test expectations

Run before handing off or pushing:

```bash
pytest -q
```

For changes that touch CLI behavior, also run the relevant command against a temporary DB. Never test live mode against production unless explicitly requested.
