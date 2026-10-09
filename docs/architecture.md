# Architecture

The bridge separates **fast event ingestion** from **slow agent work**. Signed GitHub webhooks and GitHub notification email both feed the same durable queue; that transport-neutral split is the core design choice.

```mermaid
flowchart LR
    A[GitHub webhook] --> D[Notification]
    B[IMAP / .eml] --> D
    C[Manual URL] --> D
    D --> E[Policy decision]
    E --> F[(SQLite queue)]
    F --> G[Executor pool]
    G --> H[GitHub reaction]
    G --> I[OpenClaw agent]

    F -. canonical event identity .-> F
    F -. work_key owner/repo#number .-> F
```

## Design goals

| Goal | Design response |
| --- | --- |
| Do not block an input | Ingress verifies, normalizes, classifies, and enqueues; it never waits for agent work. |
| Do not lose work | Events are durable before a source cursor or enqueue acknowledgement advances. |
| Do not duplicate dual delivery | Transport receipts converge on a canonical GitHub event when identity is provable. |
| Do not duplicate thread work | Active jobs with the same `work_key` coalesce. |
| Do not serialize unrelated repos | Different `work_key`s can run in parallel. |
| Do not widen trust accidentally | Policy gates source, scope, actions, routes, and roles. |
| Do not let one failure jam everything | Failed dispatch marks one job `blocked`; unrelated jobs continue. |

## Components

### Webhook ingress

`github-agent-bridge-webhook` accepts signed GitHub deliveries at
`POST /api/webhooks/github`. It verifies HMAC over the raw payload, persists the
receipt, normalizes supported structured events and, in `canary` or `primary`
mode, sends them through the common queue transaction. `shadow` records
deliveries without creating jobs.

The socket-activated ingress is intentionally separate from the dashboard so a
UI restart does not interrupt GitHub delivery. `webhookCanaryRepos` constrains
dual ingestion in `canary`; the global `enabledRepos` guard applies to every
transport.

### IMAP reader

`ImapReader` fetches GitHub notification emails, parses metadata, and enqueues durable `Notification` jobs.

**Invariant:** advance `last_uid` only after the notification has been durably queued or safely ignored.

IMAP remains a supported fallback while webhook coverage is being proven. Both
transports use the same ingestion persistence and queue; see
[`ingestion.md`](ingestion.md) for identity and rollout semantics.

### Policy

`Policy` decides whether a trusted GitHub event becomes:

- `auto`
- `auto_trusted`
- `ask`
- `deny`

It also selects delivery routes and repository roles for dispatched agent work.

### Queue

`JobQueue` uses SQLite/WAL.

Normal connections and transaction modes follow the
[`SQLite persistence contract`](persistence.md). Queue extraction into focused
repositories must preserve those transaction boundaries.

| Table | Purpose |
| --- | --- |
| `jobs` | Durable work items and execution state. |
| `coalesced_notifications` | Extra emails folded into an active `work_key`. |
| `ingest_receipts` | Idempotent source receipts for email and webhook delivery. |
| `github_events` | Canonical cross-source GitHub event identity and winning job. |
| `webhook_shadow_receipts` | Signed webhook delivery audit, payload hash and enqueue result. |
| `state` | Mailbox high-water marks and future cursors. |
| `worklog` | Audit trail. |

### Executor pool

The executor claims pending jobs with this constraint:

```sql
status = 'pending'
AND NOT EXISTS running job with same work_key
```

That permits parallelism across unrelated PRs/issues while serializing a single thread.

### Dispatch

Dispatch has two external side effects in live mode:

1. apply GitHub 👀 reaction when possible;
2. send one OpenClaw agent task with prompt rules and repository role context.

If dispatch fails, the job is marked `blocked`, `last_error` is stored, and the lock is released.

## Prompt resources

Prompt rules are packaged Markdown files:

```text
src/github_agent_bridge/prompt_rules/*.md
src/github_agent_bridge/prompt_rules/roles/*.md
```

They are loaded with `importlib.resources`, so they remain available from editable installs, wheels, and sdists.
