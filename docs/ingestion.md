# Event ingestion and transport migration

The bridge accepts GitHub events from three entry paths: signed GitHub
webhooks, GitHub notification email through IMAP/`.eml`, and explicit manual
enqueue/replay commands. Webhook and IMAP inputs are normalized into the same
queue contract; neither transport bypasses policy.

The bridge separates three identities:

- A **receipt** identifies one delivery from one transport. Email uses
  `Message-ID`; webhook delivery uses `X-GitHub-Delivery`.
- A **GitHub event** identifies the underlying action independently of its
  transport. Comment, review and workflow-run events use their immutable
  object IDs. Repeated state transitions use the immutable GitHub timeline
  event ID only after the occurrence has been resolved unambiguously.
- A **work key** (`owner/repo#number`) serializes work in one thread. It must
  not deduplicate distinct events in that thread.

`ingest_receipts` makes retries from a transport idempotent. `github_events`
implements first-writer-wins across transports and points every duplicate
receipt at the winning job. Both records and the job are written in one SQLite
transaction, before the IMAP high-water mark advances.

Before persistence, both transports produce the same normalized event envelope
in job metadata. The envelope records the repository, public thread number,
subject type, normalized event type/action, triggering actor, event timestamp
when GitHub exposes one, target login where applicable, canonical identity,
bridge action, work intent, and receipt provenance. Provenance and available
timing data remain transport-specific; the semantic fields are the
compatibility contract used by the parity matrix.

Comment and review identities use immutable GitHub object IDs. A merged PR is
also unique within its public PR number. For assignment, review-request, close
and reopen events, IMAP resolves GitHub `issue_event/<id>` and
`review_requested/<id>` message IDs through the exact
`repos/{owner}/{repo}/issues/events/{id}` API resource. Webhook payloads do not
carry that ID, so webhook ingestion matches the signed timestamp, actor and
target against the issue event list and accepts the result only when exactly
one timeline event matches. Both transports then use the immutable timeline ID
as the canonical occurrence identity. The resolved ID and endpoint are
retained in job metadata for audit without persisting another raw GitHub
response.

If an API lookup fails, a required identity field is absent, or multiple
timeline events match the same second/actor/target tuple, ingestion keeps the
receipt-scoped fallback. Display-actor fallback never participates in event
identity. These fallbacks prevent loss of repeated events but do not guarantee
cross-transport deduplication after the first job has completed; the rollout
mismatch gate must keep them visible and block promotion until they are
explained.

The redacted paired fixtures verify the shared semantic envelope for every
supported action and both arrival orders for events with provable identity.
Transport-local fallback events may still coalesce while work is active, but a
later reassignment, renewed review request or lifecycle transition with a
resolved timeline ID creates a new auditable job.

When a notification lacks enough stable data to prove event identity, the
event key falls back to `<source>:<source-key>`. The bridge deliberately
accepts a possible duplicate rather than risk dropping a legitimate action.

## Transport comparison

| Concern | IMAP | GitHub App webhook |
| --- | --- | --- |
| Trust | Auth headers and GitHub sender checks | Mandatory HMAC-SHA256 signature over raw bytes; optional owner allowlist through owner-specific secrets |
| Idempotency | `Message-ID` receipt | `X-GitHub-Delivery` receipt |
| Cross-source identity | IDs parsed from GitHub URLs | IDs in the structured payload |
| Latency | Polling and mail delivery | Immediate delivery with retries |
| Operations | Mailbox cursor, credentials, formatting drift | Public TLS endpoint, secret rotation, delivery monitoring |

## Gradual rollout

1. **Phase 0 (implemented):** route IMAP through the common transactional
   ingestor while preserving the existing queue and dispatch behavior.
2. **Shadow webhook (implemented):** verify signatures and persist shadow
   receipts, but do not create jobs or claim canonical events. Compare coverage
   and canonical keys with IMAP.
3. **Canary dual ingest (implemented):** allow webhook enqueue only for `webhookCanaryRepos`.
   For events with a shared immutable identity, the first source wins. Other
   events remain receipt-idempotent and may coalesce only while work is active.
4. **Webhook primary (implemented, operator-gated):** keep IMAP as a fallback
   until a complete operational cycle has no unexplained IMAP-only actionable
   events, then retire the reader separately.

Webhook ingress is exposed as `POST /api/webhooks/github`.
For production, nginx should route that exact path to the dedicated
socket-activated `github-agent-bridge-webhook.service` on port 8766. The
dashboard keeps the route for backward compatibility, but using it couples
GitHub delivery availability to dashboard/UI restarts.
For a single trusted owner, configure `GITHUB_AGENT_BRIDGE_WEBHOOK_SECRET`;
during rotation, `GITHUB_AGENT_BRIDGE_WEBHOOK_PREVIOUS_SECRET` accepts the old
secret as well. This legacy form accepts any repository signed with that shared
secret.

For multiple organizations or owners, use independent secrets and an explicit
owner allowlist:

```shell
export GITHUB_AGENT_BRIDGE_WEBHOOK_SECRETS_BY_OWNER='{"gisce":["current-gisce-secret","previous-gisce-secret"],"example":["example-secret"]}'
```

Each value is an ordered list of accepted current/rotation secrets. When this
setting is present, a payload whose `repository.full_name` owner is not in the
map is rejected before persistence. Do not reuse a secret across owners: that
would couple rotation and increase the blast radius of a leak.

`GITHUB_AGENT_BRIDGE_WEBHOOK_MAX_BYTES` defaults to 1 MiB. GitHub must send
`Content-Type: application/json`, `X-GitHub-Delivery`, `X-GitHub-Event`, and a
valid `X-Hub-Signature-256` computed over the unmodified request bytes.

### GitHub configuration

Create either a repository webhook under **Settings → Webhooks** or an
organization webhook under **Organization settings → Webhooks**:

1. Set **Payload URL** to `https://<host>/api/webhooks/github`.
2. Set **Content type** to `application/json` and **Secret** to the matching
   configured owner secret.
3. Keep SSL verification enabled and the webhook active.
4. Select individual events: **Issue comments**, **Pull request reviews**,
   **Pull request review comments**, **Commit comments**, and **Workflow runs**.
   Do not select “Send me everything” for Phase 1.

A repository webhook covers only that repository. An organization webhook
covers repositories in that organization and is the recommended deployment.
Multiple organizations and multiple hooks may use the same endpoint when each
owner has its own entry in `GITHUB_AGENT_BRIDGE_WEBHOOK_SECRETS_BY_OWNER`.
GitHub's `X-GitHub-Hook-ID` header keeps their inventory and activity separate.

The endpoint stores routing metadata, a SHA-256 payload hash, the canonical
event key, and the verified JSON payload in `webhook_shadow_receipts`. The
payload follows the same bounded retention as its receipt and is exposed only
through the administrator-only delivery detail endpoint. The monitoring API is
split so opening the overview does not load hook inventory or delivery history:

- `GET /api/webhooks/github/summary` returns mode, receipt counts, retries, and
  cross-source coverage (`both`, `imap_only`, `webhook_only`) with an explicit
  IMAP denominator and mean matching delay. The previous `/status` path remains
  a summary-only compatibility alias.
- `GET /api/webhooks/github/exceptions` returns a bounded operator queue for
  IMAP-only, webhook-only, unmatchable, decision-mismatch and potential
  split-identity events. Paired anomalies include both receipt references plus
  repository and event descriptor; the endpoint never returns raw payload or
  comment bodies.
- `GET /api/webhooks/github/timeseries?from=<iso>&to=<iso>&bucket=day|hour`
  returns only the requested interval. Daily ranges are capped at 366 days and
  hourly ranges at 31 days; omitted bounds default to the configured retention
  window ending now, capped at the daily maximum.
- `GET /api/webhooks/github/hooks?limit=<1-100>&cursor=<opaque>` returns a
  cursor-paginated per-hook inventory. A signed `ping` stores its sanitized
  configuration (never the secret or raw payload), while later deliveries
  update its latest event, repository, delivery ID, and result.
- `GET /api/webhooks/github/hooks/{hook_id}` returns the sanitized hook detail,
  aggregate retained-delivery counts, its most recent deliveries, and recent
  audited administrative actions.
- `POST /api/webhooks/github/hooks/{hook_id}/ping` lets a dashboard administrator
  ask GitHub to send a fresh `ping`. The server validates the stored URL against
  the exact `api.github.com` organization or repository hook path before calling
  `gh api`; browser OAuth credentials are never reused. The operational `gh`
  identity needs `Organization hooks: write` or `Repository webhooks: write`.
  Requested, successful, and failed actions are recorded with the administrator
  login, while CLI failure details remain hidden from the HTTP response.
- `GET /api/webhooks/github/deliveries?limit=<1-100>&cursor=<opaque>` returns a
  cursor-paginated delivery page. Optional `hook_id`, `event_name`, `repository`,
  and `result` filters are applied server-side; every row includes the known
  hook identity so operators can navigate directly to its detail.
- `GET /api/webhooks/github/deliveries/{delivery_id}` returns the retained full
  JSON payload, its SHA-256 hash, delivery metadata, and the linked job summary
  when ingestion created or coalesced into a job. Legacy receipts created before
  payload retention return `payload: null`.

The dashboard loads these resources lazily per tab, caches them separately, and
appends cursor pages as the inventory or delivery list scrolls. Hook and
delivery detail URLs are shareable under `/webhooks/hooks/{hook_id}` and
`/webhooks/deliveries/{delivery_id}`; hook detail links to GitHub's webhook
settings when the target is known.
Here “operators” means users authorized as dashboard administrators through
`GITHUB_AGENT_BRIDGE_DASHBOARD_ADMIN_USERS` or
`GITHUB_AGENT_BRIDGE_DASHBOARD_ADMIN_TEAMS`; every monitoring endpoint returns
HTTP 403 to other authenticated users and HTTP 401 to unauthenticated users.
Receipt details are retained for 30 days by default and pruned during ingestion;
set `GITHUB_AGENT_BRIDGE_WEBHOOK_RETENTION_DAYS` to change that window. Raw
payloads are pruned with their receipts.

The maintained event inventory and support levels are in
[`webhook-events.md`](webhook-events.md).

Comment and review `edited` deliveries are observed under a distinct key and
do not retrigger work. Phase 2 must make an explicit policy decision before
any non-`created` action can enqueue a job.

## Canary dual ingestion

Set `GITHUB_AGENT_BRIDGE_WEBHOOK_MODE=canary` and point
`GITHUB_AGENT_BRIDGE_WEBHOOK_POLICY` at the reader/executor policy file. Canary
mode converts only supported actionable deliveries into the common queue and
requires their repository to be explicitly listed in `webhookCanaryRepos`; an
empty allowlist enqueues nothing. This allowlist is deliberately separate from
`enabledRepos`, which remains the hard scope for every transport, so narrowing
the webhook canary does not deny IMAP work for other repositories. IMAP
continues unchanged. Both transports use the same canonical event key, so the
first committed receipt wins and the second is recorded as a duplicate of the
same job.

Actionability is independent of transport. Before dispatch, every issue
comment, review comment, formal review, and commit comment must either address
the authenticated bot explicitly, target a PR/issue assigned to it, or belong
to a PR authored by it. GitHub email delivery footers are excluded from intent
classification because notification reasons such as “your review was
requested” describe routing, not authorization.

For enqueueing, the common queue transaction commits before the monitoring
receipt. The receipt records the enqueue decision and linked job so canary
behavior is auditable in the delivery explorer. A crash in that narrow gap
cannot lose work: GitHub retries the
delivery, the durable `ingest_receipts(source='webhook', source_key=<delivery>)`
row makes the queue operation idempotent, and the retry repairs the monitoring
receipt. Events sent by a configured `botLogins` identity, `edited`
comments/reviews, unsupported families, and repositories outside
`webhookCanaryRepos` remain observational only. For `workflow_run.completed`,
only runs with `conclusion: failure` enqueue work; successful and other
conclusions remain observational. Merged pull-request deliveries share the
same canonical key as the equivalent IMAP merge notification, so dual ingestion
does not enqueue cleanup twice.

Coverage compares canonical event families shared by both transports and also
counts any source-specific email event that created trusted executable work.
This keeps `email:*` and other uncorrelated IMAP events in the denominator and
the exception queue instead of hiding the exact gaps that block promotion.
Passive email handled as `archive_notification` remains excluded. The bridge
durably records when webhook observation first starts, independently of receipt
payload retention. The promotion window covers seven complete days by default
and ends before a configurable grace period; historical IMAP receipts from
before webhook observation began remain excluded. Set
`GITHUB_AGENT_BRIDGE_WEBHOOK_PROMOTION_OBSERVATION_DAYS` to change the required
duration and `GITHUB_AGENT_BRIDGE_WEBHOOK_COVERAGE_GRACE_SECONDS` to change the
default ten-minute grace period. Receipt retention must cover the full window
plus its grace period or the gate remains `insufficient_data`.

Every durable ingestion receipt also records the normalized repository, work
key, event descriptor, bridge action, policy decision and work intent. The
dashboard compares those receipt-scoped decisions when both transports share a
canonical identity. It reports action/intent/work-key mismatches and potential
split identities separately. A potential split requires source-scoped
`email:*` and `webhook:*` identities with the same work key and event descriptor
received within ten minutes. This is deliberately an ambiguity signal, not
proof that the receipts are the same occurrence, and it never causes events to
be deduplicated. Distinct immutable comment or review IDs are never classified
as splits merely because they belong to the same thread.

The gate reports `insufficient_data` until the full configured observation window
has been observed and contains at least one cross-source comparable event. It
remains `blocked` while either transport has unmatched events or either
mismatch count is non-zero, and becomes `ready` only for a complete, nonempty,
clean window. Run `gab migrate-db` before deploying this read model.

## Primary webhook with stable IMAP fallback

After the dashboard gate has no unexplained IMAP-only actionable events, set
both `GITHUB_AGENT_BRIDGE_WEBHOOK_MODE=primary` and
`GITHUB_AGENT_BRIDGE_WEBHOOK_PRIMARY_ACK=true`. The second switch is a deliberate
operator acknowledgement; `primary` fails closed without both it and
`GITHUB_AGENT_BRIDGE_WEBHOOK_POLICY`. The configured policy remains authoritative
for trust, action, routing and the global `enabledRepos` guardrail. Primary mode
does not use the narrower `webhookCanaryRepos` allowlist.

Primary changes which source is expected to win, not the idempotency model.
Keep the IMAP reader enabled during the stable fallback phase. If webhook
delivery is late or unavailable, email still creates the canonical event and
job; if both arrive, the unique event key links the second receipt to the first
job. Rollback is configuration-only: return the endpoint to `shadow`, leave the
IMAP reader running, and inspect the exception queue before trying primary
again. Do not enable `--mark-seen` merely because primary mode is active.

This implementation does not add an arbitrary sleep to IMAP. Delaying the
reader would also delay genuine webhook gaps and complicate its durable UID
cursor. The first-source metrics make the actual winner visible, while the
shared transaction guarantees correctness independently of arrival order.

Webhook enqueueing must not be enabled until recovery of persisted-but-
unprocessed receipts and divergence metrics have been validated in production.
The remaining parity, coverage and cutover gates are tracked in
[#299](https://github.com/gisce/github-agent-bridge/issues/299); the existence
of `primary` mode is not evidence that a deployment is ready to disable IMAP.
