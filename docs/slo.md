# Service-level indicators

This document fixes the data contract for bridge SLO reporting. It is the
source of truth for the read-model, dashboard, and monitor work that follows.
The storage signals are available before the aggregate API so deployments can
collect trustworthy samples before alert thresholds are enabled.

## Window and cohort

The default window is 7 days. Consumers should also offer 24-hour and 30-day
windows. Windows end at the query instant in UTC; daily trend buckets use the
configured dashboard timezone and must return that timezone explicitly.

The service cohort contains jobs that:

- have `decision='auto_trusted'`;
- finished inside the requested window; and
- have `terminal_outcome` equal to `completed`, `no_op`, or `blocked`.

Manual cancellation, manual dismissal, policy denial, and historical rows with
an unknown outcome are excluded from the service cohort. Every aggregate must
return `sample_count` and `missing_count`; missing data is never reported as
zero latency or a 0% rate.

## Latency SLIs

| SLI | Start | End | Missing when |
| --- | --- | --- | --- |
| Source received to queued | `jobs.source_received_at` | `jobs.created_at` | source timestamp is `NULL` |
| Queued to claimed | `jobs.created_at` | earliest `job_runs.started_at` | job was never claimed |
| Claimed to first output | earliest `job_runs.started_at` | earliest `job_progress.ts` with `kind='visible'` | no visible progress exists |
| Claimed to done | earliest `job_runs.started_at` | `jobs.finished_at` | either timestamp is missing |

Email ingestion obtains the source timestamp from IMAP `INTERNALDATE`, not from
the mailbox UID or message `Date` header. Webhook ingestion captures the HTTP
ingress instant before request parsing and authentication work. Synthetic or
legacy inputs without a reliable source clock use `NULL` and contribute to the
corresponding `missing_count`.

The first claim is always `MIN(job_runs.started_at)`. `jobs.started_at` is a
latest-attempt compatibility field and must not be used for this SLI. The
claimed-to-done duration includes retry and queue wait between attempts because
it represents user-perceived completion latency.

Latency summaries report p50, p90, and p99 in seconds. Negative or malformed
intervals are invalid samples and count as missing.

## Reliability SLIs

| SLI | Numerator | Denominator |
| --- | --- | --- |
| Blocked rate | cohort jobs with `terminal_outcome='blocked'` | all jobs in the service cohort |
| No-op rate | cohort jobs with `terminal_outcome='no_op'` | all jobs in the service cohort |
| Retry rate | cohort jobs with more than one `job_runs` row | claimed jobs in the service cohort |
| Coalescing rate | accepted notifications stored in `coalesced_notifications` | newly created jobs plus accepted coalesced notifications |

Retry reporting should also expose the absolute number of `job_runs` whose
result is `requeued`. Coalescing is a notification-level rate, not a job-level
rate; counting `jobs.coalesced_count > 0` would undercount bursts.

## Structured terminal outcomes

`jobs.terminal_outcome` is the metric input. Consumers must not search job
summaries, details, errors, or agent output for keywords.

| Outcome | Meaning |
| --- | --- |
| `completed` | Agent work completed through the normal execution path. |
| `no_op` | The bridge explicitly determined that no new action was appropriate. |
| `blocked` | Execution ended requiring operator or requester attention. |
| `cancelled` | A running job was manually cancelled. |
| `denied` | Policy rejected the notification at ingestion. |
| `dismissed` | An operator manually closed a non-running terminal/waiting job. |

`outcome_reason` is a stable machine-readable reason code for drill-down. A
retry or requeue clears both outcome columns before the next attempt. Existing
rows intentionally remain `NULL`; reconstructing semantic outcomes from prose
would make historical rates look more precise than they are.

## Rollout invariant

Applying the schema change remains an explicit operator action: pause or drain
active jobs, back up the SQLite database, and run `gab migrate-db`. Ordinary
`JobQueue` construction validates migration history but never applies pending
migrations.
