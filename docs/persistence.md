# SQLite persistence contract

The bridge keeps explicit SQL and SQLite-specific concurrency primitives. The
persistence boundary standardizes connections and transactions; it is not an
ORM and must not absorb policy or orchestration decisions.

## Connection policy

`github_agent_bridge.persistence.Database` is the target owner for normal
application connections. New connection sites must use it; existing
feature-local helpers are migrated to it slice by slice without combining that
mechanical change with repository extraction. Callers choose the capability
they need:

| Entry point | Capability | Enforced policy |
| --- | --- | --- |
| `read_write()` | Schema and data writes | 30-second timeout, 30-second busy timeout, `sqlite3.Row`, foreign keys, WAL, autocommit outside explicit transactions |
| `read_only()` | Queries only | SQLite URI `mode=ro`, 30-second timeout, 30-second busy timeout, `sqlite3.Row`, foreign keys, `query_only=ON` |
| `read(operation)` | Named repository/read-model query boundary | Read-only policy plus slow/contention telemetry for the complete logical operation |
| `transaction(DEFERRED, operation=...)` | Unit of work whose first statement determines the lock | Read-write policy plus explicit `BEGIN`, commit on success and rollback on failure |
| `transaction(IMMEDIATE, operation=...)` | Read-modify-write unit that must reserve the writer before reading decisions | Read-write policy plus `BEGIN IMMEDIATE`, commit on success and rollback on failure |

`read_write()` and `read_only()` are low-level connection factories. Repository
and read-model operations should use `read(operation)` and every write workflow
must pass both an explicit transaction mode and a stable lowercase dotted
operation name. Use the weakest transaction mode that protects the invariant.
If one unit of work spans future domain repositories, the service owns one
transaction and passes that connection to every participating repository;
repositories must not open a nested independent transaction.

## Operational telemetry

Named reads and transactions emit a warning when the complete operation takes
at least one second. `SQLITE_BUSY` and `SQLITE_LOCKED` failures emit a contention
warning regardless of duration. Records contain only the logical operation
name, access/transaction mode and elapsed milliseconds; raw SQL, bound values
and SQLite error text are deliberately excluded so tokens, webhook payloads and
other sensitive parameters cannot leak through database telemetry.

Backup and restore are deliberately excluded from repositories. The specialized
`backup_sqlite_database()` and `restore_sqlite_database()` operations use
SQLite's backup API and are shared by autoupdate and the explicit migration
command.

## Adding or changing persisted data

Keep schema, write-model and read-model changes separate and explicit:

1. Add every schema change as the next immutable module under
   `sql/migrations/`, and update `sql/schema.sql` so a fresh database and an
   upgraded database converge on the same schema. Test both paths. Application
   services and maintenance commands must never add columns or indexes as a
   compatibility side effect.
2. Add write and domain lookup methods under `persistence/`. Return typed DTOs
   rather than leaking `sqlite3.Row`; use a named `Database.read()` for reads and
   a named, explicit `Database.transaction()` mode for writes. A service may own a
   cross-repository transaction and pass its connection into repository
   methods when one invariant spans several tables.
3. Add denormalized UI/operational queries to `DashboardQueries`, not to HTTP
   handlers, CLI commands or write repositories. Accept typed, allowlisted
   filters and bind all values. Keep hot endpoints to one query where practical.
4. Exercise repository behavior against a real temporary SQLite database. For
   hot queries, pin the intended indexes with `EXPLAIN QUERY PLAN`; do not mock
   SQL strings as a substitute for schema-level coverage.

`tests/test_persistence_architecture.py` enforces these ownership boundaries.
Direct `sqlite3.connect()` is reserved for `persistence/database.py`, including
the specialized backup/restore functions. SQL execution is restricted to
repositories, migrations and the dashboard read model.

## Retention and cleanup ownership

Retention belongs to the repository that writes each dataset; HTTP handlers
and read models do not delete data as a side effect of reads.

| Data | Owner | Lifecycle |
| --- | --- | --- |
| Webhook receipt payloads | `WebhookRepository` | Pruned on receipt ingestion using the configured webhook retention window. |
| Process samples | `ObservabilityRepository` | Pruned when the monitor records a sample, using the configured sample-retention window. |
| Job runs, acknowledgements, commit statuses, coalesced notifications, session events and progress | `JobRepository` via the `jobs` parent | Deleted by foreign-key cascade when a future explicit job-retention operation deletes the parent job. Runs are never pruned independently because that would corrupt runtime totals. |
| Worklog | Future explicit job-retention operation | The legacy table has no job foreign key. Job cleanup must delete matching `job_id` rows in the same transaction before deleting the job; ordinary reads must not prune it. |
| Ingest receipts and canonical GitHub events | Ingestion/audit retention | They detach from a deleted job with `ON DELETE SET NULL` and remain as deduplication/audit records until a separately configured ingestion-retention policy exists. |
| Feedback events, proposals and learned rules | Feedback operator workflow | Kept until an explicit operator action removes or supersedes them; job cleanup does not own learned policy data. |

There is intentionally no automatic completed-job retention policy yet. When
one is added, it must be an explicit repository/CLI operation with a documented
window, one transaction for parent and non-FK children, and tests proving that
active jobs and retained audit records are untouched.

## Dashboard read model

`github_agent_bridge.dashboard_data.DashboardQueries` is the read-only boundary
used by HTTP handlers, CLI status/list commands, and monitoring. It owns
dashboard SQL and row-to-JSON mapping separately from write repositories so
denormalized UI queries can evolve without leaking `sqlite3.Row` or SQL into
application orchestration.

Job-list callers pass a typed `JobListFilters` value. The query maps each field
to a fixed column and binds every value; callers cannot supply column names.
The list remains one SQL statement and keeps the indexed dashboard ordering.

The read model assumes the current migrated schema. Health/status first
validates migration history and reports `schema_ok=false` for an old or partial
database; normal queries do not carry indefinite `table_exists()` or
`column_exists()` branches. Apply migrations explicitly before starting a newer
dashboard. The module-level query functions remain compatibility shims for
Python callers, while services use `DashboardQueries` directly.

Executor heartbeat writes, acknowledgement claims, streamed session-event
writes, and individual IMAP reader storage operations treat `SQLITE_BUSY` and
`SQLITE_LOCKED` as transient contention after the connection timeout: they wait
and retry instead of terminating the worker, reader pass, or background thread.
Reader retries stay inside the failed SQLite operation, so they do not fetch the
message again or repeat the IMAP `\Seen` side effect. Stream readers enqueue
activity before persistence so a blocked SQLite writer cannot stop draining the
OpenClaw subprocess pipes. Once the main CLI process exits, readers get a
bounded drain window before the bridge stops them and closes its pipe ends; a
descendant that inherited stdout or stderr cannot hold the worker indefinitely.
Acknowledgement retries stop before the external GitHub reaction, so ambiguous
post-side-effect failures still surface. Other `OperationalError` failures
also propagate so schema or storage faults are not hidden. Recovered heartbeat
or session-event contention increments the worker's persisted
`recent_error_count` on the next successful heartbeat.

## Queue transaction boundaries

The following boundaries are behavioral contracts. Repository extraction may
move SQL, but it must not split these units of work.

### Ingestion, deduplication and coalescing

One `BEGIN IMMEDIATE` transaction owns all of these changes:

1. reserve the source receipt;
2. resolve or create the canonical GitHub event;
3. deduplicate or coalesce against active jobs;
4. create/update the job and worklog;
5. create the durable acknowledgement request;
6. link the receipt and canonical event to the selected job.

The transaction commits before optional feedback enrichment. A failed
enrichment must not make an accepted notification disappear; conversely, a
failure before commit must leave no partial receipt, event, job or
acknowledgement.

### Claiming a job

Claiming uses one `BEGIN IMMEDIATE` transaction. The executor pause state is
read only after that transaction begins. Candidate selection, the transition
to `running`, attempt/session metadata, `job_runs` creation and audit/progress
events commit together. This preserves all three locking invariants:

- two workers cannot claim the same job;
- a running `work_key` excludes another active job for that key;
- a completed pause blocks every later claim.

### Finishing or requeueing a job

Finishing, cancelling, blocking and requeueing are read-modify-write
operations and use `BEGIN IMMEDIATE`. Job status/lock changes, the active
`job_runs` result and the associated worklog/session/progress events are one
unit. A failure must roll back the whole transition so operational state does
not claim a result that its run history cannot explain.

### Acknowledgement delivery

Creating an acknowledgement is part of ingestion. Reserving an acknowledgement
for delivery is a separate `BEGIN IMMEDIATE` transaction so two executors
cannot deliver the same pending acknowledgement. The external GitHub reaction
happens after that commit; its success or failure is then recorded in a short,
independent update because an external side effect cannot be made atomic with
SQLite.

## Regression contract

Real temporary SQLite databases, not SQL-string mocks, must protect these
invariants before queue SQL moves behind repositories:

- one canonical job per immutable GitHub event across transports;
- one successful worker claim per job under concurrent access;
- serialization of active jobs sharing a `work_key`;
- pause-state reads inside the claim transaction;
- rollback of acknowledgement/job creation and job/job-run transitions as
  complete units.
