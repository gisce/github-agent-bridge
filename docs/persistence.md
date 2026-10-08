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
| `transaction(DEFERRED)` | Unit of work whose first statement determines the lock | Read-write policy plus explicit `BEGIN`, commit on success and rollback on failure |
| `transaction(IMMEDIATE)` | Read-modify-write unit that must reserve the writer before reading decisions | Read-write policy plus `BEGIN IMMEDIATE`, commit on success and rollback on failure |

Read models should use `read_only()`. A write workflow should use the weakest
transaction mode that protects its invariant. If one unit of work spans future
domain repositories, the service owns one transaction and passes that
connection to every participating repository; repositories must not open a
nested independent transaction.

Backup and restore are deliberately excluded. They use SQLite's backup API in
`autoupdate.py` and remain specialized database operations rather than
repository methods.

Executor heartbeat writes, acknowledgement claims and streamed session-event
writes treat `SQLITE_BUSY` and `SQLITE_LOCKED` as transient contention after the
connection timeout: they wait and retry instead of terminating the background
thread. Stream readers enqueue activity before persistence so a blocked SQLite
writer cannot stop draining the OpenClaw subprocess pipes. Acknowledgement
retries stop before the external GitHub reaction, so ambiguous post-side-effect
failures still surface. Other `OperationalError` failures also propagate so
schema or storage faults are not hidden. Recovered heartbeat or session-event
contention increments the worker's persisted `recent_error_count` on the next
successful heartbeat.

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
