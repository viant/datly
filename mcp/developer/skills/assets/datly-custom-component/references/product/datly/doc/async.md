# Async jobs, storage events and completion

[All guides](references/product/datly/doc/README.md) · [Mutations](references/product/datly/doc/mutations.md)

The programmatic async services persist a job before publishing an AFS event,
then replay it through the same registered component engine. They preserve the
original Datly job model and execution boundaries. The candidate includes explicitly wired HTTP negotiation, canonical replay,
reader results and differ integration. Standalone configuration wires these same owners when a linked host supplies
explicit authorization; see [configured async](references/product/datly/standalone/ASYNC.md).

## Storage contract

The durable table retains the original **34-column `DATLY_JOBS` schema**. Runtime
`jobs.Record` adds persistence-only `State`, `Metrics` and `SQLQuery` to the SDK
job model. There is no replacement job table, StateCodec/StateRef column, lease
column or separate scheduler row shape.

Provision the original-shaped table through your database deployment process,
or use the existing SQLX missing-table creation (`DisableTableCreation=false`).
Existing tables are not migrated. Current SQLite DDL is test-harness data, not a
portable production schema API.
`bootstrap.JobStoreConfig` selects the named SQL connector, optional dataset and
table (default `DATLY_JOBS`). Native SQLX handles typed persistence and dialect
placeholders. Existing SQLite acceptance does not prove live BigQuery, MySQL,
PostgreSQL or cloud event delivery.

`State` is the inline JSON continuation payload captured by canonical Bindly
replay plans before Init. It preserves supported request sources and typed
canonical values without creating a second binder.
Invalid/oversized state is rejected; the original 63 KiB truncation behavior is
not used to silently corrupt replay. Durable row identity, route and state are
authority; event state or embedded principal fields cannot override them.

## Enable the application-owned services

An embedding host supplies `application.WithAsync(application.AsyncConfig{...})`
when constructing its Manager. Required choices are:

| Choice | Purpose |
| --- | --- |
| `Store` | Caller-supplied native SQL-backed job store on the intended connector/table. |
| `Authorize` | Mandatory policy for submission, replay and status; refresh current access. |
| `FS`, `Notification` | AFS service and storage-event destination. |
| `Watch` | Optional JobURL/FailedJobURL polling, MaxJobs and error callback. |
| `TTL`, `ErrorTTL`, `Notify` | Job terminal retention and post-persistence notification policy. |

The [current API](references/product/datly/application/async.go.txt) defines the exact structs; see the
[configuration guide](references/product/datly/doc/configuration.md) for deployment settings. The watcher starts on the first successful publication.
An empty watch JobURL disables polling while programmatic scheduling/status and
external dispatch remain available. Notification destination separately controls
where scheduling publishes. Configure explicitly distinct job and failed-event
locations. `MaxJobs=0` preserves unlimited watcher admission; choose a positive
bound when that is not intended.

Use `Manager.ScheduleJob`, `JobStatus`, `RepublishJob`, `HandleJob` or
`DispatchStorageEvent` for their respective operations. These APIs take trusted
canonical values and explicit application policy. HTTP controls must bind declared input fields and exact authored routes, as
described below; there is no universal `async=true` query parameter. Standalone
accepts `Jobs` plus HTTP `Async` routes and the original watcher fields, but
rejects startup without linked `AsyncOptions.Authorize`. The
[configuration and host contract](references/product/datly/standalone/ASYNC.md) describes defaults,
worker inspection without request input, FS ownership and default denial.

## How a job runs

1. Capture detached canonical input before input initialization. For supported
   readers, prepare root query information without performing the root SQL read.
   Scheduling a Go/Velty job does not execute its handler or mutation.
2. Insert a durable PENDING row with route, event destination and expiry metadata.
3. Publish an original-compatible `.job` event through AFS. A temporary `.upload`
   is moved into place so local polling does not consume an incomplete event.
4. Dispatch an AFS storage object or let the watcher admit it. Reload the durable
   row and authorize against current access; guard the PENDING→RUNNING claim.
5. Execute the registered reader or custom Go/Velty handler through the canonical
   binding/DI/Data engine, with scoped event/job metadata and a synthetic local
   request retaining the persisted method/URI. There is no HTTP loopback.
6. Observe canonical completion. Persist DONE/ERROR only when outcome evidence
   permits it, then notify. The polling watcher deletes successful events or
   archives failures; uncertain completion retains the event for reconciliation.

A scheduled row is replayed against metadata current at dispatch, including after
reload. Already admitted operations keep their generation. This does not persist
an executable old generation or promise automatic input migration across contract
changes.

## Native cache and metrics

Async readers use the same native SQLX read services as synchronous readers.
There is no job-specific row cache. Existing execution metrics populate stored
Metrics/SQLQuery and actual native cache key/set/namespace fields where emitted.
A job ID is not a cache key; job retention is not read-cache expiry. A destination
configuration does not establish support for arbitrary destination-table output.

## Completion and failures

| Event | Result and operator responsibility |
| --- | --- |
| Event publication fails after insertion | Retain the pending row/EventURL; return `PublicationError` with identity. `RepublishJob` retries delivery without resetting executed work. |
| Duplicate terminal/running event | Consult durable status; do not rerun blindly. |
| Handler/SQL failure with known completion | Persist ERROR and its error TTL; notify only after terminal persistence. |
| Caller-owned pending transaction or unknown completion | Return `ErrCompletionPending`; keep RUNNING and event, without terminal callback. Reconciliation remains an explicit gap. |
| Commit confirmed but terminal row update fails | Keep RUNNING and return completion-pending; duplicate delivery must not execute the mutation again. |
| Shutdown | Stop admission, cancel and join accepted jobs, watcher post-processing and notification before releasing services. |

Default job success TTL is one hour and error TTL ten seconds when no override is
supplied. These are row expiry policies, not crash-recovery or delivery guarantees.
SQL insertion and event publication are not one distributed atomic transaction.
After-commit notification alone is not an outbox or exactly-once messaging.

## Authorization and replay limits

Stored UserID/email/claims do not prove current permission. The mandatory policy
must revalidate access and refresh declared authorization inputs. There is no
ambient JWT input or automatic verification of serialized claims. Read the
[JWT input pattern](references/product/datly/doc/security.md) before connecting a transport submit path.

The approved async rebase uses canonical Bindly source replay and refreshes
Current/Previous dependency reads with their real provenance before generated
mutation policy runs. It does not restore fabricated read metadata from JSON.
The scoped differ delegates to the native comparison owner. Status/result access
revalidates current declared inputs; result inspection also applies the target
route's API-key policy when configured. This is candidate evidence, not a promise
that every future contract change can replay old jobs.

Reader dry run only prepares supported root SQL. It rejects non-reader handlers
and partitioned roots and does not sandbox Init, providers, templates, nested
components or arbitrary external effects. Rollback is not a side-effect-free
mutation preview.

Remaining limits include destination-table output, schema migration of existing
tables, reconciliation/recovery, and live cloud/database qualification. Match-key reuse
and explicit HTTP controls exist in the candidate; they do not establish
exactly-once delivery or a distributed transaction.

## Explicit HTTP scheduling, status and results

The host's `gateway/http.AsyncRoute` names an exact `Route`, canonical string
`MatchKey` input and optional boolean `SyncFlag` input. These are input contract
names, not hardcoded HTTP parameter locations. The ordinary input tags determine
where values come from. Register the application-owned async admission/service
on the same Manager generation as the route.

For inspection, an independently authored route uses `AsyncInspect{JobID, Target,
Result}`: `JobID` names a declared string input, `Target` identifies the protected
route, and `Result=false` performs status-only inspection without a reader call.
`Result=true` requires a completed reader and current query/body/JWT sources.
An explicit result `SyncFlag` requests refresh; otherwise lookup is cache-only
within the completed job's persisted SQL query scope.

The adapter reports missing/expired result cache as HTTP 410, incompatible
current query as 409, unsupported cache lookup as 503, missing job as 404 and
in-progress/unavailable results as 409. No arbitrary client target URL is accepted.
The `xls` format forces synchronous behavior in the current adapter; do not infer
additional format spellings or a universal download scheduling rule.

## Job schema and filesystem checklist

The [SQLite schema fixture](references/product/datly/internal/testharness/sqlite/jobs_schema.go.txt) contains
the exact 34 original-shaped columns. It is a runnable test schema, not a portable
production migration. Keep `State`, `Metrics`, `SQLQuery`, event URL and terminal
retention fields; do not add `StateCodec`, lease or alternate scheduler columns.
AFS owns event writes/moves, watcher discovery, successful deletion and failure
archival. Configure credentials, job/failed roots and concurrency explicitly.
Exercise a durable insert before publication, duplicate delivery, current JWT
denial, fresh dependency read, failed terminal persistence, caller-pending
transaction and shutdown with active filesystem work. A dryrun is reader SQL
preparation only; it cannot promise that providers, Init or nested work have no
external effects.
