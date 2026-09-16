# Mutation hooks and business messages

A typed mutation hook can receive the configured invocation-scoped message bus:

```go
type RecordHooks struct {
    Bus handler.MessageBus `bind:"kind=mbus,required"`
    pending []RecordChanged
}
```

Import `handler` from `github.com/viant/xdatly/handler`. Configure a message-bus
capability for the component; the tag requests that service and does not create
one. The same hook instance retains its business state across entity phases and
root finalization. Hook fields are application state, not client input fields.

Decide whether a message is needed in the business hook. Save a typed payload or
intent on the hook; do not publish a commit-dependent message during validation
or merely because a write was queued. For example, with application types
`Record`, `RecordChanged`, `Input` and `Output`:

```go
func (h *RecordHooks) Validate(ctx context.Context, row *Record,
    state handler.LifecycleContext[Record, handler.NoParent, Output]) error {
    if row.Value >= 10 {
        h.pending = append(h.pending, RecordChanged{ID: row.ID, Value: row.Value})
    }
    return nil
}

func (h *RecordHooks) Finalize(ctx context.Context, input *Input,
    output *Output, outcome handler.Outcome) error {
    if !outcome.CommitConfirmed() {
        return nil
    }
    for _, payload := range h.pending {
        if _, err := h.Bus.Push(ctx, h.Bus.Message("records.changed", payload)); err != nil {
            return err
        }
    }
    return nil
}
```

This is the root `handler/mutation.Finalizer[Input, Output]` contract. Retain the
required `Init` method and the entity/parent types of the selected role. For
newly sequenced IDs, assemble or refresh the event identity after sequencing and
reconciliation, rather than freezing an unallocated ID during Validate. The
example assumes its record ID is already known.

Rollback and caller-pending outcomes do not permit commit-dependent publication.
The caller transaction owner must arrange any later completion notification;
queueing and flushing into that transaction do not establish commit. A bus error
after confirmed commit is a finalization failure, not a database rollback.
Sending after commit alone does not provide atomic DB/message delivery or
exactly-once behavior; retain the application's delivery/idempotency policy.

Verify business-event and no-event cases, SQL failure, and a caller-owned pending
transaction. Assert the payload matches a committed row and that one finalization
occurs. Use a recording bus in tests; no live broker is needed for this contract.

## Async and dry run

Async execution must preserve the original Datly job schema (`DATLY_JOBS`) and
replay through the same canonical execution path. Do not invent a replacement
job table or treat the SDK job model alone as a completed async runtime. Confirm
the connected build supports the requested job lifecycle before promising it.

Original reader dry run prepares execution information without performing the
SQL read. It does not establish a general mutation or external-side-effect
sandbox. Do not equate rollback with a side-effect-free preview or publish a
business completion message for dry-run work.

### Job storage and authorization

Preserve the original 34-column `DATLY_JOBS` schema, including inline JSON `State`,
`Metrics`, `SQLQuery`, event URL and terminal retention. Existing tables are not
implicitly migrated. No replacement scheduler, lease, StateCodec or StateRef
column is required. State captures supported canonical request sources before
Init; invalid or oversized state is rejected. Durable route/identity/state takes
precedence over event contents and stored principal claims.

Configure the existing job store, AFS notification destination, distinct job and
failed-event roots, retention and bounded admission. Host `Jobs`, `JobURL`,
`FailedJobURL`, `MaxJobs` and HTTP `Async` settings require the linked
`AsyncOptions.Authorize` policy. It must refresh current permission at submission,
replay and inspection; serialized claims are not JWT verification. `MaxJobs=0`
means unlimited watcher admission; empty watch JobURL disables polling. Defaults
are one-hour success retention and ten-second error retention, independent of
read-cache TTL. Confirm the connected host's exact settings.

### Durable execution and completion

The framework captures canonical input, inserts PENDING durably, publishes an
AFS `.job` event, authorizes and claims PENDING→RUNNING, executes the same registered
Go component, then records DONE/ERROR only on known completion. Scheduling does
not run the mutation. Replay uses current component metadata and fresh authorized
Current/Previous reads with actual loaded-field evidence. Already admitted calls
retain one generation; old job inputs do not acquire automatic schema migration.

| Condition | Required behavior |
| --- | --- |
| Event publication fails after durable insertion | Retain pending identity/EventURL; report publication failure and retry delivery without resetting executed work. |
| Duplicate event | Consult durable status; never blindly execute running or terminal work. |
| Known handler/SQL failure | Persist ERROR and error expiry before terminal notification. |
| Caller-pending or unknown completion | Retain RUNNING/event; no terminal notification or blind mutation retry. |
| Commit succeeds but terminal persistence fails | Retain completion-pending evidence; duplicate delivery cannot rerun committed writes. |
| Shutdown | Stop admission and join accepted work, notification and AFS post-processing before releasing services. |

Durable insertion and event publication are not one atomic transaction. Recovery
and reconciliation must be explicitly supported; a completion callback is not an
exactly-once guarantee. Use the native SQLX cache/metrics for async readers; a job
ID is not a cache key and job expiry is not result-cache expiry.

### Explicit HTTP job controls

The host's `AsyncRoute` declares an exact route, canonical string `MatchKey` input
and optional boolean `SyncFlag` input. Ordinary binding metadata determines their
locations; do not invent a universal `async=true` parameter. Inspection uses an
independently authored route with declared string JobID and a trusted target.
Status-only inspection does not run the reader. Result inspection requires a
completed reader plus current query/body/JWT authorization and applicable API-key
policy. Default result lookup is cache-only; refresh requires explicit authority.

Check expired/missing result cache (410), incompatible current query (409),
unsupported cache lookup (503), missing job (404) and unavailable/in-progress result
(409) against the connected adapter. Client-supplied target URLs are not authority.
The current `xls` route format forces synchronous behavior; do not infer aliases.
Reader dry run prepares root SQL, rejects unsupported non-reader/partitioned
roots and does not sandbox Init, providers, nested components or external calls.
Live cloud delivery, destination-table output and existing-table migrations need
separate support and acceptance.
