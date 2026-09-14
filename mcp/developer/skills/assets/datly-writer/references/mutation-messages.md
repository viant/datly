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
    state handler.EntityState[Record, handler.NoParent]) error {
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
