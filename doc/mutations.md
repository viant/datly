# Mutations, validation and stable identity

[All guides](README.md) · [Custom handlers](custom-handlers.md)

Choose a generated mutation policy when a declared write graph and typed hooks
express the operation. Choose a custom Go or Velty handler when the application
needs different orchestration. Both use the same canonical binding and Data
transaction owners. HTTP PATCH, POST or PUT alone does not select a write policy.

## Declare what may change

Specify input/output types, writable views/tables, full identity tuples, parent
links, read-only joins and the current-state read needed for comparison. Preserve
four distinct facts:

| Fact | Why it matters |
| --- | --- |
| Original presence | Omitted, supplied NULL, false and zero have different write intent. |
| Original identity | Classifies the request before initialization or sequence allocation. |
| Previous database state | Authoritative current-state comparison, including provenance where required. |
| Working values | May gain defaults, generated IDs and reconciled links without changing original presence. |

Keep `Has`/set markers internal (`json:"-"`), including MCP schema and error
payloads. An ID supplied as zero is still supplied. A newly allocated ID does
not turn an originally new entity into an update. Composite identity matching
must compare the complete tuple, never an incidental prefix.

The [writer examples](../../llm/datly-writer/references/writer-examples.md) and
[writer contract](../../llm/datly-writer/references/writer-contract.md) carry the
maintained authored policy patterns. They also include required behaviors still
under development; use the status boundaries below when selecting a build.

## Lifecycle and validation

Generated policies capture original presence and topology before input
`Init`/`InitMCP`. Preparation follows `SyncPresence` → invariant backfill → entity
`Init` → framework Go/database checks → custom `Validate`. Only then can
sequencing, diffing, identity/link reconciliation and queueing proceed according
to the compiled policy. The active transaction must be established by the Data
owner for phases requiring transactional database checks.

New entities receive complete checks; sparse existing entities use presence-gated
checks. Backfilling a working field does not mean the client supplied it. A native
NOT NULL constraint rejects NULL, not every false or numeric zero. Framework
violations stop custom validation and writes. Author `validate` constraints
through canonical tags/DQL refinements and retain safe structured violations.

Schema inspection is not universal constraint discovery. NOT NULL metadata has
implemented support; complete UNIQUE/reference discovery is not claimed.
Authored native UNIQUE tags remain the explicit contract, and absent driver
metadata means unknown. Automatic UNIQUE discovery expansion is not planned by
this documentation. See the [implementation status](status.md).

## IDs, foreign keys and produced values

Sequence allocation prepares stable IDs **before Queue**. Pending supplied IDs
must be visible to the sequencer so generated values do not collide with known
request identities. `AfterSequence` observes allocated identities; later diff
and reconcile phases preserve original action classification.

The generated mutation program can defer an originally absent child link field
only when its exact captured parent is an INSERT. Captured identity and the
captured full-tuple Previous index establish that action before input
initialization. Business validation postpones only rules that depend on those
unavailable fields. Final Go/NULL/UNIQUE/reference checks run after reconciliation
and before Queue, with no remaining field deferral.

Pending-parent references require the captured edge, unchanged typed topology,
verified INSERT decisions, parent-before-child order and a native match of the
reference target and SQL-bound values in the same transaction. Other references
continue through native database validation. IDs and links are stable before
Queue; parent DML is never flushed early.

Supplied nil/zero remains supplied, including when initialization changes its
working value or marker. Parent UPDATE does not authorize deferral; a nested
insert with a supplied valid FK can still pass ordinary validation. Incomplete
composite identities may omit only exact authorized produced parts and never
match Previous by a prefix. Ordinary and self relations, real SQLite composite
FKs and final Go/UNIQUE failures are covered in this isolated candidate.

This policy belongs to the generated mutation program. Custom Go and Velty
handlers must explicitly invoke that program or compose their own validation.
Native reference metadata remains field-based; this work does not add automatic
compound-constraint discovery. [Status](status.md) records the release boundary.

## Hooks and messages

Typed entity hooks customize preparation, validation, sequence observation and
queue observation. Root outcome finalization receives completion evidence. Keep
business values stable after validation and verify graph/order constraints around
observation hooks; hooks are not permission to silently replace the write graph.

A hook can request the configured invocation-scoped bus:

```go
type RecordHooks struct {
    Bus handler.MessageBus `bind:"kind=mbus,required"`
}
```

Import `handler` from `github.com/viant/xdatly/handler`. The host must supply
the bus; the tag does not create it. Save business event intent during the
appropriate phase and publish a commit-dependent message only after
`outcome.CommitConfirmed()`. Build its identity after sequencing/reconciliation
if the original entity had no allocated ID. The [complete message-hook pattern](../../llm/datly-writer/references/mutation-messages.md)
shows the typed finalizer and failure semantics.

## Transactions and failure

`handler.Data` combines focused DML, sequencer and flush capabilities. Nested
components share the root invocation Data capability. A flush completes a
transaction opened locally for that flush; with a supplied parent transaction,
flush executes into it while the caller retains commit/rollback responsibility.
Queued operations and successful handler return are not commit evidence.

An outcome distinguishes confirmed commit, failure, caller-owned pending work
and uncertain completion. Do not publish a completion message for pending work.
A post-commit bus/finalizer error cannot roll back an already committed database
transaction. After-commit publication by itself is not an atomic outbox or an
exactly-once delivery guarantee.

## Custom orchestration and acceptance

The [demo writer](../standalone/testdata/app/records/records.go) proves a simple
insert plus lifecycle; it is not a generic patch engine. A custom handler must
explicitly preserve validation, sparse presence and identity policy; it does not
inherit every generated phase automatically. Velty composes the same services.

Test mixed new/existing entities, omitted/null/zero values, composite identities,
sequencing collisions, final links, hook order, rollback and caller-pending
transactions with SQLite and database FK enforcement enabled. Generated-code
compilation and API schemas are additional checks, not substitutes for actual
writes. Canonical source/provenance [async replay](async.md) is present in the candidate;
it is integrated in the local `v1` release copy; final release regression remains required.
