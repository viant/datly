# Datly 1.0 writer contract

For current enablement and pending integration boundaries, consult [availability-and-operations.md](references/product/llm/datly-writer/references/availability-and-operations.md) when using operational or extension features below. Required behavior is not a claim of connected-build availability.

Use this reference when authoring an application component that changes data. It describes the agreed Datly 1.0 contract, including required features still being integrated. Do not silently omit a required phase because the installed implementation is incomplete. Check the implementation-status section before promising that an endpoint meets the complete contract.

The application developer should work with DQL, Go shapes, tags, public handler interfaces, and application business rules—not with Datly's internal emitters or transaction implementation. Use the actual developer-MCP tools exposed by the environment; inspect their schemas instead of inventing tool names or request fields.

## 1. Mental model

Datly supplies typed binding, reads, sequencing, validation, buffered writes, and transaction completion. Application code supplies authorization and business policy. DQL is a declarative source for component metadata and typed views; it is not permission to mutate every table mentioned in a query.

| Term | Meaning for a writer |
|---|---|
| Component | An input/output contract, routes, handler identity, and configured dependencies. |
| Entity / writable record | A typed Go record whose columns may participate in the selected write policy. |
| Current / previous read | A typed database lookup used to identify existing records and obtain values needed for validation/backfill. |
| Original snapshot | Immutable request-side values, identity suppliedness, and presence captured before input initialization. It is not the previous database row. |
| Presence marker | Internal field bits distinguishing omission from an explicitly supplied zero, false, empty string, or null. |
| Auxiliary view | Read/business input that is available to the component but excluded from generated mutation traversal. |
| DerivedView | A query-derived relation/output, such as a count or aggregate. It is not implicitly a writable entity. |
| Queued write | A buffered operation. It is not proof that SQL executed or that a transaction committed. |

Keep authorization filters and current-row lookups scoped to the caller. A record existing in the database is not, by itself, authorization to update it.

## 2. Select graph generation and contract ownership

The standard authoring surface is a reader-like DQL graph plus an explicit `gen`
operation and pure Go output. Link authoritative application types when they
exist; otherwise generate owned shapes. Application Go hooks carry business
rules. Generation owns binding, Previous reads and mutation orchestration.
See [writer-examples.md](references/product/llm/datly-writer/references/writer-examples.md) for the primary graph.

Existing Go-only components remain supported for explicitly chosen application
contracts. Preserve their types and authored methods. In generated shapes,
update only owned projections: append fields, propagate CAST type changes and
remove dropped owned columns without rewriting unrelated authored content.

| Operation | Policy |
| --- | --- |
| `post` | Insert intended writable records. |
| `put` | Update under the declared policy; do not infer insert-on-miss. |
| `patch` | Match original supplied tuples and apply declared existing/missing actions. |
| `get` | Generate the reader graph; no mutation traversal. |

Route method and operation must agree. Custom Go orchestration is an explicit
application choice; it is not a replacement for missing high-level generation.
Discover connected `gen` support as described in
[developer-mcp.md](references/product/llm/datly-writer/references/developer-mcp.md).

## 3. Go shapes and tags

Inspect the generated row shape or link an existing authoritative application type.
The fragment below explains SQL mapping and internal presence; standard `gen`
derives component/body/output binding and its handler from the DQL graph.

```go
type Record struct {
    ID   int    `json:"id,omitempty" sqlx:"ID,primaryKey=true,autoincrement=true"`
    Name string `json:"name,omitempty" sqlx:"NAME"`

    Has *RecordHas `json:"-" sqlx:"-" setMarker:"true" typeName:"RecordHas"`
}

type RecordHas struct {
    ID   bool
    Name bool
}

```

Do not copy `ID int` or an autoincrement policy into schemas whose actual identity
type/policy differs. Go component/binding tags below describe generated metadata
and explicitly existing Go-only contracts; they are not an additional writer
plumbing step.

Relevant authoring tags:

| Tag / option | Purpose |
|---|---|
| `component:"Name,path=...,method=...,connector=...,handler=...,view=..."` | Declare the component and its named handler/route metadata. |
| `parameter:"Name,kind=...,in=..."` or `bind:"..."` | Bind a field through its source provider. Do not put both aliases on one field. |
| `required`, `errorCode`, `errorMessage` | Binding requirements and authored safe failure metadata. Binding requirements do not replace entity validation. |
| `minAllowedRecords`, `maxAllowedRecords`, `expectedReturned` | Minimum, maximum, and exact source-record counts. Counts apply at the binding source, before codecs reshape the result. |
| `view:"Name,type=...,table=...,connector=..."` | Typed view metadata. Current-state reads are ordinary views. |
| `sql:"SELECT ..."` / `sql:"uri=namespace:path.sql"` | Inline or resource-owned SQL. The resource must actually resolve. |
| `codec:"structql,uri=..."` | Derive typed data from a source parameter through StructQL. Other codecs use their registered names and contracts. |
| `on:"ID=ParentID"` | A parent-to-child relation key link. Composite relations carry all required links. |
| `self:"child=ID,parent=ParentID"` | A self-tree holder's local identity/parent-field relation. |
| `view:"...,auxiliary=true"` | Business/read-only input excluded from generated writes. |
| `view:"...,entityHooks=app.RecordHooks"` | The canonical hook type for a view. Preserve full package identity; do not resolve types by basename alone. |
| `invariant:"Schedule"` | A field belongs to one named invariant group. |
| `json:"-" sqlx:"-" setMarker:"true"` | Internal presence metadata, not application payload data or MCP input schema. |

Current views may use reader controls such as `limit`, `batch`, `batchConcurrency`, `match`, `publishParent`, partitioning, and read callbacks. Fetch concurrency is not permission to run mutation phases or commits concurrently.

Route activation is separate from write policy. For example:

```go
ID *int `parameter:"ID,kind=path,in=id,uri=/{id},required=true" mcpEnabled:"true" pathMcpEnabled:"true"`
```

The URI-specific parameter belongs to the activated route, not automatically to the base route. An MCP path variant receives its own derived name, such as `ThingsById` for a tool named `Things`. This does not automatically copy the URI ID into a body entity's key. Declare and validate that relationship explicitly.

An MCP tool's argument object follows its published schema. Do not assume it is identical to the HTTP JSON envelope.

Report/cube composition and DerivedViews are reader/report features. Enabling them, or enabling a URI variant, does not make their data writable.

## 4. DQL graph and generated Previous reads

Declare writable tables and relation keys in the reader-like graph. Parenthesized
physical tables, such as `JOIN (LOOKUP_TABLE) lookup ON lookup.ID=r.LOOKUP_ID`,
are auxiliary: readable by business logic and excluded from sequencing, mutation
hooks, relinking and DML. A parenthesized physical root is also auxiliary.
A real `(SELECT ...)` subquery retains its declared query meaning.

Attach `entity_hooks(r, 'hooks.RecordHooks')` using a declared package import.
Attach validation and cohesive groups with column `tag` annotations, for example
`tag(r.START, 'invariant:"Schedule"')` and
`tag(r.END, 'invariant:"Schedule" validate:"gtfield(Start)"')`.
Use exact resolved Go field names in cross-field validation rules.

The generator derives body/output bindings and typed Previous reads restricted to
all requested original identity tuples and declared authorization. Check the
preview for complete composite keys, authorized scope and no accidental
pagination/truncation. Do not author manual Body/Existing/Data or key-extraction
plumbing for standard `gen`. The generator must report unsupported metadata or
capabilities instead of producing a broader table scan.

## 5. Required generated mutation lifecycle

The generator supplies this order. Use it to review hooks and observable behavior;
do not implement these phases as an authored DQL program:

1. Bind the component input and completed current reads. Capture immutable original presence/identity and a detached processing baseline **before** input `Init` and `InitMCP`.
2. Run input `Init`, then `InitMCP` when MCP context is present. `InitMCP` remains supported.
3. Prepare invocation-local dependencies and hook instances. Do not perform business initialization in dependency preparation.
4. Run recursive `SyncPresence` against the captured processing baseline; prepare typed entity frames and database `Previous` values.
5. Run invariant-group backfill when needed, without marking hydrated fields supplied.
6. Run entity `Init` using marker-aware setters for business changes.
7. Run framework Go-tag and database-derived validation, then custom entity `Validate`.
8. Begin/join the managed transaction before sequencing.
9. Register captured originally supplied IDs with the scoped sequencer, allocate stable IDs for eligible original-unassigned insert candidates, then run `AfterSequence`.
10. Diff against original identity and the frozen database match; determine allowed insert/update actions.
11. Reconcile identities and populate declared parent/self foreign keys from the allocated IDs. Run final validation over the values that will be written, with no unresolved deferred constraints. Business values must remain frozen.
12. Prepare detached typed write payloads and queue them in deterministic traversal order.
13. Run observational `AfterQueue`, verify that queued working values/markers/topology did not change, and populate the output from the working body.
14. Let the existing invocation owner flush/complete its transactions. Invoke one outcome-aware finalization path.

The sequencer owns ID reservation and allocation. Generated reconciliation owns foreign-key population. IDs and links must be ready before Queue; neither allocation during INSERT/Flush nor relationship backfill after execution satisfies this lifecycle. Supplied-ID collision handling must use native sequence identity and reservation semantics. Final duplicate checks remain safeguards.

There is no automatic second `SyncPresence` between entity `Init` and `Validate`. Setters provide the presence signal for `Init` changes. Graph membership, ordering, parent edges, and self holders must not change after initial synchronization/frame preparation; reject such changes rather than silently ignoring added records. Construct the graph before capture or use an explicitly custom orchestration policy.

`AfterSequence` may perform only the identity/link work allowed by the compiled policy; it may not change validated business values or their markers. `AfterQueue` must observe rows, not alter identities, links, business values, markers, or graph structure.

## 6. Original identity and sparse presence

Identity suppliedness comes from the immutable original marker—not `ID != 0`, a sequencer-mutated value, or the current mutable marker.

- Original marker true with identity zero is an explicitly supplied identity.
- Original marker false with a later nonzero sequenced/derived ID remains a new/unassigned request entity for matching purposes.
- A supplied but unmatched identity does not guarantee UPDATE; apply the declared missing-row policy.
- A missing original marker is different from a known marker with all bits false. Do not silently invent suppliedness.
- Original supplied composite keys must be complete. Reject partial tuples unless an explicitly implemented producer policy defines them; never guess missing components.
- Multiple new unassigned entities remain separate inserts. Do not report them as duplicate zero/null identities merely because their initial values match.
- Validate duplicate assigned tuples even when a positional fast path appears to match them.
- An unresolved insert identity cannot be left for SQLX to backfill only into a detached write DTO while the returned working body remains stale. Require a completed declared producer or an explicit marker-aware assignment. This readiness check is not UPDATE classification.

Matching the request graph to its processing baseline is separate from matching database rows:

- Processing association may prefer stable source pointer identity. For collections, try the same position, verify identity, then use an identity-index fallback where association is provable.
- Reordered/replaced value-slice items with no supplied identity can be ambiguous. Do not match them by zero key, slot address, or coincidental business equality. Return an actionable ambiguity error when association cannot be proven.
- Database `Previous` matching is identity-only. Sparse request fields are not required to equal the database row.
- No arbitrary framework depth cap should truncate a valid graph. Retain real cycle and ambiguous-parent detection.

Owned generated entities expose typed getters/setters and `SyncPresence(snapshot handler.EntitySnapshot[T]) error`. Getters do not mark fields. Setters assign the exact field type and mark presence. For an imported/linked entity, call `snapshot.SyncPresence(entity)` instead of trying to add a method to a foreign Go type. A nil current entity is a no-op; a nonnil entity without a usable baseline is an error. Failed recursive synchronization must leave all working markers unchanged.

## 7. Reusable typed entity hooks

Application hooks use public interfaces from `github.com/viant/xdatly/handler`:

```go
type RecordHooks struct {
    Input  *RecordInput   `parameter:",kind=input"`
    Logger handler.Logger `parameter:",kind=logger"`
}

func (h *RecordHooks) Init(
    ctx context.Context,
    current *Record,
    state handler.EntityState[Record, handler.NoParent],
) error {
    return nil
}

func (h *RecordHooks) Validate(
    ctx context.Context,
    current *Record,
    state handler.EntityState[Record, handler.NoParent],
) error {
    return nil
}
```

The empty bodies above show the verified signature, not complete application validation. Implement the requested business behavior.

`EntityState[T, P]` supplies:

- `Previous *T`: detached database projection, treated as read-only.
- `PreviousFields`: actual loaded-field evidence, using canonical Go names. An unselected field is not a known zero.
- `Original`: immutable original suppliedness, also using canonical Go names.
- `Parent *P`: the enclosing declared relation parent; root uses `handler.NoParent` with nil parent.
- `SelfParent *T`: the immediate self-edge parent, nil at a self-tree root. Self descendants retain their enclosing `Parent`.

Instantiate and bind one hook object per view/role per invocation. Reuse it across `Init`, `Validate`, optional `AfterSequence`, optional `AfterQueue`, and supported root finalization. Do not share mutable request hook state globally. Application input/configuration/logger/message-bus dependencies belong on the hook object through normal scoped binding; entity types need not acquire an `any` parent or a large session facade.

Use exact generic signatures and canonical package identities. A different parent's hook type, an unknown type, or a same-short-name type from another package must not silently match.

## 8. Invariants and backfill

An invariant groups business fields that must be validated together. A group can contain three, four, or more fields; it is not limited to pairs.

```go
Start *time.Time `invariant:"Schedule"`
End   *time.Time `invariant:"Schedule"`
Zone  string     `invariant:"Schedule"`
Days  []string   `invariant:"Schedule"`
```

These are field fragments; also declare their SQL/JSON metadata and presence bits as appropriate. Each field currently declares one group. Do not silently split a comma-separated list into overlapping groups.

The generated application methods are:

```go
HasScheduleChanges() bool
BackfillScheduleIfNeeded(previous *Record, previousFields handler.FieldSet) error
```

Backfill applies only when the group has changes and some members are omitted. Copy only omitted members from the matched, actually loaded `Previous`; retain explicitly supplied zero, false, empty, and null values. Validate all necessary source fields and prepare detached copies before assigning anything, so an error cannot partly hydrate the group. Hydration must not mark omitted fields supplied or overwrite original presence.

No previous row is normal for an insert: backfill is a no-op and entity `Init` may derive defaults. An update that requires a previous value must fail explicitly if the row/evidence needed for that value is unavailable. The generated runtime must provide actual `PreviousFields`; nil must not mean “assume every database field was loaded.” A deliberate standalone helper caller may use nil to assert that its supplied typed previous value is complete.

Validate the complete effective invariant after backfill and initialization, even though hydrated members retain false presence bits. Ordinary field-level sparse gating must not skip a cross-field invariant that was activated by another member.

## 9. Composite keys, links, and read batching

Use typed tuples for compound identity. Do not concatenate strings or replace `(tenant,id)` membership with independent `tenant IN (...) AND id IN (...)` lists: that admits cross-product rows.

The generator must derive the full ordered key tuple from the request, using typed SQL-column metadata and the native dialect's composite membership support. Do not hardcode row-value-IN syntax or placeholder numbering; dialects may use different rendering. Bind values and account for all query arguments when applying placeholder/batch limits.

Current reads can be partitioned or fetched in bounded batches. Supported read controls include Go `batch` / `batchConcurrency` and DQL `batch_size(alias,n)` / `batch_concurrency(alias,n)`. Serial is the default. This controls current/relation fetching, not concurrent mutation commits. Hooks consuming a completed relation must see the full assembled relation, not a fetch-batch prefix.

During reconciliation:

- Restore a matched existing entity's working/output identity to its captured matched tuple before propagating links.
- Generic updates exclude identity columns from UPDATE SET. They do not implement primary-key-changing mutations.
- Use every canonical parent-link component and its checked pointer/value conversion.
- Self holders may have different links; retain the exact holder context.
- A link that would change an existing primary key is an error, not implicit reparenting through a new key.
- Preserve existing pointer-holder identities so value children remain attached to the same working graph.
- Validate final identities/collisions before queueing, and return generated IDs/links in the original working body order.

Ambiguous multiple-parent/self-holder contexts must be diagnosed. Do not resolve them by choosing whichever graph path happened to be visited first.

## 10. Framework and application validation

This is a required target feature, not optional polish:

- Obtain database constraints from canonical schema metadata and propagate appropriate validation tags to generated shapes.
- Allow DQL `tag(view.column, 'validate:...')` to customize the field's authored validation metadata.
- Validate new rows as complete inserts. For sparse updates, gate ordinary required/presence checks by effective suppliedness and still validate activated invariants completely.
- Run framework Go-tag validation and database-backed checks before calling custom entity `Validate`.
- Keep database access inside the scoped validation/data owners; do not pass raw DB/transaction handles to entity hooks.
- Preserve database uniqueness, foreign-key, nullability, and actual execution errors as final authority. Preflight checks cannot eliminate concurrent constraint races.

SQL `NOT NULL` does not mean “nonzero” or “nonempty.” Integer zero, false, and an empty string may all be non-null database values. A nullable pointer carrying nil is different from a present scalar zero. Do not translate every NOT NULL column into an indiscriminate nonzero rule.

The public validation capability returns `*handler.Validation`. Use `result.Err()` to distinguish success from failure; a nonnil result object is not itself a failure. Failed validation defaults to status 422 unless an explicit code is supplied. Do not queue writes after a validation failure.

## 11. Capabilities, transactions, and outcomes

Prefer focused invocation capabilities:

| Capability | Public operations / responsibility |
|---|---|
| `handler.Binder` | `Bind(ctx,target)`, `Lookup(ctx,key)`; one invocation scope. |
| `handler.Sequencer` | `Allocate(ctx,table,dest,selector)`; prepares identifiers. |
| `handler.DML` | `Insert`, `Update`, `Delete`, `Execute`; buffers operations. |
| `handler.Flusher` | `Flush(ctx,table)` when explicit custom orchestration needs it. |
| `handler.Data` | The existing aggregate of DML, sequencing, and flushing. |
| `handler.TransactionStarter` | `Start(ctx)`; opens or joins the managed unit, not commit/rollback control. |
| `handler.Validator`, `Logger`, `MessageBus` | Configured services; do not assume an unconfigured capability exists. |
| Opt-in connector provider | `Connector(ctx,name)` returns a borrowed exact-name DB for specialized integration. It is outside managed Data participation unless explicitly integrated. |

Do not install a second binder, transaction engine, raw row-map pipeline, or write/cache invalidation layer. A called component reuses the appropriate root data ownership. Different databases remain distinct units; do not promise a distributed atomic commit.

Queueing, flushing into a caller-owned transaction, and committing are different outcomes. The unified finalizer receives `handler.Outcome`; use `CommitConfirmed()` before commit-dependent message publication. It is false for no transaction, caller-pending work, failures, unknown completion, or partial/mixed unit completion. A post-commit finalizer failure must not be reported as a database rollback.

Use the same root hook object for finalization where configured. An explicit definition-level finalizer may also cover pre-capture failures and must be safe for concurrent invocations. Cancellation must not suppress outcome notification. An outbox can be an application design choice, but Datly must not silently create an unrequested durable messaging subsystem.

## 12. Explicit custom Go orchestration

Use the custom-component skill only for an explicitly application-owned workflow.
Its typed Go handler supplies `handler.Contract[I,O]` and receives scoped services.
It must own any custom identity, validation and completion policy. The standard
writer remains a declarative graph with generated pure Go and application hooks.
Do not combine legacy row-writing callbacks with the generated entity lifecycle.

## 13. Output and error control

The generic writer output is the transformed working body, including populated IDs and reconciled links—not the frozen Current projection or a map-shaped reconstruction. Custom handlers may produce their declared typed output or a public transport response.

Application code can explicitly supply a safe error status and body:

```go
return &response.Error{
    Code:    409,
    Payload: &ErrorPayload{Message: "record conflict"},
    Cause:   err,
}
```

Import `response` from `github.com/viant/xdatly/response`. `ErrorPayload` is the application's typed public payload. `Cause` remains private/wrappable; do not place confidential SQL, credentials, or internal diagnostics in the payload. An explicit nil payload means JSON null, not “use the default error body.”

The target HTTP and MCP adapters must preserve explicit status/body intent in their protocol-supported representation, including wrapped errors. Do not assume that setting an output field named `Status` changes HTTP status or MCP error state. `session.Response().SetStatusCode(code)` controls the response status; returning a typed error communicates failure. Validation and binding errors retain their safe authored status/message contracts.

For scoped message-bus hook examples and the original async job/dry-run boundary, see [mutation-messages.md](references/product/llm/datly-writer/references/mutation-messages.md).

## 14. Regeneration and delivery checks

Persisted dynamic shapes follow the current SQL projection for fields proven to be generated-owned. For example, changing `CAST(view.column AS int)` to `CAST(view.column AS *int)` changes the existing Go field to `*int`; reversing the CAST restores `int`. Removing a projected column removes its owned field and corresponding generated presence/accessor support. This applies to readers and generated Go mutation components, including repeated generation with the same catalog.

Retain the order of surviving fields, append new fields, and preserve unrelated authored fields, comments, methods and tags. The projection inventory and fingerprints establish edit/removal authority; an older incomplete inventory must not authorize deletion of unproven fields. Reject conflicts with authored changes explicitly. Regenerate Go handlers and router artifacts under their ownership checks. Do not delete unknown files, remove manifests to bypass protection, or overwrite edited generated artifacts to make a run pass.

SQL text may be maintained as stable named resources while Go tags remain stable. Register compiled `embed.FS` resources explicitly with the shared Bindly store, or use the configured package loader's resource discovery. Missing namespaces/resources are errors, not a reason to substitute unrelated inline SQL. Preserve old resource/shape ownership when views disappear or are renamed.

Before claiming a writer complete, verify through an isolated SQLite-backed application/runtime path:

- POST, PUT, and PATCH actions actually requested by the application.
- Omitted versus explicit null/zero/false/empty values.
- Original supplied zero, later sequenced IDs, and mutated existing IDs.
- Composite tuple lookup, reordered/subset arrays, duplicates, partial identities, and separate new inserts.
- Parent/self links and returned IDs matching persisted IDs.
- Auxiliary/DerivedView records remain unwritten; use database triggers to catch accidental writes when useful.
- Invariant backfill preserves presence and fails atomically on missing projection evidence.
- Framework validation runs before custom validation and before writes.
- Queue/commit/rollback/caller-pending behavior, cancellation, and exactly one outcome finalization path.
- HTTP and MCP sparse-body/schema/error behavior where exposed.
- Regeneration preserves application code and stable shapes/resources.

## Implementation status to check before delivery

This section is a delivery warning, not a reduction of the target contract above.

| Area | Current checkout status |
|---|---|
| High-level graph + operation `gen` to pure Go | Implementation delivered in an isolated review worktree; under review. Release CLI availability is not established. Discover connected support; missing `gen` remains a reported capability gap. |
| Original snapshots, recursive sync, typed hooks, invariant helpers, typed Previous evidence, sequencing/diff/reconcile/queue, output/finalization | Required generated behavior. Verify generated Go fixtures and hook preservation on the connected build; parser or metadata acceptance alone does not establish it. |
| Framework Go + DB validation, schema-to-validate tags, and DQL validation customization execution | Current framework Go/database validation and generated NOT NULL/customization paths have native/generated SQLite tests. Complete UNIQUE/reference discovery is not established; authored native UNIQUE tags remain explicit. Relation-produced FK deferral is restricted to captured parent INSERTs and final validation runs before Queue. Never substitute custom-hook-only validation or infer constraint absence from missing metadata. |
| Native `OnInsert` / `OnUpdate` callbacks and native default generators inside the generic post-validation write path | Explicitly rejected by generic policy until moved to a safe earlier phase. Ordinary custom handlers retain native behavior. |
| Generic primary-key-changing updates | Unsupported; existing identities are restored. Use explicitly custom orchestration for a different policy. |
| Ambiguous value associations or conflicting parent/self-holder contexts | Explicit errors, not guessed matches or arbitrary truncation. |

Use the public interfaces and examples in [tags-and-interfaces.md](references/product/llm/datly-writer/references/tags-and-interfaces.md) and [writer-examples.md](references/product/llm/datly-writer/references/writer-examples.md). Application authoring does not require framework source access.
