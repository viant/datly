# Datly 1.0 writer contract

For current enablement and pending integration boundaries, consult [availability-and-operations.md](../../../../availability-and-operations.md) when using operational or extension features below. Required behavior is not a claim of connected-build availability.

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

The standard authoring surface is a reader-like DQL graph plus an explicit `transcribe`
operation and pure Go output. Link authoritative application types when they
exist; otherwise generate owned shapes. Application Go hooks carry business
rules. Every complete writer example declares `input_type`, `output_type`, the
row type, a typed main output holder such as
`#define($_ = $Data<[]*Record>(output/body))`, and
`#setting($_ = $case_format('lc'))`. `Data` names the Go output field; `body`
selects the generated writer output binding. Global Structology casing owns
routine lowercase/camel-case presentation; do not add JSON tags or holder
`WithTag` for that purpose. Generation owns binding, Previous reads and mutation
orchestration.
See [writer-examples.md](../../../../writer-examples.md) for the primary graph.

Existing Go-only components remain supported for explicitly chosen application
contracts. Preserve their types and authored methods. In generated shapes,
update only owned projections: append fields, propagate CAST type changes and
remove dropped owned columns without rewriting unrelated authored content.

| Operation | Policy |
| --- | --- |
| `post` | Insert intended writable records. |
| `put` | Update under the declared policy; do not infer insert-on-miss. |
| `patch` | Match resolved complete tuples against authorized Previous and apply declared existing/missing actions. |
| `get` | Generate the reader graph; no mutation traversal. |

Route method and operation must agree. Custom Go orchestration is an explicit
application choice; it is not a replacement for missing high-level generation.
Discover connected `transcribe` support as described in
[developer-mcp.md](../../../../developer-mcp.md).

## 3. Go shapes and tags

Inspect the generated row shape or link an existing authoritative application type.
The fragment below explains SQL mapping and internal presence; standard `transcribe`
derives component/body/output binding and its handler from the DQL graph.

```go
type Record struct {
    ID   int    `sqlx:"ID,primaryKey=true,autoincrement=true"`
    Name string `sqlx:"NAME"`

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

Attach `lifecycle_type(records, 'hooks.RecordLifecycle')` to the named outer view,
using a declared package import, for example
`#import('hooks', 'example.com/shop/hooks')`. The imported type must exist and
match the entity/parent signatures. For a new lifecycle in the generated
`#package`, explicitly name a local type, such as
`lifecycle_type(records, 'RecordLifecycle')`; pure Go transcription creates its
empty methods once. Foreign missing types and invalid methods fail without a
fallback. Every child role needs its own declaration if it needs hooks; auxiliary
views cannot declare mutation lifecycles. Omitting the declaration leaves normal
framework validation and writes hookless. No lifecycle names are inferred.
`lifecycle_type` requires a generated Go mutation writer; GET readers and
unsupported lowering modes reject it before file writes. Reader request Init
belongs to the declared input type, output Finalize to the declared output type,
and row OnFetch is separate. `entity_hooks` is unsupported DQL. Regeneration preserves authored/imported hooks
and validates their current signatures. Declare cohesive groups with
`invariant(records.START, 'Schedule')` and
`invariant(records.END, 'Schedule')`. Add the separate validation annotation
`tag(records.END, 'validate:"gtfield(Start)"')` in the outer projection.
Use exact resolved Go field names in cross-field validation rules.

The generator derives body/output bindings and authorized typed Previous reads.
Root lookup uses requested keys; child discovery can use captured parent scope
to include existing children whose IDs Input.Init resolves. Resolution uses only
rows already available in that authorized snapshot, and does not trigger an
unrestricted reload. Check the
preview for complete composite keys, authorized scope and no accidental
pagination/truncation. Do not author manual Body/Existing/Data or key-extraction
plumbing for standard `transcribe`. The generator must report unsupported metadata or
capabilities instead of silently broadening the authorized scope.

## 5. Required generated mutation lifecycle

The generator supplies this order. Use it to review hooks and observable behavior;
do not implement these phases as an authored DQL program:

1. Bind the component input and completed current reads. Capture immutable original presence/identity and a detached processing baseline **before** input `Init` and `InitMCP`; capture detached Previous evidence and prepare public read indexes.
2. Run input `Init`, then `InitMCP` when MCP context is present. `InitMCP` remains supported.
3. Prepare invocation-local dependencies and hook instances. Do not perform business initialization in dependency preparation.
4. Run recursive `SyncPresence` against the captured processing baseline; resolve initialized identity candidates against authorized Previous, check parent scope, and prepare typed frames. Freeze established key parts and the match/missing-row decision.
5. Run invariant-group backfill when needed, without marking hydrated fields supplied.
6. Run entity `Init` using marker-aware setters for business changes.
7. Run framework Go-tag and database-derived validation, then custom entity `Validate`.
8. Begin/join the managed transaction before sequencing.
9. Reserve established IDs with the scoped sequencer and allocate stable IDs for eligible unresolved INSERT candidates, then run `AfterSequence`.
10. Diff using the frozen resolved identity and database match/missing-row decision; produce the allowed actions without reclassifying sequenced rows.
11. Reconcile identities and populate declared parent/self foreign keys from the allocated IDs. Run final validation over the values that will be written, with no unresolved deferred constraints. Business values must remain frozen.
12. Prepare detached typed write payloads and queue them in deterministic traversal order.
13. Run observational `AfterQueue`, verify that queued working values/markers/topology did not change, and populate the output from the working body.
14. Let the existing invocation owner flush/complete its transactions. Invoke one outcome-aware finalization path.

### Sequencing strategy and native ownership

Use `#setting($_ = $sequence_strategy('transient'))` or
`#setting($_ = $sequence_strategy('reservation'))` only when selecting an explicit
policy. Omit it for the native default: original MySQL transient transaction,
PostgreSQL 10+ exact nextval values, or SQLite native reservation. The MySQL table
allocator is optional and must be provisioned before choosing `reservation`.
Never suggest `maxid` or a process-local counter for concurrent managed writers.
The parser rejects unsupported values; this setting survives generated tags and
is applied to the real invocation Data source.

The root database unit resolves and freezes the version-matched native default
before Data opens, and owns policy and transaction/capability identity. Children
inherit policy; explicitly naming that same default succeeds, while a differing
explicit setting fails without switching the open root. The original MySQL algorithm
is unchanged: it needs an allocator connection, may wait on caller-held locks,
and its transient source INSERTs execute defaults/triggers. Transactional rows
roll back; external or nontransactional effects need not. Do not silently switch
to an allocator table to hide these constraints. PostgreSQL batches must retain
their actual values, including cache/interleaving gaps and descending sequences.

The sequencer owns ID reservation and allocation. Generated reconciliation owns foreign-key population. IDs and links must be ready before Queue; neither allocation during INSERT/Flush nor relationship backfill after execution satisfies this lifecycle. Supplied-ID collision handling must use native sequence identity and reservation semantics. Final duplicate checks remain safeguards.

There is no automatic second `SyncPresence` between entity `Init` and `Validate`. Setters provide the presence signal for `Init` changes. Graph membership, ordering, parent edges, and self holders must not change after initial synchronization/frame preparation; reject such changes rather than silently ignoring added records. Construct the graph before capture or use an explicitly custom orchestration policy.

`AfterSequence` may perform only the identity/link work allowed by the compiled policy; it may not change validated business values or their markers. `AfterQueue` must observe rows, not alter identities, links, business values, markers, or graph structure.

## 6. Original identity and sparse presence

Original suppliedness comes from the immutable request capture. Database matching
uses a separate initialized candidate: nonzero scalars and non-nil pointers
(including pointers to zero) participate even without an original marker. An
absent scalar zero requires explicit presence, such as a generated setter.
`Input.Init` can resolve an omitted or replace a supplied identity, subject to
authorized Previous and parent scope. Entity Init runs after this match is frozen.

- Original marker true with identity zero is an explicitly supplied identity.
- Resolving identity before frame preparation can select UPDATE while `Original.Has("Id")` stays false. Once classified INSERT, later sequencing or link production cannot turn it into UPDATE.
- A supplied but unmatched identity does not guarantee UPDATE; apply the declared missing-row policy.
- A missing original marker is different from a known marker with all bits false. Do not silently invent suppliedness.
- Match only complete resolved tuples. A partial tuple may proceed as INSERT only under the compiled producer policy for every missing part; established parts are already frozen. Never match by a prefix.
- Multiple new unassigned entities remain separate inserts. Do not report them as duplicate zero/null identities merely because their initial values match.
- Validate duplicate assigned tuples even when a positional fast path appears to match them.
- An unresolved insert identity cannot be left for SQLX to backfill only into a detached write DTO while the returned working body remains stale. Require a completed declared producer or an explicit marker-aware assignment. This readiness check is not UPDATE classification.

Matching the request graph to its processing baseline is separate from matching database rows:

- Processing association may prefer stable source pointer identity. For collections, try the same position, verify identity, then use an identity-index fallback where association is provable.
- Reordered/replaced value-slice items with no supplied identity can be ambiguous. Do not match them by zero key, slot address, or coincidental business equality. Return an actionable ambiguity error when association cannot be proven.
- Database `Previous` matching is identity-only. Sparse request fields are not required to equal the database row.
- No arbitrary framework depth cap should truncate a valid graph. Retain real cycle and ambiguous-parent detection.

Owned generated entities expose typed getters/setters and `SyncPresence(snapshot handler.EntitySnapshot[T]) error`. Getters do not mark fields. Setters assign the exact field type and mark presence. For an imported/linked entity, call `snapshot.SyncPresence(entity)` instead of trying to add a method to a foreign Go type. A nil current entity is a no-op; a nonnil entity without a usable baseline is an error. Failed recursive synchronization must leave all working markers unchanged.

## Typed read indexes for application hooks

Generated inputs expose `PrepareReadIndexes(ctx)` and `ReadIndexes(ctx)`. Capture
prepares and caches detached read collections before Input.Init; entity hooks can
obtain the same set through their bound input. Default eager maps cover canonical
read identity (declared primary keys for independent auxiliary reads) and complete
parent/child or self-link equality tuples. Business names, dates or `Id` suffixes
do not select automatic groups.

```go
reads, err := input.ReadIndexes(ctx)
if err != nil {
    return err
}
exists := reads.CurrentItemsById.Has(itemID)
children := reads.CurrentItemsGroupedByOrderId[orderID]
byName := reads.CurrentItems.GroupByName()
```

Names follow actual read slots and Go fields. `GroupByName()` is built on demand;
`IndexByName()` also runs on demand and returns an error for duplicate keys.
Comparable composite keys have generated struct types; maps provide `Has`.
Groups retain all rows with valid keys, including zero; null parts are excluded.
Noncomparable fields retain row data without invalid map helpers. There is no
global business-group cache.

Every declared read field needs loaded-field evidence, even without an eager map.
Failed preparation clears the cache. Public helpers are detached from canonical
Previous; changing their rows/maps cannot authorize a write or change its match.
Owned inputs keep support in their package even with split destinations. Foreign
inputs retain their owner; a typed free builder and optional definition-level
`ResolveIdentity` callback supply the adapter before Input.Init. Linked inputs
must already declare their read slots. The default support file is `indexes.go`;
`$file_prefix` and `$support_dest('indexes','lookup.go')` follow normal precedence.

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
    state handler.LifecycleContext[Record, handler.NoParent, Output],
) error {
    return nil
}

func (h *RecordHooks) Validate(
    ctx context.Context,
    current *Record,
    state handler.LifecycleContext[Record, handler.NoParent, Output],
) error {
    return nil
}
```

The empty bodies above show the verified signature, not complete application validation. Implement the requested business behavior.

`LifecycleContext[T, P, O]` embeds `EntityState[T, P]` and supplies:

- `Previous *T`: detached database projection, treated as read-only.
- `PreviousFields`: actual loaded-field evidence, using canonical Go names. An unselected field is not a known zero.
- `Original`: immutable original suppliedness, also using canonical Go names.
- `Parent *P`: the enclosing declared relation parent; root uses `handler.NoParent` with nil parent.
- `SelfParent *T`: the immediate self-edge parent, nil at a self-tree root. Self descendants retain their enclosing `Parent`.
- `Output *O`: the shared invocation-owned component response; it remains available when a hook returns a validation error.

Instantiate and bind one hook object per view/role per invocation. Reuse it across `Init`, `Validate`, optional `AfterSequence`, optional `AfterQueue`, and supported root finalization. Do not share mutable request hook state globally. Application input/configuration/logger/message-bus dependencies belong on the hook object through normal scoped binding; entity types need not acquire an `any` parent or a large session facade.

Use exact generic signatures and canonical package identities. A different parent's hook type, an unknown type, or a same-short-name type from another package must not silently match.

### Child lifecycle with a typed parent

For the declared `Order → Items` relation, `lifecycle_type(items, 'ItemLifecycle')`
selects a lifecycle whose state is `LifecycleContext[Item, Order, Output]`. Replace its generated
`Validate` body with application logic such as the following (imports: `context`,
`fmt`, `time`, and `xhandler "github.com/viant/xdatly/handler"`):

```go
func (hooks *ItemLifecycle) Validate(
    ctx context.Context,
    item *Item,
    state xhandler.LifecycleContext[Item, Order, Output],
) error {
    order := state.Parent
    if order == nil {
        return fmt.Errorf("item requires an order")
    }
    if order.WindowEnd != nil && order.WindowEnd.Before(time.Now()) {
        return fmt.Errorf("cannot modify items after the delivery window closes")
    }
    return nil
}
```

This example uses the parent Order's declared `WindowEnd` field. `Parent` is the
current typed parent; `Previous` is the matched previous **item**, not the previous
order. The same parent-bearing state is available in `Init`, `AfterSequence`, and
`AfterQueue`. A recursive relation also exposes its immediate `SelfParent`.
Keep validation read-only; use marker-aware setters in initialization when values
need to change. Use `AfterSequence` for logic that requires allocated identifiers.

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

- Preserve the frozen resolved/matched identity in working/output values; reject later changes to established key parts before propagating links.
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

For scoped message-bus hook examples and the original async job/dry-run boundary, see [mutation-messages.md](../../../../mutation-messages.md).

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
| High-level graph + operation `transcribe` to pure Go | Available as `datly transcribe <operation>` (`get`, `patch`, `post`, `put`) in the v1 source CLI. Go is the default output; inspect the connected MCP target metadata for server operation support. |
| Original snapshots, recursive sync, typed hooks, invariant helpers, typed Previous evidence, sequencing/diff/reconcile/queue, output/finalization | Required generated behavior. Verify generated Go fixtures and hook preservation on the connected build; parser or metadata acceptance alone does not establish it. |
| Framework Go + DB validation, schema-to-validate tags, and DQL validation customization execution | Current framework Go/database validation and generated NOT NULL/customization paths have native/generated SQLite tests. Complete UNIQUE/reference discovery is not established; authored native UNIQUE tags remain explicit. Relation-produced FK deferral is restricted to captured parent INSERTs and final validation runs before Queue. Never substitute custom-hook-only validation or infer constraint absence from missing metadata. |
| Native `OnInsert` / `OnUpdate` callbacks and native default generators inside the generic post-validation write path | Explicitly rejected by generic policy until moved to a safe earlier phase. Ordinary custom handlers retain native behavior. |
| Generic primary-key-changing updates | Unsupported; existing identities are restored. Use explicitly custom orchestration for a different policy. |
| Ambiguous value associations or conflicting parent/self-holder contexts | Explicit errors, not guessed matches or arbitrary truncation. |

Use the public interfaces and examples in [tags-and-interfaces.md](../../../../tags-and-interfaces.md) and [writer-examples.md](../../../../writer-examples.md). Application authoring does not require framework source access.

## Explicit deletion and token validation

Generated PATCH and PUT writers accept two optional outer annotations. Names
belong to the application; neither annotation creates a field implicitly.

```sql
#package('example.com/shop/orders/write')
#setting($_ = $input_type('OrdersInput'))
#setting($_ = $output_type('OrdersOutput'))
#setting($_ = $case_format('lc'))
#define($_ = $Data<[]*Order>(output/body))
#setting($_ = $route('/orders', 'PATCH'))
#setting($_ = $connector('main'))
SELECT orders.*, items.*,
       type(orders, 'Order'), type(items, 'Item'),
       lifecycle_type(items, 'ChildLifecycle'),
       concurrency_token(orders.VERSION),
       CAST(items.should_delete AS bool),
       delete_marker(items.should_delete)
FROM (SELECT o.* FROM ORDERS o) orders
LEFT JOIN (SELECT i.*, '' AS should_delete FROM ITEMS i) items
    ON items.ORDER_ID = orders.ID
```

Generate from the source package in an existing project:

```sh
datly transcribe patch -dir "$PROJECT" \
  -schema -connector main -driver sqlite3 -dsn "$PROJECT/orders.db" \
  example.com/shop/orders/source
```

Keep the existing authorization filters in the inner SQL. Generated Previous
reads preserve them. `delete_marker` designates a logical boolean pseudo field;
outer CAST supplies its Go type when the inner expression has another type.
It stays in the request and Original snapshot, and is excluded from physical
INSERT/UPDATE mappings and generated Previous projections. An application-owned
linked row must declare its logical field with `sqlx:"-"` and a Has marker.

For example, with the names above:

```json
{"data":[{"id":1,"version":0,"items":[
  {"id":11,"shouldDelete":true},
  {"id":12,"name":"updated"},
  {"name":"new"}
]}]}
```

Only the supplied row 11 is a deletion request. Every identity part must be
supplied, and the complete tuple must match an authorized Previous row under
that captured parent. Missing, unknown, out-of-scope, or incomplete composite
identities fail before sequencing; initialization cannot supply missing delete
identity parts. Zero identity parts retain their normal Has-based meaning.
An omitted row, omitted collection, empty collection, or false flag never
requests deletion. Other supplied rows retain the normal insert/update policy.

The child lifecycle receives `LifecycleContext[Item, Order, Output]`: `state.Parent` is typed,
`state.Previous` is the matched child, and `item.ShouldDelete` is available to
business validation. Delete payloads may contain only identity and flag; framework
INSERT/UPDATE required/reference checks do not require business fields on them.
`Validate` remains read-only. Deleting a parent does not generate child actions.
Explicitly marked descendants queue before their marked ancestors; a supplied
unflagged child below a marked parent fails. Database constraints still apply.
All actions use the invocation's buffered DML and existing transaction owner;
a supplied transaction remains caller-owned.

`concurrency_token` is **validation-only**. For updates, its check runs first in
the validation phase, before framework constraints and application Validate
callbacks. It compares the captured, client-supplied expected token with the
loaded authorized Previous token. Missing Has presence, a null token, missing
Previous evidence, or a mismatch returns `*handler.Conflict` (status 409) before
sequencing or queuing mutations. Inserts and explicitly marked deletes do not
perform the update-token check.

Numeric tokens compare in their canonical Go numeric type. `time.Time` tokens
compare instants with `Time.Equal`, including equal instants expressed in
different time zones. Presence is independent of the value: a supplied numeric
zero is an expected token; an omitted numeric zero is missing.

The expected token is captured before input/entity initialization. Init may
explicitly prepare the next working token using a setter; that does not change
the captured expected value. Token advancement is an application or database
concern. The framework never increments or rewrites a version automatically.

There is a race window between loading Previous, validating it, and executing
DML. Another writer can change the row during that window. This annotation adds
no token predicate to UPDATE/DELETE WHERE clauses, no vendor locks, and no
row-count conflict machinery. It does **not** provide atomic race prevention.

Regenerate through the same high-level command and preserve create-once lifecycle
edits. Verify mixed mutations, identity-only deletes, omissions/false flags,
parent/composite scope, token presence and instant equality, validation order,
rollback/caller-owned transactions, and regeneration in the generated package.
