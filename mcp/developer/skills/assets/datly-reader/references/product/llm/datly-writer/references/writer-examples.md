# Writer authoring examples

Select mutation behavior explicitly; the HTTP verb alone is not the generated policy.

For migrations, first apply [complete writer reverse engineering](reverse-engineering.md).
The introductory example below teaches syntax; the multi-relation example shows
body and table-role reconstruction. Neither substitutes for source-specific acceptance.

## PATCH graph and application hooks

```sql
#package('example.com/app/records/write')
#setting($_ = $input_type('RecordsInput'))
#setting($_ = $output_type('RecordsOutput'))
#setting($_ = $case_format('lc'))
#define($_ = $Data<[]*Record>(output/body))
#setting($_ = $route('/v1/records', 'PATCH'))
#setting($_ = $connector('main'))
SELECT records.*, children.*, lookup.*,
       type(records, 'Record'), type(children, 'Child'), type(lookup, 'Lookup'),
       lifecycle_type(records, 'RecordLifecycle'),
       invariant(records.START, 'Schedule'),
       invariant(records.END, 'Schedule'),
       tag(records.END, 'validate:"gtfield(Start)"')
FROM (SELECT r.* FROM records r) records
JOIN (SELECT c.* FROM record_children c) children ON children.record_id = records.id
JOIN (SELECT l.* FROM (lookup_values) l) lookup ON lookup.id = records.lookup_id AND 1=1
```

Select `transcribe` operation `patch` with pure Go output; for a build advertising the
reviewed interface:

```sh
datly transcribe patch -dir "$PROJECT" \
  -schema -connector main -driver sqlite3 -dsn "$PROJECT/schema.db" \
  example.com/app/source/write
```

Save this DQL as `source/write/Records.dql` in the existing `example.com/app`
module at `$PROJECT`. The required `#package` selects `records/write`; the source
package and `-dir` have different roles. Input/output settings name contracts,
while outer `type` annotations name row shapes. The explicitly typed
`$Data<[]*Record>(output/body)` binds the main output holder: `Data` is the Go
field, `body` is the generated writer output binding. Global `case_format('lc')`
uses Structology lower-camel casing for output; use it instead of per-column JSON
tags or holder `WithTag` annotations for routine casing. Default artifacts include
`views.go`, `input.go`, `output.go`, `router.go`, `lifecycle.go` and mutation support.
Only the explicitly named root lifecycle is scaffolded; children remain hookless.
To use an existing foreign type, declare its package with
`#import('hooks', 'example.com/app/recordhooks')` and name
`lifecycle_type(records, 'hooks.RecordLifecycle')`. Author that type in its
package; generation preserves it and does not create a foreign scaffold.
Edit the create-once local lifecycle file; see [filename overrides](developer-mcp.md#operation-based-generation-to-pure-go)
for optional prefixes and exact destinations.

High-level generation is available in the `v1` source CLI. Discover connected
server support as described in [developer-mcp.md](developer-mcp.md); report a
missing capability without substituting translation.

The generator owns Body/Existing/Data binding, original identity tuple extraction,
authorized complete Previous reads, Has/SyncPresence, sparse validation, sequencing,
parent links, writes and completion. Authors declare the graph and Go hook metadata.
`(lookup_values)` is auxiliary read-only data; `AND 1=1` makes this relation a
single holder, while the unmarked children relation stays many. Ordinary joins declare writable
relations; composite links must include every key part. Schema discovery supplies
actual identities, constraints and date types. Bind authorization explicitly using
the [JWT input pattern](tags-and-interfaces.md#jwt-input-and-authorization-predicates).
Current reads may discover children through authorized parent scope so input
initialization can resolve omitted child IDs. Do not add manual key-extraction
or pagination plumbing. Inspect the scope and generated read evidence.

Use `post` for insertion, `put` for the declared update policy, and `get` for a
reader; align each DQL route method with the chosen operation. Keep independently
exposed reader and writer components in distinct packages/routes as appropriate.

## Generated or linked Go shape and marker

~~~~go
type Record struct {
    ID   *int64 `sqlx:"id,primaryKey"`
    Name string `sqlx:"name" validate:"required"`
    Has  *RecordHas `json:"-" sqlx:"-" setMarker:"true"`
}
type RecordHas struct { ID, Name bool }
~~~~

Clients send business data, not Has. Omitted Name is skipped in sparse required checks and is not cleared. Supplied empty Name fails if the application's required rule forbids empty text. Do not equate every DB NOT NULL column with a nonzero business requirement.

## Cohesive date invariants

~~~~go
type Window struct {
    Start *time.Time `invariant:"Schedule"`
    End   *time.Time `invariant:"Schedule" validate:"gtfield(Start)"`
    Has   *WindowHas `json:"-" sqlx:"-" setMarker:"true"`
}
type WindowHas struct { Start, End bool }
~~~~

Import `time` for the linked Go field fragment. The DQL tags above generate the
same group metadata. For an existing row with only End supplied, the generator backfills Start from loaded Previous without setting its marker, then validates the effective Start/End dates, including an end-before-start failure. New rows receive complete checks. Unknown/unselected previous values are not known zeros; failed backfill must not partially mutate the group.

## Hook responsibilities

~~~~go
func (h *RecordHooks) Init(ctx context.Context, row *model.Record,
    state handler.LifecycleContext[model.Record, handler.NoParent, Output]) error {
    // Application defaults/normalization, using marker-aware setters.
    return nil
}
func (h *RecordHooks) Validate(ctx context.Context, row *model.Record,
    state handler.LifecycleContext[model.Record, handler.NoParent, Output]) error {
    // Custom checks, only after framework/schema/database validation passed.
    return nil
}
~~~~

Comments identify application-specific rules, not a validation bypass. Reuse the same injected hook object. Do not write DB rows from validation hooks.

AfterSequence observes allocated IDs before Diff. Reconcile preserves frozen keys and fills declared parent/self links. AfterQueue must not alter queued values. Outcome-aware completion runs once; publish only after CommitConfirmed().

## Controlled error response

~~~~go
return &response.Error{
    Code: 401,
    Payload: map[string]any{
        "message": "Access denied",
        "error": map[string]any{"reason": "policy"},
        "violations": violations,
    },
    Cause: internalCause,
}
~~~~

Use 401 for the application's chosen authorization response; other validation may select another status. Preserve the public body through wrapping and keep the private cause out. Return an error when mutation must stop/roll back.

## Fixtures

Cover existing/new/supplied-zero identities, partial composite keys, same ID across tenants, mixed rows, reordered children, omitted/empty/null/false values, DB unique/reference failures, custom failures, cancellation, repeated sequence allocation, rollback, pending caller transactions and regeneration.

## Explicit child deletion and expected tokens

Use the complete DQL, JSON and high-level `datly transcribe patch` example in
[the mutation marker contract](writer-contract.md#explicit-deletion-and-token-validation).
Choose entity, lifecycle, token and flag names in DQL. Keep logical flags in the
inner projection and annotate their outer view column; never replace generation
with hand-written Velty loops, manual Current reads, or missing-child deletion.


## Multiple writable collections and auxiliary lookup data

This complete DQL authoring example extends the maintained
[writable graph/body contract](../../../datly/doc/mutations.md). It assumes actual
ORDERS, ITEMS, ORDER_NOTES, ORDER_KINDS and PRODUCTS schema authority; substitute
the original application's names, keys, filters and policies rather than adding
these tables to a migration. Discover all column types, defaults and FK metadata
from that schema. The example is an authoring pattern, not Platform acceptance.

| View | Role | Generated body/Current and mutation behavior |
|---|---|---|
| orders | Writable root | Order records with independent Items and Notes collections |
| items | Writable child | Full item identity; TENANT_ID and ORDER_ID link to parent |
| notes | Writable child | Separate collection; same complete parent link |
| kind | Read-only auxiliary | To-one kind lookup; excluded from sequencing and DML |
| product | Read-only auxiliary | Item lookup by tenant and product; excluded from DML |

```sql
#package('example.com/shop/orders/write')
#import('jwt','github.com/viant/scy/auth/jwt')
#setting($_ = $input_type('OrdersInput'))
#setting($_ = $output_type('OrdersOutput'))
#setting($_ = $case_format('lc'))
#setting($_ = $route('/orders','PATCH'))
#setting($_ = $connector('main'))
#define($_ = $Jwt<string,*jwt.Claims>(header/Authorization).Required().WithCodec(JwtClaim).WithStatusCode(401))
#define($_ = $TenantID<int>(param/Jwt.AccountId).WithPredicate(0,'equal','o','TENANT_ID'))
#define($_ = $Data<[]*Order>(output/body))
SELECT orders.*, items.*, notes.*, kind.*, product.*,
       type(orders,'Order'), type(items,'Item'), type(notes,'Note'),
       type(kind,'Kind'), type(product,'Product'),
       lifecycle_type(orders,'OrderLifecycle'),
       lifecycle_type(items,'ItemLifecycle'),
       lifecycle_type(notes,'NoteLifecycle'),
       invariant(orders.WINDOW_START,'DeliveryWindow'),
       invariant(orders.WINDOW_END,'DeliveryWindow')
FROM (${embed:sql/orders.sql}) orders
LEFT JOIN (${embed:sql/items.sql}) items
  ON items.ORDER_ID=orders.ID AND items.TENANT_ID=orders.TENANT_ID
LEFT JOIN (${embed:sql/notes.sql}) notes
  ON notes.ORDER_ID=orders.ID AND notes.TENANT_ID=orders.TENANT_ID
LEFT JOIN (${embed:sql/kind.sql}) kind
  ON kind.ID=orders.KIND_ID AND 1=1
LEFT JOIN (${embed:sql/product.sql}) product
  ON product.ID=items.PRODUCT_ID AND product.TENANT_ID=items.TENANT_ID AND 1=1
```

Adjacent SQL assets:

```sql
-- sql/orders.sql
SELECT o.* FROM ORDERS o
${predicate.Builder().CombineOr($predicate.FilterGroup(0,"AND")).Build("WHERE")}
```

```sql
-- sql/items.sql
SELECT i.* FROM ITEMS i
```

```sql
-- sql/notes.sql
SELECT n.* FROM ORDER_NOTES n
```

```sql
-- sql/kind.sql
SELECT k.* FROM (ORDER_KINDS) k
```

```sql
-- sql/product.sql
SELECT p.* FROM (PRODUCTS) p
```

Use the actual claim/property and original authorization policy, not the example's
TenantID blindly. Filtering Previous does not authorize a new body record:
OrderLifecycle must verify the source policy and assign trusted ownership with
generated setters. Child hooks validate source-specific rules using typed parent,
Previous and auxiliary indexes. They never bind input, issue reads, allocate IDs
or write SQL. Add explicit delete markers only when the original operation has
that deletion behavior; omission of Notes or Items must not delete stored rows.

Transcribe with the source database dialect. For a MySQL-original migration,
use `transcribe patch -schema -connector main -driver mysql` with a private,
prepared authoring connector; use generated shapes unchanged in the hydrated
SQLite runtime tier. MySQL identity allocation stays original transient by native
default where that is the source policy. Do not enable reservation/scoped
sequences or add allocator tables to compensate for a generation problem.

Inspect generated body, field types, presence markers, Current reads, relation
holders and FK tags. Prove two independent child collections, new parent/child
IDs, every composite link, cross-tenant rejection, sparse update, explicit deletion
when declared, auxiliary no-DML, complete physical rollback and generation
stability. Add original event/side-effect requirements to the graph and acceptance
matrix separately; this example does not claim append-only event semantics.
