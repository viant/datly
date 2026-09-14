# Typed readers, relations and projections

[All guides](README.md) · [Cubes and reports](reports.md)

A reader maps a declared query graph into your public result shape. Use it for
lists, nested entities, summaries and controlled analytical endpoints. SQLX owns
row-to-struct binding; Datly coordinates view policy, batches, relations and
output slots.

## Define input and output

The [quickstart](quickstart.md) shows a linked reader with a required path input
and SQL resource. An application can instead declare SQL on an output field:

```go
type Input struct {
    TenantID int `parameter:"TenantID,kind=query,in=tenantId,required=true"`
    Offset int `parameter:"Offset,kind=query,in=offset" querySelector:"Records"`
}
type Record struct {
    ID int `json:"id" sqlx:"id"`
    Name string `json:"name" sqlx:"name"`
}
type Output struct {
    Data []*Record `json:"data" parameter:"Data,kind=output,in=view" view:"Records,limit=1,selectorOffset=true" sql:"SELECT id,name FROM records WHERE tenant_id=:TenantID ORDER BY id"`
}
```

This fragment needs an `xdatly.Component[Input, Output]` declaration with root
view `Records`, a configured connector and a custom project build. The selector targets
the view identity `Records`, not the Go holder `Data` or an SQL alias.
Offset requires a positive limit. Enforce tenant access separately.

## Relate typed datasets

Describe parent/child holders and complete join keys in the view contract.
A shared ID is often insufficient: `(tenant_id, id)` must match as a tuple,
not as independent tenant-IN and ID-IN filters. Both same-ID/different-tenant
fixtures and NULL/zero-key controls are useful acceptance cases.

Ordinary relations populate nested typed collections or objects. SelfReference
connects recursive entities; it does not produce an aggregate output. DerivedView
is an ordinary relation of kind `derived` whose query derives from another view.
See the [reader contract](../../llm/datly-reader/references/reader-contract.md)
for authoring choices, matching modes and imported shapes.

`batch` controls parent-key batches. `batchConcurrency`, `relationalConcurrency`
and partitioner concurrency control distinct work. Composite keys and unrelated
predicates consume the database's same placeholder budget; a large configured
batch cannot override it. Modes `read_all`, `read_matched` and `read_derived`
select supported matching strategies rather than changing relation identity.

## Return counts independently from a page

Add typed output slots, not an extra metadata field on the root view:

```go
type Totals struct { Count int `json:"count" sqlx:"count"` }
type Bounds struct {
    Minimum *int `json:"minimum" sqlx:"minimum"`
    Maximum *int `json:"maximum" sqlx:"maximum"`
}
// Add these fields to Output:
// Totals *Totals `json:"totals" parameter:"Totals,kind=output,in=derived" view:"Totals" sql:"SELECT COUNT(*) AS count FROM ($View.Records.NonWindowSQL) parent"`
// Bounds *Bounds `json:"bounds" parameter:"Bounds,kind=output,in=derived" view:"Bounds,allowNulls=true" sql:"SELECT MIN(id) AS minimum,MAX(id) AS maximum FROM ($View.Records.NonWindowSQL) parent"`
```

For three matching IDs with page size one, offset ten returns no page rows while
the count remains three and bounds remain 1–3. With no matching rows, count is
zero and nullable bounds are NULL. The [complete Go and DQL examples](../../llm/datly-reader/references/reader-examples.md#complete-go-shape-pattern-one-row-pages-full-match-count-and-bounds)
show both independent holders and their SQLite expectations.

`NonWindowSQL` removes selector/view pagination, including `set_limit`; it keeps
literal LIMIT/OFFSET authored inside the SQL source. This preserves the meaning
of a business-limited subquery. Multiple holders stay independent; dotted output
paths distinguish `Left.Total` from `Right.Total`. Current top-level derived
execution expects a struct/pointer result; arbitrary cardinalities are not
established by these examples.

## Projection and loaded-field evidence

Enable only the selectors the API needs: projection, ordering, criteria, limit,
offset or page. Configure filterable/orderable columns and permitted SQL methods.
A physical column's existence is not an authorization policy.

A selected-away field differs from a loaded SQL NULL. Hooks/codecs must not infer
that every Go zero was read. Public read-metadata contracts report loaded fields
by bound view, row ordinal and relation holder; independent outputs can be selected
through `ReadOutputProjection.Output("Left.Total")`. Unknown evidence means
unknown, including some reducer/replacement outputs; it is not “all fields.”
Read evidence is also not an immutable pre-hook copy of business values.

## Hooks, partitions and codecs

Row hooks use the SDK signatures:

```go
func (r *Record) OnFetch(ctx context.Context) error { return nil }
func (r *Record) OnRelation(ctx context.Context) {}
```

Import `context`. `OnFetch` runs after decoding, before indexing/relating.
`OnRelation` runs after selected children have been assembled across batches;
it has no error return. Cache replay must preserve application hook behavior.
Use these for derived display values or completed-relation work, not hidden writes.

A public `reader.Partitioner` returns typed partition descriptions with table or
expression and bound arguments. A `ReducerProvider` supplies a typed result
reducer. Concurrent arrival establishes no business order; sort when the reducer
requires one. Related reduction sees the completed batches. Do not replace the
normal SQLX mapping path with raw row maps.

Built-in SQL encodings such as JSON/CSV and custom codecs are distinct. For a
custom codec, supply its implementation and output type, and test NULL, errors,
selected-away fields and cached reads. Invocation state must not live in a shared
codec or hook service accidentally.

## Verification and tuning

Start with realistic SQLite cases for no rows, NULLs, duplicate IDs across tenants,
multiple batches, derived outputs and hook counts. Add the actual HTTP/MCP path
for public behavior. Native cache and partition tests prove their own slices,
not a universal combination of every feature. Read [warmup limits](cache-and-warmup.md)
before relying on full-projection cache reuse, and [observability](observability.md)
before making performance claims. The native reader uses a three-attempt retry policy for errors containing
`invalid connection`, resolving the same connector again and preserving native
cancellation and retry-safety checks. There is no public retry tag or configurable
attempt count.

See [project init/build](project-build.md) and [view selectors and formats](selectors-and-formats.md) for automatic discovery, exact-name policy and download contracts.
