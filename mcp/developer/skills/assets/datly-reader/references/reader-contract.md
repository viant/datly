# Authoring Datly readers

For current enablement and pending integration boundaries, consult [cache-and-operations.md](cache-and-operations.md) when using operational or extension features below. Required behavior is not a claim of connected-build availability.

A reader turns bound application inputs into typed views and an output contract. Author the domain types, query, relationships, and allowed client operations; add Go behavior where the application needs it. Use [project build](project-build.md) for initial setup. You do not need to understand Datly's compiler or collectors to author a reader.

This guide includes required capabilities still under development. Keep the requested application contract intact; when a checkout cannot execute it, identify the blocker rather than silently weakening the design.

## Choose Go shapes, DQL, or both

Go shapes make application types, methods, and reusable domain objects explicit. DQL combines SQL with input, route, view, and output declarations. Both describe the same component model; avoid maintaining two competing implementations.

Keep the Go field (`TenantID`), SQL column (`tenant_id`), view namespace (`orders`), and public route/tool name distinct. A Go import alias is a local spelling of an existing package type. Reuse that type rather than generating a look-alike struct.

These are field/tag fragments for the appropriate input, output, or row type:

```go
ID *int `parameter:"ID,kind=path,in=id,uri=/{id},required=true" mcpEnabled:"true" pathMcpEnabled:"true"`
Rows []*Row `view:"rows,table=records,connector=main,batch=100,batchConcurrency=2" sql:"SELECT id,name FROM records"`
ID int64 `sqlx:"id,primaryKey=true"`
Name string `sqlx:"name"`
```

Use `sql:"uri=queries/records.sql"` for a resource-backed query. This differs from the path parameter's `uri=/{id}`, which activates an alternative route. Resource paths belong to the component's resource context, not the process working directory.

Use explicit scalar types, pointers where NULL is meaningful, named domain types for rich values, and typed slices for collections. Presence is separate from truthiness: omitted, NULL, zero, false, and an empty collection can mean different things. Preserve those distinctions when transforming data.

## Views and related data

A view names a typed result and its query/source. Common `view` options:

| Concern | Options |
| --- | --- |
| Source and type | `table`, `connector`, `uri`, `type`, `dest` |
| Defaults | `orderBy`, `limit`, `offset`, `allowNulls`, `groupable` |
| Related reads | `batch`, `batchConcurrency`, `match`, `publishParent`, `relationalConcurrency` |
| Partitioning | `partitioner`, `concurrency` |
| Cache | `cache`, `cacheWarmup` |

Declare relationships using the complete parent/child identity. For example:

```go
`on:"TenantKey:p.tenant_id(true)=TenantID:c.tenant_id,account_key=account_key"`
```

The left side is the parent, the right side the child; the optional parent inclusion flag is part of the link. Do not reduce a tenant+ID relation to ID alone. Test two tenants with the same ID.

Recursive hierarchies use a self holder:

```go
Children []*Node `self:"child=ID,parent=ParentID" sqlx:"-"`
```

Use the row's actual identity/parent fields. Duplicate identities or conflicting associations require explicit handling, not guessed attachment. A self-tree parent and an enclosing relation parent are distinct. Required nesting should not be replaced with a small arbitrary depth cap.

### DerivedView and aggregates

A DerivedView is a query-derived view, such as counts, bounds, or totals. For a paginated list, decide whether a count means the page or all matching records; query the intended scope deliberately.

Attach a top-level derived output with `parameter:"Totals,kind=output,in=derived"` plus its `view`/`sql` tags, or declare `$Totals<Totals>(output/derived)` in DQL. Its output holder (`Totals`) is separate from the root view identity (`Records`) used in `$View.Records.NonWindowSQL`. Query selectors also target the root view identity, not the collection's JSON name. See the [complete paginated-count example](reader-examples.md#complete-go-shape-pattern-one-row-pages-full-match-count-and-bounds) for exact Go and DQL declarations, two derived slots, NULL handling, and verified empty-page behavior.

Multiple derived outputs are independent. An output can contain paginated `Rows`, `Left.Total`, and `Right.Total`; a total may still be useful when `Rows` is empty. Full holder paths distinguish the two totals. Avoid definitions where an aggregate overwrites the root collection or another slot. Nested independent output slots support pointer containers; do not assume every dotted ordinary row-relation holder has the same support.

## Batching, concurrency, and partitioning

Tune these separately:

- `batch`: parent keys in one related query.
- `batchConcurrency`: concurrent batches.
- `relationalConcurrency`: concurrent related reads.
- Partitioner `concurrency`: concurrent partitions.

Database bind-variable limits still apply; composite keys and other arguments consume the same budget. A large requested batch does not override it. `match` accepts `read_all`, `read_matched`, or `read_derived`; choose the intended strategy, not whichever hides an invalid query.

Application-defined partitioning uses public `github.com/viant/xdatly/reader.Partitioner`:

```go
Partitions(ctx context.Context, request reader.PartitionRequest) ([]reader.Partition, error)
```

A partition supplies its table/expression and bound arguments. A `ReducerProvider` can return a `reader.Reducer` implementing `Reduce(ctx context.Context, rows any) (any, error)`. The value is the complete typed result, not untyped row maps. Cross-partition/batch order is unspecified; order-sensitive reducers must sort. Related-view reduction runs after all batches complete.

## Add behavior with public hooks

Methods on your row type provide read behavior:

```go
func (r *Row) OnFetch(ctx context.Context) error {
    // Construct application values from loaded fields.
    return nil
}

func (r *Row) OnRelation(ctx context.Context) {
    // All selected child relations are assembled.
}
```

`OnFetch` runs after decoding and before indexing/relating. `OnRelation` sees the completed child collection, including every batch. Its current public signature has no error return. Cache hits must still execute domain behavior.

Keep hook mutation intentional. Replacing/reordering collections changes identity and may make loaded-field evidence unavailable. If selected view hooks need coordination, use public `handler.DataSync` rather than unrelated global locks.

Input `Init(ctx context.Context) error` prepares application input before execution; it is not a row fetch hook. Preserve MCP-specific input initialization too. Use the writer skill for mutation-oriented entity Init/Validate rather than assuming reader hooks persist data.

## Client selectors and authored predicates

Expose only needed operations. Selector options include `selectorProjection`, `selectorOrderBy`, `selectorCriteria`, `selectorLimit`, `selectorOffset`, `selectorPage`, `selectorNamespace`, `selectorFilterable`, `selectorOrderable`, `selectorSQLMethods`, `selectorDefaultOrder`, `selectorDefaultLimit`, `selectorNoLimit`, and `selectorOrderByColumns`.

A database column's existence does not make it a safe public filter. Tenant restrictions and hidden business filters belong in authored predicates, even if their physical fields are not public.

For multiview controls, use one explicit view argument. Current Go tag parsing
accepts `querySelector:"inventory"` or `querySelector:"view=inventory"`. The
candidate DQL uses:

Declaration fragment; adapt to the [complete reader contract](reader-examples.md#parameterized-dql-reader)
with the target view identity and existing input/output bindings.

```sql
#define($_ = $Fields<[]string>(query/_fields).Optional().QuerySelector('inventory'))
#define($_ = $Limit<int>(query/product_limit).Optional().QuerySelector('products'))
```

The selector property is inferred from the logical input kind/name. Do not write
a two-argument selector form or guess a child view from a JSON holder. Source
column whitelists remain the selector/builder owner's policy: expose only
allowed public fields, keep join keys available to the reader when needed, and
hide selected-away internal keys from payloads. The parent source-membership
guard is separate; do not duplicate or claim it until its review is merged.

Declare typed predicates and bind values as SQL arguments. Composite membership must match complete typed tuples. Independent tenant-IN and ID-IN clauses admit combinations the caller never requested. Custom predicate handlers should be request-specific and use normal dependency binding.

## Cache, codecs, and loaded fields

`cache` selects a configured named cache; the tag alone does not create a service. Warmup preloads selected cases/fields under the same view/query contract. Choose expiry, limits, and refresh deliberately. Do not assume writes automatically invalidate all related caches.

Custom `codec` conversion differs from built-in SQL encodings such as `enc=JSON` or `enc=CSV`. Supply the implementation and output type. Verify NULL, conversion errors, selected-away fields, and cached replay. A selected-away field is not a NULL value to decode. Respect the codec's lifetime/thread-safety contract; put request state in invocation-local behavior.

When application logic must distinguish actual loaded fields, public read-metadata contracts address the bound view result by input field, row ordinal, and relation holder. `ReadProjection.Fields` describes loaded fields; optional `ReadOutputProjection.Output("Left.Total")` selects an independent output. Unknown evidence is not “all fields loaded.” Arbitrary replacement/reducer outputs may have unknown evidence. Loaded-field evidence is not an immutable pre-hook copy of values.

## Rich imported shapes and internal backing fields

Required target declarations include:

Syntax fragment; adapt within the
[complete reader contract](reader-examples.md#parameterized-dql-reader).

```sql
CAST(view.pseudo AS importAlias.ShapeName),
tag(view.column, 'validate:...')
```

`ShapeName` is an existing imported Go domain type. `validate:...` represents the authored validation tag payload—not a literal validator rule. Use supported validation rules and report malformed declarations explicitly.

For a hook-built rich value, distinguish:

- **Logical field:** the application/client-facing domain value, not itself a DML column.
- **Physical fields:** SQL-backed values used to construct/persist it; possibly internal to the response, but still SQL-mapped.

`internal:"true"` is not `sqlx:"-"`: internal visibility must not delete reader/writer mappings. `SELECT * EXCEPT physical_column` expresses internal backing-column metadata in this authoring model.

`OnFetch` constructs the rich value. Initialization maps supplied nested input back to physical fields and updates their presence. Omitted nested properties must not overwrite stored values just because their Go defaults are zero; explicit zero/false must preserve write intent.

Not every rich CAST is logical: physical JSON/custom typed columns retain mapping and codec semantics. Metadata declarations leave executable SQL; actual SQL CAST expressions remain query expressions. Full rich CAST/tag parsing and end-to-end acceptance are required but still in development at this checkpoint. Preserve requested syntax in the design; label interim linked-field or ColumnType/ColumnTag examples as interim, not complete parity.

For persisted generated projections, a changed CAST updates the owned Go field
(for example, `int` to `*int`), and removing a selected column removes its owned
field and generated support. Retained fields keep their order; unrelated authored
fields, methods, tags and comments stay protected. This regeneration behavior
also applies to generated Go mutation writers. Ownership inventory and
fingerprints must prove that an existing field can be changed or removed; an
unproven older field is not safe to delete. Preserve SQL NULL separately from a
non-null zero when a pointer CAST is selected.

## Reports, cube compose, and WithURI MCP routes

Reports expose dimensions, measures, filters, ordering, and limits over a reader. Compose combines bounded cube requests. Go-only activation supports this component tag:

```go
`component:"Things,path=/things,method=GET,report=true,reportCompose=true,reportComposeMaxCubes=12" mcp:"[{\"kind\":\"tool\",\"name\":\"Things\"}]"`
```

Compose is configurable, not unlimited. Defaults: 8 cubes, result limit 100, timeout 30000 ms. Zero selects defaults, not infinity. Source authorization and query/bind budgets remain effective.

WithURI activates a parameter on an alternative route with a dedicated MCP exposure:

```sql
#package('example.com/app/things/read')
#setting($_ = $input_type('ThingsInput'))
#setting($_ = $output_type('ThingsOutput'))
#setting($_ = $case_format('lc'))
#setting($_ = $connector('main'))
#setting($_ = $route('/things','GET'))
#setting($_ = $mcp('Things'))
#define($_ = $Id<int>(path/id).WithURI('/{id}'))
#define($_ = $Things<[]*Thing>(output/view))
SELECT t.id, type(t, 'Thing') FROM things t
```

This complete reader requires `things(id)` and connector `main`. Root rows
bind to `ThingsOutput.Things []*Thing` under JSON key `things`. The `Id`
declaration illustrates route activation; this query retains its unfiltered
SQL and does not implement an ID lookup.

This yields `/things/{id}` and `ThingsById`. Go `mcpEnabled` and `pathMcpEnabled` control base/alternative visibility separately. Verify each route's active input and tool schema. These declarations expose your component; they do not invent a developer-MCP authoring API.

## Packages, resources, and live updates

Select application packages by canonical module path. Private imported types may be needed without publishing their packages' routes. Dependency loading is not endpoint exposure.

Keep SQL resources with their component and declared namespace. Generated holders expose `EmbedFS() *embed.FS` and `EmbedNamespace() string`; package bootstrap finds those holders through runtime typelinks from blank-imported packages. Do not add a resource registry or depend on mutable working-directory files leaking across a live update.

Compiled Go factories require linked package authority; finding a source name does not make it executable. The generated holder exposes the typed factory, while `internal/datlylink` selects the package. Reuse named types/factories, preserve handwritten hooks, and protect manual edits during regeneration.

A live DQL update publishes a complete validated generation. New requests see the new generation; in-flight requests and nested calls retain a consistent old one. Failed/stale updates leave old routes/types active. Explicit reload is not an automatic database watcher.

## Retry, specialized access, and acceptance

Read retries use the native SQLX reader with a three-attempt policy for errors containing `invalid connection`, resolving the same canonical named/default connector again. Native retry safety and cancellation rules still apply; do not replay delivered rows or hooks. Account for driver retries. This policy is implemented; there is no public DQL retry tag or configurable attempt count.

Most readers need configured connectors, not raw database access. Specialized integrations may explicitly opt into `github.com/viant/xdatly/connector.Provider`:

```go
Connector(ctx context.Context, name string) (*sql.DB, error)
```

Names are exact; empty/unknown/canceled requests fail. The DB is borrowed: do not close it. Direct work is outside managed transaction/flush behavior. This is not a session/transaction getter or client-visible MCP input.

Prove your reader with data-driven SQLite tests: real binding/query/output, same-ID different-tenant relations, zero/NULL/omitted values, empty rows with populated totals, complete multi-batch children, cache replay, and failures. Exercise actual HTTP/MCP transport for public exposure. A tag roundtrip proves syntax preservation, not a functioning reader.


## To-one outer JOIN shorthand

For reader and writer DQL generation, this shorthand marks only the joined relation as one:

Syntax fragment; adapt within the
[complete reader contract](reader-examples.md#parameterized-dql-reader).

```sql
JOIN (...) product
  ON product.id = inventory.product_id AND 1=1
```

Keep the actual key equality (including every composite-key part). The marker
selects a single generated holder instead of a slice; it is not a database
uniqueness constraint. An unmarked sibling relation remains many. This applies
to the outer DQL view join, not arbitrary inner SQL filters. Explicit cardinality
controls retain precedence. Regeneration safely updates unedited generator-owned
holders in both directions, between `*Child` and `[]*Child`. Edited fields,
changed child identity, or untrustworthy ownership require an explicit migration;
never delete authored code to bypass the guard. The shorthand and exact-name
corrections are integrated with reader/writer regeneration tests; run the affected
acceptance cases when changing either path.
