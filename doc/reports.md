# Reports, cubes and composition

[All guides](README.md) · [Readers](readers.md)

Use a report when callers need to select dimensions, measures and filters from a
declared reader contract. Use composition when a result compares several such
query frames—for example web versus store spend—without exposing arbitrary SQL
sources to the caller.

## Enable reports in component metadata

A Go component field can opt into report derivation and bounded composition:

```go
// Tag fragment on an xdatly.Component[I, O] field:
`component:"Things,path=/things,method=GET,report=true,reportCompose=true,reportComposeMaxCubes=12" mcp:"[{\"kind\":\"tool\",\"name\":\"Things\"}]"`
```

Configured standalone/custom builds discover the selected linked packages and
register the source reader, its POST `/cube` endpoint and opt-in `/cube/compose`
endpoint together. Enable `report=true` on a GET reader with a groupable output
view; enable `reportCompose=true` for composition. Link the source input/output
and referenced types in the executable; derived report inputs are compiled during
publication and do not need an application-owned binder or report engine.

The source reader remains available. SQL URI/embed resources, scoped independent
view providers, declared JWT inputs, SQLX mapping and native cache services keep
their existing owners. Failed derivation or resource resolution leaves the previous
generation published. Go contract/method changes still require a rebuild.

Embedding uses `report.NewProjectCompiler(...).CompileArtifacts(...)` followed by
`Compilation.RuntimeComponents(...)`. Standalone uses these same owners. An HTTP
API-key route cannot become an MCP tool: set `reportMCPTool=false` and
`reportComposeMCPTool=false` on that reader, or author an MCP-compatible policy.
JWT verification requires configured `JWTValidator` plus the declared source input;
a body property is not an ambient credential. Compiled JWT verifier inputs stay
outside cube filters, so an omitted frame filter cannot mask Authorization and
a frame's `jwt` property cannot replace the transport credential.

Declare field roles and input filters in the source contract. The derived API
preserves source authorization and maps requested contract field names to SQL
columns. Composition accepts declared SQL output aliases and explicit authored
selector mappings; a different Go/JSON spelling is not an implicit SQL alias. Required filters stay required, including dependencies and non-query
sources. Explicit cube filters override their source values; omitted cube filters
retain source binding. Composition masks omitted frame filters so they cannot
borrow accidental values from the outer HTTP request.

Dictionary enrichment follows the selected grouping grain. For a lookup keyed by
country and region, selecting both dimensions includes the lookup; selecting only
country leaves it out. A composite lookup must not add an unselected dimension
and silently split the aggregate into smaller groups.

## Generated cube components

Transcribing a cube-enabled reader emits a named cube input, output alias and
component holder alongside the source. The cube registers as an ordinary POST
component at the source route plus `/cube`. Other components can depend on that
route, and MCP exposes the named input contract. The generated factory delegates
to `report.NewLinkedFacade`; source execution still owns SQL, codecs, validation
and authorization.

The facade maps dimension and measure selections to source fields and forwards
public source inputs through `filters`, whether or not they declare predicates.
Filter values have a hidden `Has` marker so explicit `false`, zero and empty
strings remain distinct from omission. Generated setters update that marker for
Go callers. Internal dependencies and context inputs remain source-owned. Cube
projection, ordering and pagination control the root view; other-view selectors
and source criteria can still be forwarded.

Selection JSON names come from normalized Go field identities: SQL
`campaign_id` and linked `CampaignId` both expose `campaignId`. Reader JSON tags
do not rename cube selections; `json:"-"` still excludes them. An input field
tagged `mcp:"-"` remains in the facade's HTTP contract and retains its source
defaults and binding, but is omitted recursively from MCP input discovery.

Dynamic DQL uses the same facade metadata and handler with runtime-derived
shapes. Its synthetic type namespace belongs to the source component scope and
version under `/_datly_cube`, keeping it separate from generated Go contracts.
Linked source metadata records its companion to prevent duplicate dynamic cube
registration; discovery validates that the companion exists in the same package.
Re-authoring DQL clears this generated marker and derives its own cube.

## Report ordering permission

Native cubes can order selected dimensions and measures even when the source
reader declares `selectorOrderBy=false`. Direct reads still enforce that setting.
The report supplies an invocation-local permission for its exact target route
and canonical view, restricted to selected scalar fields. SQL ordering validation
still resolves explicit aliases, enforces orderable-column restrictions, and
checks the final grouped projection. Hidden lookup keys do not add sortable
report fields. Invalid or disabled ordering returns HTTP 400 and an MCP tool
error; database and internal failures retain normal sanitized error handling.

A handler-backed report must explicitly forward this permission to its designated
private reader, alongside its existing typed selectors:

```go
request := exec.ComponentRequest{
    Target: privateReaderTarget,
    Input: input,
    Providers: selectorProviders,
    ReportOrdering: exec.ForwardReportOrdering(ctx, privateReaderTarget, "readerView"),
}
result, err := invoker.InvokeComponent(ctx, request)
```

Use the private reader's canonical view name. `ForwardReportOrdering` returns nil
for ordinary handler invocations. Component boundaries never inherit permission
implicitly, even if selectors are forwarded; unrelated children retain their own
policies. The grant does not replace authentication, change execution ownership,
or mutate shared reader metadata.

Pagination is separate: generated cube inputs currently advertise `limit` and
`offset` unconditionally, but both still obey the source reader's permissions and
limit cap. Report ordering permission does not enable either control. Composition
has its own validated outer SQL ordering and does not grant per-frame ordering.

## Compose query frames

This body comes from the [HTTP/MCP composition fixture](../report/compose_integration_test.go).
It requires that fixture's `Spend` contract; it is not a request against the
quickstart service:

```json
{
  "cubes": [
    {"filters": {"accountIDs": "1,2", "tenant": "acme", "region": "EU", "channel": "web", "status": "active"}},
    {"inheritFrom": 1, "filters": {"channel": "store"}}
  ],
  "sql": "SELECT t1.AccountID, t1.TotalSpend AS web, COALESCE(t2.TotalSpend, 0) AS store FROM $CubeSQL1 AS t1 LEFT JOIN $CubeSQL2 AS t2 ON t1.AccountID = t2.AccountID ORDER BY t1.AccountID LIMIT 8"
}
```

`inheritFrom` references a prior frame using one-based numbering. The wrapper
uses `$CubeSQL1` … `$CubeSQLN` and validated contract columns. It does not name
arbitrary tables or bypass source predicates. Each frame's SQL and arguments are
prepared separately, then composed with ordered bindings.

Composition is list-based. Default limits are 8 cubes, result limit 100 and
30000 ms timeout; zero selects defaults rather than infinity. Authored settings
can change permitted budgets. The combined query must still fit the dialect's
placeholder limit even when each frame fits independently.

### Combine independently aggregated rows

An opt-in compose endpoint also accepts bounded `UNION ALL` requests. Supply
one frame per branch in order: branch N reads only `$CubeSQLN AS tN`. Each
branch projects explicitly aliased source fields or scalar literals; the same
column aliases and compatible concrete types must appear in every branch.
A NULL literal paired with a concrete column produces a nullable output type.
Columns containing only NULL, implicit aliases, arithmetic, aggregate calls,
joins, branch WHERE/GROUP/HAVING, CTEs and subqueries are rejected. Ordinary
join composition keeps its existing contract.

For example, a root measure frame and category measure frame may be combined:

```sql
SELECT 'root' AS rowKey, NULL AS parentKey, 'All categories' AS category,
       t1.Amount AS amount
FROM $CubeSQL1 AS t1
UNION ALL
SELECT t2.CategoryKey AS rowKey, 'root' AS parentKey, t2.Category AS category,
       t2.Amount AS amount
FROM $CubeSQL2 AS t2
ORDER BY rowKey
LIMIT 50 OFFSET 0
```

The example requires declared string `Category` and `CategoryKey` dimensions
and a numeric `Amount` measure. Author `CategoryKey` as a stable, unique key
in a separate leaf namespace (for example `leaf:` followed by an injective
encoding of the category identity). The key must be functionally determined
by Category so it does not change the grouping grain. The hierarchy consumer
must reject duplicate keys and a leaf key equal to the reserved `root` key.
Composition itself does not validate hierarchy identity. Each
frame's referenced fields determine its native grouping: the root is computed
without Category, independently of the leaf rows. Ratios and distinct counts
are never obtained by summing the leaf results.

Only the final branch may carry ordering and pagination. ORDER BY accepts
output aliases; a positive OFFSET requires ordering. LIMIT is positive and
LIMIT plus OFFSET cannot exceed the authored compose MaxLimit. Omitted LIMIT
uses that configured bound. The limit applies once to the combined result,
not to either prepared frame. Native execution reads at most one additional combined-result row as a
lookahead, including when LIMIT equals MaxLimit; the response still returns
at most LIMIT rows. This documented one-row lookahead is the only allowance
beyond the response budget. No frame receives a separate pagination limit.

Responses retain `columns` and `data` and add `limit`, `offset`, `hasMore`,
`complete`, and `truncated`. All are explicitly encoded, including false and
zero values. `hasMore` is true only when the lookahead row exists; `complete`
is true only at offset zero with no lookahead. `truncated` is the inverse of
`complete`, including a final page that omitted earlier rows. Thus an exact-cap
result is distinguishable from a truncated result, while an offset tail never
claims to be the complete dataset. These additive fields also apply to the
existing join composition endpoint, with its existing offset-zero contract.
`Plan.Render` retains exact-limit semantics for existing SDK callers;
`Plan.RenderPage` supplies the bounded lookahead used by native execution.

Consumers that require complete hierarchy/export data must require
`complete=true` (equivalently `hasMore=false` and `truncated=false`). The budget
is still finite; a truncated response is not an export-completion signal.
When `hasMore=true` at `LIMIT+OFFSET==MaxLimit`, a further page beyond that
authored budget is rejected rather than reported as empty.

Each branch is prepared through the same source component and its authorization
predicates. Inheritance only fills omitted input filters from preceding frames;
it does not grant authority to override trusted scope. Invalid source filter
values and rejected source authorization fail the entire composition.

## Grouping and cache projection

Selecting dimensions changes grouping semantics. A cache warmed at
`Tenant, AccountID, Region` cannot safely answer a request dropping `Region`
merely by hiding that column: sums/counts may require reaggregation. For the
requested grouped cache-reuse contract, regular queries must retain **all warmed
dimensions** and may select a subset of measures. Full projection warmup plus
narrower ordinary-field reads is a separate case.

Parent release-candidate race tests pass the AFS and live Docker Aerospike
warmup/cube-mask matrix, including narrowed ordinary/cube projections, indexed
groups, pagination and replay after source tables are removed. Cube reuse retains
all warmed dimensions and permits a subset of measures. This is bounded parent
acceptance, not production-scale qualification; the exact-name correction still
awaits final code delivery and review. See [cache acceptance](cache-and-warmup.md).

## Verify the result you expose

### Semantic ordered prefixes

Cube composition accepts optional integer `resultLimit` for a source-defined
ordered prefix, separately from SQL `LIMIT`/`OFFSET` transport paging. Omit it
to preserve existing behavior. This initial contract requires one flat cube
selection, an explicit `ORDER BY`, no UNION/join/outer GROUP BY, and a positive
prefix size no larger than the configured composition limit.

```json
{"cubes":[{"filters":{"tenant":"sample"}}],"sql":"SELECT t1.Category AS category, t1.Amount AS amount FROM $CubeSQL1 AS t1 ORDER BY amount DESC, category ASC LIMIT 2 OFFSET 2","resultLimit":5}
```

Source predicates, grouping, native calculations and ordering run before the
prefix. The example returns positions 3–4 of that five-row semantic result.
Lookahead never crosses its end. A complete zero-offset read of all five rows
returns `complete:true`, `hasMore:false`, `truncated:false`, and echoes
`resultLimit:5`; this means complete **within the declared semantic result**, not
the unrestricted source population. Later pages remain partial as usual. Reading
all pages for export has exactly the same membership as the complete result.
No tie-breaker is invented: declare additional ordering terms if stable tied-row
identity is required across pages. This is positional selection, not dense rank.

`CompileWithResultLimit` is the corresponding planner API; nil preserves
`Compile`. Both exact `Render` and lookahead `RenderPage` honor the prefix.
Do not use this feature before client-side percent-of-total, running totals or
other table calculations whose original partition includes excluded rows.
Those cases require their calculations to execute before membership selection.
Source cache/authorization rules remain unchanged; the semantic limit never
becomes a fact predicate or changes contributor scope.

Test multiple dimensions, aggregate-only requests, nullable left joins, required
filters, renamed columns, inherited frames and exceeded budgets. Confirm that
HTTP and native MCP use the same exposed catalog and denied rows remain denied
inside every frame. Keep report data and physical internal columns separate in
schemas. [Grouped tests](../report/grouped_sqlite_test.go) and
[composition tests](../report/cubecompose/sqlite_test.go) provide concrete cases.

## Configured-runtime verification

The discovered-package fixture in `standalone/source_report_test.go` exercises
SQLite grouped cubes and inherited composition, required header filters, JWT and
API-key denial, explicit SQL aliases, URI reload, failed publication and resource
ownership. `standalone/source_report_cache_test.go` warms the native AFS cache,
retains all grouping dimensions while narrowing measures, then verifies replay
after deleting the source table. Dropping a dimension cannot reuse that grouping.
`standalone/source_report_mcp_test.go` exercises native MCP stdio and provides the
streamable HTTP test for a host with TCP access. Both stdio and streamable HTTP have executed acceptance tests; verify the
transport and deployment configuration your application actually uses.

`standalone/source_report_build_test.go` builds a custom executable with automatic
package linking, then exercises cube/composition through its generated Registry
and Workspace. The runtime JWT tests use explicit linking: automatic traversal of
`jwt.RegisteredClaims` is a separate unresolved linker case, not established by
this automatic-build test. Multi-key composition ON expressions also need their
own parser acceptance; the configured fixture proves a single-key left join.

The `exec` request options in this guide are Datly runtime-adapter controls.
Application DI ordinarily uses `github.com/viant/xdatly/handler.ComponentInvoker`;
these controls are not fields of its public request contract.

### Transport page overrides

The generated compose input accepts optional `page: {"limit": 20, "offset": 20}`.
This overrides only the compiled transport window, preserving the authored SQL,
source filters, expressions, grouping, ordering and optional semantic `resultLimit`.
Omission (or null) preserves existing SQL LIMIT/OFFSET behavior. An explicit empty
or zero page is rejected: limit must be positive, offset nonnegative, and their
sum no larger than the native compose MaxLimit. Positive offsets require explicit
ordering. The planner validates these bounds without integer overflow; execution
uses the native combined query offset and one bounded lookahead row.

`Plan.WithPage(Page)` returns a detached plan, leaving its compiled input unchanged.
`PreparedComposition.Page()` returns copied effective compiled LIMIT/OFFSET metadata;
`ResultLimit()` returns a copied optional semantic bound. `Budget().MaxRows` remains
the producer ceiling. Hosts should validate the effective authored window against
their report cap, rather than rejecting a small authored LIMIT merely because the
producer ceiling is higher. A producer ceiling of 1000, report cap of 100 and an
authored LIMIT of 10 are compatible.

## Bound scalar composition controls

A source may opt into typed wrapper values with
`CubeComposeSettings{Enabled: true, ValuesType: "controls.WrapperValues"}` or the
component holder tag `reportComposeValuesType:"controls.WrapperValues"`. Resolve named types
from the same package/import catalog as the source component. `ValuesType` may
also contain an authored inline Go struct shape, materialized through the existing
Go shape compiler; adding controls this way requires no application binary change.
For example, persist the setting in component metadata:

```json
{"report":{"compose":{"enabled":true,"valuesType":"struct { Threshold int64 `json:\"threshold\"`; Metric int `json:\"metric\" choice:\"1\" choice:\"2\" validate:\"choice(1,2)\"` }"}}}
```

DQL may pass the same shape or named type as the optional sixth
`cubeCompose(true,true,8,100,30000,'TYPE_EXPRESSION')` argument. The type
must be a flat struct of exported scalar fields (or pointers to scalars), with
safe, distinct JSON field names. Collections, nested structs and anonymous fields
are rejected. Existing sources without `ValuesType` keep their existing input
contract and reject SQL placeholders.

```go
type WrapperValues struct {
    Threshold int64  `json:"threshold"`
    Metric    int    `json:"metric" choice:"1" choice:"2" validate:"choice(1,2)"`
    Label     string `json:"label" choice:"alpha" choice:"beta" validate:"choice(alpha,beta)"`
}
```

Reference those exact JSON names as `$Values.threshold`, `$Values.metric`, and
`$Values.label` in authored wrapper scalar expressions. For example:

```sql
SELECT t1.key, t1.amount
FROM $CubeSQL1 AS t1
WHERE t1.amount >= $Values.threshold
ORDER BY t1.amount DESC
LIMIT 20
```

Invoke the generated native compose component normally, including a `values`
object alongside `cubes`, `sql`, optional `page`, and optional `resultLimit`:

```json
{"cubes":[{"filters":{}}],"sql":"SELECT t1.amount FROM $CubeSQL1 AS t1 WHERE t1.amount >= $Values.threshold","values":{"threshold":1000}}
```

`PrepareComposition` validates references against the same native authored type;
its `ValuesType()` metadata exposes that shape. Preparation grants no execution
authority. Execute through the registered compose target and ComponentInvoker,
which still prepares each source with its original filters and authorization.
Wrapper filters apply after source aggregation; changing a threshold never
changes the fact-row predicates. Page and semantic result-limit rules are
unchanged.

Every referenced value must be present and non-null. Declared unused fields may
be omitted. Supplied nulls, unknown keys (including differently cased aliases),
wrong scalar types and integer overflows are rejected. Explicit zero, false and
empty string remain values. Integers decode directly into their declared width,
without a floating-point intermediate. `choice` tags retain their existing schema
metadata role; use explicit `validate:"choice(...)"` for membership checks.
Only supplied fields are validated using the existing validator. The generated
HTTP/MCP schema retains the authored scalar and choice metadata while making
all controls optional until a particular SQL statement references them. Supplied
controls are non-null even when their Go declarations use pointers. Internally the generated input
holds `Values` as `json.RawMessage` so strict native decoding can preserve exact
keys and integer values; trusted Go callers populate that field with JSON too.

All wrapper values render as SQL arguments in lexical clause order, including
projection expressions before source arguments and WHERE/HAVING/ORDER BY
expressions after them. UNION ALL admits typed scalar values in its already
supported projection positions. Values cannot identify a table, field, SQL
function, operator, sort direction, LIMIT/OFFSET or cube frame, and are never
interpolated into SQL. Choose among authored metric expressions with an approved
compiled expression or a separately supported authored CASE expression; never
accept SQL expressions from a client value.
