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
a body property is not an ambient credential.

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
