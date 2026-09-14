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

Metadata alone is not host registration. Use the canonical report compilation
and registration path when embedding; [report registration tests](../report/registration_test.go)
show the complete owner. The current linked standalone path rejects report
derivation, so this is not a switch to add to the quickstart executable.

Declare field roles and input filters in the source contract. The derived API
preserves source authorization and maps requested contract field names to SQL
columns. Required filters stay required, including dependencies and non-query
sources. Omitted frame filters must not borrow accidental values from the outer
HTTP request.

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
