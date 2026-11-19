# Offline prepared columns on mainline

The public `repository/shape` DQL compiler accepts trusted Go providers through
`shape.WithPreparedColumns` (or `WithPreparedColumnsDefault` on an Engine).
The provider derives metadata from an authoritative checked-in schema or model.
It must validate the exact supplied SQL, including expressions and aliases, and
return the **complete ordered result**. CAST annotations alone are not evidence
of physical columns or their types.

```go
planned, err := compile.New().Compile(ctx, &shape.Source{
    Name: "records", Path: "records.dql", DQL: dql,
}, shape.WithPreparedColumns(func(ctx context.Context, in column.PreparedRequest) (*column.PreparedResult, error) {
    // Application integration: validate the exact query against its offline model.
    return model.ResultColumns(ctx, in.Component, in.View, in.Connector, in.SQL)
}), shape.WithColumnDiscoveryMode(shape.CompileColumnDiscoveryOff))
if err != nil { return err }
artifacts, err := load.New().LoadComponent(ctx, planned)
```

`column.PreparedColumn` carries the vendor result name, concrete `reflect.Type`,
nullability and SQLX tags (including key/reference metadata). Metadata is copied
into the plan and fresh runtime columns and schemas are materialized by the
ordinary loader. Linked types cannot widen this result; normal column-config
type/tag refinements still apply. No connection is opened, no DDL is emitted, and
missing or invalid metadata never falls back to live discovery. Forced live
column discovery (`on`) and a prepared provider are mutually exclusive.

Closed projections must match every result name and its order, including aliases;
a sole wildcard over a closed subquery or CTE is also checked. Duplicate, empty
or untyped metadata and absent column annotation targets fail. A physical-table
wildcard relies on the trusted provider for completeness. Optional `Table`
metadata is accepted only for a matching direct physical SQL source.

The provider receives source/view/connector identities, unchanged `ExecutableSQL`,
and an analysis SQL copy with local `${embed:...}` resources and braced constants
expanded. Neither executable SQL nor resource file bytes are rewritten. Predicates
remain in the SQL. Embedded files are local authoring inputs; missing or cyclic
references fail. The provider cannot mutate the source or plan through its request.

This is a **public shape compile/load integration**, not a new flag on the legacy
`internal/translator` CLI, an HTTP/request field, or a runtime startup schema gate.
It does not assert anything about a deployed database and does not change writer
field selection or authorize unsupported writes. The v1 `transcribe` API is a
separate implementation.

Current explicit limits: dynamic projections, mixed wildcard projections,
wildcard exclusions, SQL-URI-only views, and summary projections are rejected in
prepared mode. Exec/handler views are not column-discovered by this path. Existing
shape compiler support for DQL, relations, and SQL parsing still applies; prepared
metadata does not extend that syntax. Ordinary query execution still needs its
normal configured runtime connector.
