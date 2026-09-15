# Stateless reader builder

`authoring/readerbuilder` edits one complete DQL component at a time. The caller
sends the complete current DQL and one operation. The response contains the
complete resulting DQL, the compiled `spec.Component`, source occurrences and
diagnostics. The service stores no editing session.

```go
builder := readerbuilder.New(readerbuilder.Config{
    Scope: "example.com/shop/orders/read",
    Name: "Orders",
    AvailableConnectors: []string{"main", "analytics"},
    AvailableCaches: []string{"shared"},
})
http.Handle("/v1/datly/reader-builder", builder)
```

The HTTP handler accepts POST and strictly decodes a maximum 1 MiB JSON object.
Malformed transport input returns HTTP 400. A decoded edit failure returns the
normal response with `applied:false`, unchanged DQL and diagnostics.

## Operations

- `inspect`: returns unchanged DQL and structure, including partial structure
  for invalid source.
- `addField`: adds a typed binding and optional `QuerySelector(view)`.
- `addFieldPredicate`, `updateFieldPredicate`, `removeFieldPredicate`: edit a
  repeatable predicate option. Add/update checks every known expansion of the
  group and adds a missing builder to the selected wrapped view.
- `addFunction`, `updateFunction`, `removeFunction`: edit a standalone outer
  projection call represented by its name and ordered DQL-expression arguments.
- `setSetting`: upserts or removes a component `#setting` call. This covers the
  reader MCP declaration, connector, cache/warmup, cube and cube composer.
- `addView`, `updateView`, `removeView`: edit named wrapped relation views.
  Compilation verifies links and requested parent identity; failed/dependent
  removals return the original DQL.

Example predicate request:

```json
{
  "dql": "#setting($_ = $route('/records','GET'))\n#define($_ = $IDs<[]int>(query/ids).Optional())\nSELECT records.* FROM (SELECT r.id FROM records r) records",
  "operation": {
    "type": "addFieldPredicate",
    "predicate": {
      "field": "IDs",
      "view": "records",
      "group": 0,
      "name": "in",
      "args": ["r", "id"]
    }
  }
}
```

The edit also inserts a group-0 predicate builder in `records` because one is
missing. If another view already expands group 0, the request must list the
complete intended `expansionViews`; otherwise the edit fails instead of
silently changing both queries.

When the selected view already has another predicate group, the request must
also supply `combineOperator` (`AND` or `OR`). `groupOperator` controls the
predicates inside the new group and defaults to `AND`. The editor extends the
existing builder chain; it rejects multiple/standalone expansion shapes it
cannot identify uniquely.

Generic selector calls expose every `spec.Selector` capability. For example:

```json
{"type":"addFunction","function":{"name":"selector_page","args":["records","true"]}}
```

Use `addField` to declare the corresponding canonical selector input such as
`Page`, with `querySelector:"Records"`. Permissions, bindings and defaults are
separate metadata.

Cube activation accepts only the intentionally narrow grouped-main shape
documented in [DQL limits](dql.md#limits-and-semantic-validation). Composer DQL
supports `cubeCompose(enabled[,mcpTool,maxCubes,maxLimit,timeoutMs])`.

The builder validates named cache references against service-provided caches or
an enabled cache configuration in the submitted DQL. It edits warmup metadata;
it does not populate or probe a runtime cache.
