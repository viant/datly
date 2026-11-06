# Caller-dependent contracts in v1

A linked view/domain type, component Input/Output, or composite implements `github.com/viant/xdatly/contract.Provider`:

```go
Contract(context.Context, bindly.Binder) (*contract.Policy, error)
```

The hook binds arbitrary application state through the component's native binding
scope. JWT verification, component resolution, constants and registered codecs retain
their existing owners. Discovery does not execute the target business handler.

A policy returns a stable `Name` and a `Field` callback. The callback receives the
canonical parent path, original parent type, and editable field name/tags. Match both
the business type and field name for targeted rules. `Hide`, `Required`, `Optional`,
`SetEnum`, `SetDescription`, `SetExample`, `SetTag` and `RemoveTag` preserve unrelated
metadata. Fields are optional unless explicitly required. Entity validation tags
and native insert/update presence validation retain their own behavior.

## Storage and codecs

The original linked instance, storage types, field positions, embedding, offsets and
methods remain authoritative. `shape.Representation` carries opt-in metadata without
requiring `reflect.StructOf`. Hidden fields remain in application state. JSON decoding
filters hidden members; JSON encoding omits them without clearing original values.

Native standard JSON and Structology JSON paths accept metadata. Configured codecs
can implement the shared optional `contract.JSONEncoder` / `contract.JSONDecoder`
capabilities. Existing configured codecs retain the reflected projection where Go
can represent it; method-bearing embeddings require metadata participation.
Presence holders remain internal and are derived by the native decoder/binder.

See [feature boundaries](dynamic-contract-boundaries.md) for custom JSON methods.
Complete custom JSON/Text representations cannot be rewritten internally. Recursive
rewrites remain explicitly unsupported; unchanged recursive linked types are retained.
Rewritten output is currently JSON-only; unsupported formats fail before execution.

## Composition and traversal

Nested `Contract` implementations are discovered through pointers, slices, arrays,
map values and embedded structs. Policies apply nested-first, enclosing-parent-last.
Application code does not manually delegate to each child.

`nil, nil` from `Field` retains that field and continues descending.
`field.Clone().SkipChildren()` skips descendants for that policy only. It can be
combined with an edit such as `Hide()`. Nested policies remain active, and a skip
flag is not inherited by other policies. The native walker prunes a branch only
when no active policy needs it. Skip-only results retain original linked identity.

## Caching and transport consistency

Policy selection runs for each caller. Named variants cache immutable metadata by
registration generation, component, route, direction, original type and the composed
policy names/paths. A name must identify the same metadata edits. Empty names disable
retained variant caching. Caller data and callback closures are never cached.

HTTP, MCP invocation, OpenAPI and MCP input/output schemas consume the selected
contract. Dynamic discovery is private/non-retained, and shared catalogs are not
mutated. Optional metadata does not bypass authorization or change native MCP
unknown-argument leniency. Enum tags describe schemas; they do not add runtime enum
membership validation.

## Declared operation in contract state

Contract hooks can bind trusted target metadata using the existing Binder:

```go
var state struct {
    Method string `parameter:",kind=contract,in=method,required=true"`
    Path   string `parameter:",kind=contract,in=path,required=true"`
}
if err := binder.Bind(ctx, &state); err != nil {
    return nil, err
}
```

These values come from the declared component target during HTTP/MCP discovery
and invocation. They are not the outer schema-fetch GET or MCP transport POST.
The `contract` provider kind is runtime-owned; protocol/component providers cannot
replace it. Existing `http_request` metadata retains its transport meaning.

DQL already supports distinct names for selected method/path targets through
`mcp_route`. Declaring both POST and PATCH can expose CreateCampaign and PatchCampaign
without inferring tool names or treating client arguments as operation authority.
A route method does not override a generated writer's mutation policy. Applications
must keep schema requiredness consistent with the selected execution policy; a
POST writer exposed over PATCH remains insert-only unless its writer policy changes.


> Packaging boundary: Application-facing hook, composition, codec, caching and trusted operation contract; excludes local verification history, benchmarks and revision pins.
