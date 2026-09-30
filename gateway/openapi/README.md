# OpenAPI generation

`Generator.Generate` creates an OpenAPI 3.0.1 document from the same compiled
registrations passed to `runtime.NewRuntime`. `NewHandler` generates and encodes
an immutable snapshot for an application-owned HTTP mux. Neither entry point
executes components or opens databases.

```go
// entries are the results of bootstrap.Artifact.Registration.
rt, err := runtime.NewRuntime(entries, runtimeOptions...)
if err != nil {
    return err
}
request := openapi.Request{
    Info: openapi3.Info{Title: "Records API", Version: "1.0.0"},
    Components: entries,
    Visibility: rt,
}
document, err := (openapi.Generator{}).Generate(ctx, request)
if err != nil {
    return err
}
// document is caller-owned and can also be encoded with encoding/json.
_ = document

docs, err := openapi.NewHandler(ctx, request)
if err != nil {
    return err
}
mux := http.NewServeMux()
mux.Handle("/openapi.json", docs)
mux.Handle("/", gatewayhttp.NewHandler(rt, logger, "1.0.0"))
```

Imports are `github.com/viant/datly/gateway/openapi` and its `openapi3`
subpackage. The serving runtime implements `Visibility`; supplying it applies
its existing public-package selection. Use the same registration catalog as
that runtime. Omitting visibility intentionally documents every supplied route.
Keep registrations immutable while generating; returned documents share no
mutable schema or operation state with registrations or other documents.

Go-shape authoring and DQL authoring converge on bootstrap artifacts, so this
package consumes their compiled results through one entry point. Tests exercise
real tagged Go contracts, bootstrap compilation, registered handlers, HTTP
binding, and a SQLite-backed typed reader. DQL -> generated multi-module package
-> reload -> OpenAPI acceptance is a separate gate.

The registry InputCatalog traverses canonical, exact component dependencies and
uses `InputContract.ForRoute(...).Fields()` for transport names,
source types, and requiredness. A codec's destination field type does not replace
its compiled transport source type. WithURI is already expanded by bootstrap;
the document reflects each route's effective bindings without reinterpreting
activation syntax. Private child inputs participate in a public parent contract
without publishing private routes or inheriting child HTTP-only API-key gates.
Identical shared inputs merge requiredness; incompatible overlaps, missing targets
and exact-route cycles fail explicitly. Generic Go field discovery and type expressions come from
`viant/x/shape`. JSON tag tokenization uses `tagly`.

Supported document behavior:

- GET, PUT, POST, DELETE, OPTIONS, HEAD, PATCH, and TRACE path operations;
  path/query/header/cookie inputs, repeated scalar query keys, JSON bodies and
  scalar/repeated URL-encoded form fields. Body documentation supports POST,
  PUT, and PATCH.
- Whole JSON bodies and literal named top-level body keys; required body inputs
  determine request-body and property requiredness. Transport names retain
  binding authority even when JSON field tags differ.
- Structs, arrays/slices, nesting, recursive object references, string-keyed maps,
  scalar types, time.Time, JSON byte strings, and fixed array lengths. Pointer
  references use a null-only `anyOf` branch because OpenAPI 3.0 Reference Objects
  do not support nullable siblings. Input/output schemas have separate identities.
- JSON-hidden and unexported fields are omitted. Body presence holders are always
  internal. An output presence holder must also be JSON-hidden in the actual
  encoding contract; otherwise generation fails instead of hiding a wire leak.
- The compiled output plan's `Wire` authority and default format, with route marshaller precedence.
  A declared `FormatSelector()` lists the representable JSON, CSV, XML and XLSX
  response media types; `header/Accept` is represented by those media types,
  while a query source remains an input parameter. JSON has a typed schema,
  CSV/XML a text schema, XLS/XLSX a binary schema. HEAD has
  no response body. The ordinary HTTP success response is 200.
- API-key security from the route policy actually enforced by gateway/http, and
  bearer JWT security from an explicitly constructed runtime/auth verifier codec.
  Registration captures concrete verified-codec evidence; codec names or ordinary
  Authorization text cannot assert it. Required/optional JWT inputs produce the
  corresponding security alternatives, with API-key requirements retained on
  every branch. Key values are never published. Same-header API-key/JWT composition
  fails explicitly. Reserved header inputs without usable runtime policy also fail.
- Duplicate route/component identities, equivalent templates, schema identity
  collisions, duplicate JSON names, missing route contracts, inconsistent output
  types, and unsupported shapes fail with an error and no partial document.

This is a bounded generator, not complete original OpenAPI feature parity.
CLI export and automatic endpoint mounting are not wired. Legacy `_format`
overrides without a declared selector, tabular JSON's alternate shape under
the same media type, dynamic handler-selected status,
response headers, explicit Response objects, and arbitrary error bodies are not
invented from types. The document describes the default response representation.
There is no header-text bearer, OAuth, or custom authentication schema inference.

Transformed JSON presentation (exclusions, casing/date transformations, custom
serialization), XML/tabular schemas, embedded JSON field dominance, JSON `string` options,
object-valued query/form/header/cookie parameters, non-query array parameters, the exact `[]byte`
query/form encoding, non-string map keys, conditional transport policies, and
bodies on other methods currently return explicit errors. Correct support for
presentation and embedding requires projections from the compiled output and
native shape/tag owners. The compiled output owner now decides wire representability; source settings do
not override it. Rich transformed projections remain outstanding. Named byte
slices in query/forms use repeated numeric values, while JSON retains base64 byte
semantics. Fixed query arrays retain their compiled length bounds.

Original Datly baseline:
`gateway/router/openapi/{openapi3.go,generator_paths.go,generator_operation.go,path.go,schema.go,schema_build.go}`
and `openapi3/`, inspected read-only at commit
`54ae48e3a4649d32aa3018051932fab2ad5f6b38`. Document/path/schema/media/security
concerns remain separate. Model declarations were ported with the Apache-2.0
license and provenance in `openapi3/{LICENSE,NOTICE}`. Mutable loading sessions,
heavy runtime reach-through, speculative default errors, header-text bearer
inference, and silently ignored unsupported methods were not brought across.

Run `go test -race ./gateway/openapi/...` and `go vet ./gateway/openapi/...`.

## Application publication

The application manager stages JSON/YAML document snapshots and exports a pinned
generation with `ExportOpenAPI`. Request route selection retains complete
dependency authority. See [API documentation](../../doc/api-documentation.md)
and [configuration](../../doc/configuration.md) for configuration, publication
and startup export behavior. See [release status](../../doc/status.md) for
validation limits. Full media/UI parity is not implied by schema publication.
