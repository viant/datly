# Control errors and shape custom output

[All guides](references/product/datly/doc/README.md) · [Generated mutators](references/product/datly/doc/generated-mutator.md) · [Hook flows](references/product/datly/doc/hooks.md)

## Errors from binding and declarations

Use declaration metadata for expected binding failures:

```sql
#define($_ = $OrderID<int>(path/id).Required().WithStatusCode(400).WithErrorMessage('order id is required'))
```

The failure status/message belongs to the declared input. JWT verification,
required values and codecs run through the canonical input path. Do not substitute
unverified input claims for authorization or leak private diagnostics to clients.

## Errors from business hooks

Return an error from Init, Validate, AfterSequence or AfterQueue to fail that
phase and stop later writer phases. Structured public errors use the SDK's
`response.Error`:

```go
return &response.Error{
    Code: 422,
    Payload: struct {
        Code string `json:"code"`
        Message string `json:"message"`
    }{Code: "invalid_window", Message: "start must not exceed end"},
    Cause: err,
}
```

Import `github.com/viant/xdatly/response`. `Cause` is the private wrapped error;
`Payload` is deliberately public. Author safe messages and preserve useful causes
for diagnostics. HTTP status and MCP tool errors have transport-specific surfaces;
test the one your application exposes.

Inside a custom handler, `session.Response().SetStatusCode(...)` sets status,
and the response facade also accepts errors/metrics. It does not replace returning
a failure when an operation must stop. Outcome-aware finalization distinguishes
rollback, confirmed commit, caller-owned pending work and uncertain completion.

## Shape an ordinary typed output

A reader's declared projection defines its row contract. A writer's output is
independent of its request body: author a separate output type when the API needs
an envelope, summary or selected fields. Generated output shapes are selected by
DQL output declarations/type settings; they are not an automatic mirror of all
database columns or a combined reader/writer component.

Use JSON tags, explicit output holders and supported selector/exclusion settings
to define the public shape. Custom handlers can fill a typed output directly.
Output finalizers can enrich it at the supported lifecycle point. Do not expose
internal Has markers or read-provenance metadata as user data.

## Return an already-shaped response body

Use `response.Response` when application code already produced final bytes.
`response.Buffered` is the supplied in-memory implementation:

```go
func CSV() response.Response {
    return response.NewBuffered(
        response.WithStatusCode(200),
        response.WithHeader("Content-Type", "text/csv; charset=utf-8"),
        response.WithHeader("Content-Disposition", "attachment; filename=orders.csv"),
        response.WithBytes([]byte("id,total\n42,125.50\n")),
    )
}
```

The HTTP adapter recognizes the transport-ready response and writes its body
rather than JSON-marshalling the buffer as a Go entity. `WithBuffer` accepts an
existing bytes.Buffer. Keep backing bytes stable after handing off the response.
Use the supported direct/output-body contract in the component; an arbitrary
interface field does not by itself declare a transport response.

Document content type, status, headers, schema and examples explicitly for
OpenAPI/MCP. Runtime bytes cannot provide a reliable static schema. See
[API documentation](references/product/datly/doc/api-documentation.md) and the
[HTTP response writer](references/product/datly/gateway/http/response.go.txt).

## Keep completion separate from presentation

A custom error body, transformed output or success status does not prove a database
commit. Commit-dependent messages use the mutation outcome contract. An output
finalizer's ordering depends on its selected interface; injector-aware finalization
can compose child work before completion, while outcome finalization observes
the resulting transaction evidence. See [custom handlers](references/product/datly/doc/custom-handlers.md#finalization-and-commit-evidence).
