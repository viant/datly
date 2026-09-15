# Control errors and shape custom output

[All guides](README.md) · [Generated mutators](generated-mutator.md) · [Hook flows](hooks.md)

## Errors from binding and declarations

Declaration fragment for expected binding failures; adapt the input/route in the
[complete reader contract](programming-model.md#dql-describes-the-data-operation):

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

Output declaration fragment from the
[complete reader contract](programming-model.md#dql-describes-the-data-operation);
name the output holder as well as the output type:

```sql
#setting($_ = $output_type('OrdersOutput'))
#setting($_ = $case_format('lc'))
#define($_ = $Orders<[]*Order>(output/view))
```

`output/view` binds the main reader result to `OrdersOutput.Orders`. The `Order`
row shape is declared by `type(orders, 'Order')` in the outer query. This yields
an object with an `orders` collection; the output type setting alone does not
express that association. The [complete reader example](programming-model.md#dql-describes-the-data-operation)
shows the query, global case policy, generated public shape and resulting JSON.

Use explicit output holders, the selected projection, `case_format('lc')`, and
supported selector/exclusion settings to define the public shape. The case policy
is passed to the native Structology JSON marshaler and applies to the envelope
and nested fields; routine per-field JSON tags are unnecessary. Custom handlers can fill a typed output directly.
Output finalizers can enrich it at the supported lifecycle point. Do not expose
internal Has markers or read-provenance metadata as user data.

### Rename a field while retaining the case policy

Use a `format` name when the public name differs from the Go field or SQL column:

```go
Name string `format:"name=CustomerName"`
```

With `case_format('lc')`, the JSON property is `customerName`. In an outer DQL
projection, the equivalent annotation is
`tag(orders.NAME, 'format:"name=CustomerName"')`. This changes presentation;
the SQL column mapping remains independent.

A nonempty `json:"Exact_Name"` takes precedence over the format name and remains
exact even with global casing enabled. Use `format` for ordinary renames rather
than introducing a fixed JSON name. A JSON tag with no name, such as
`json:",omitempty"`, does not itself fix the property name.

## Preserve database NULL or use scalar defaults

Null handling starts in the reader's SQL projection, before JSON encoding.
With the default view policy, nullable scalar columns receive `COALESCE`
fallbacks: numeric values become `0`, strings become `''`, and booleans become
`FALSE`. Types without a supported scalar fallback are not automatically
coalesced.

To preserve database NULLs for a view, add `allow_nulls(orders)` to its outer
DQL projection. Keep the database query inside the view as ordinary SQL.
Use nullable Go fields, such as `*int` or `*string`, to represent those NULLs.
An explicit outer `CAST(orders.TOTAL AS *float64)` also preserves the pointer
shape: the reader does not replace pointer NULLs with scalar defaults, even
when the view has not enabled `allow_nulls`.

Structology encodes a retained nil pointer as JSON `null` unless an applicable
omission policy removes the field. SQL `allow_nulls`, Go pointer shape and JSON
omission are separate controls; enabling one does not imply the others. A
non-pointer scalar cannot preserve the distinction between NULL and its zero
value.

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
[API documentation](api-documentation.md) and the
[HTTP response writer](../gateway/http/response.go).

## Keep completion separate from presentation

A custom error body, transformed output or success status does not prove a database
commit. Commit-dependent messages use the mutation outcome contract. An output
finalizer's ordering depends on its selected interface; injector-aware finalization
can compose child work before completion, while outcome finalization observes
the resulting transaction evidence. See [custom handlers](custom-handlers.md#finalization-and-commit-evidence).
