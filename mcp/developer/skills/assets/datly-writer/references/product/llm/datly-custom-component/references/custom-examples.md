# Custom component examples

## Typed application-owned handler

~~~~go
package greeting

import (
    "context"
    "fmt"
    xdatly "github.com/viant/xdatly"
    "github.com/viant/xdatly/handler"
)

type Components struct {
    Greet xdatly.Component[Input, Output] `component:"Greet,path=/v1/greeting,method=POST,handler=NewGreeting"`
}
type Input struct {
    Name string `parameter:"Name,kind=body,in=name" validate:"required"`
}
type Output struct { Message string `json:"message"` }
type Greeting struct{}
func NewGreeting() handler.Contract[Input, Output] { return &Greeting{} }
var _ handler.Contract[Input, Output] = (*Greeting)(nil)

func (*Greeting) Exec(ctx context.Context, session handler.Session,
    input *Input, output *Output) error {
    if input == nil || output == nil || session == nil || session.Binder() == nil {
        return fmt.Errorf("greeting invocation is incomplete")
    }
    // Dependencies are local to this invocation, not mutable shared handler state.
    deps := struct {
        Validator handler.Validator `bind:"kind=validator"`
    }{}
    if err := session.Binder().Bind(ctx, &deps); err != nil { return err }
    if deps.Validator == nil { return fmt.Errorf("validator capability is required") }
    result, err := deps.Validator.Validate(ctx, input)
    if err != nil { return err }
    if err := result.Err(); err != nil { return err }
    output.Message = "Hello, " + input.Name
    return nil
}
~~~~

The validator must be supplied by the project; a tag does not install it. The developer MCP workflow registers the named constructor and selected exposure.

## Component dependency

~~~~go
deps := struct {
    Access *AccessResult `bind:"kind=component,in=GET:/internal-access,required"`
}{}
if err := session.Binder().Bind(ctx, &deps); err != nil { return err }
~~~~

Use the actual registered component identity and declared dependent input. The dependency can remain private; do not expose it or call an arbitrary URL implicitly.

### Explicit typed forwarding and trusted service registration

Use this when `POST /v1/records/check` receives JSON `tenantId`/`id`, but the existing private `GET /internal/records/lookup` has its own typed input. The types and package scopes below are application examples: use the actual lookup component's input/output contracts and registered identity rather than inventing substitute DTOs.

Application handler imports:

~~~~go
import (
    "context"
    "fmt"
    dexec "github.com/viant/datly/exec"
    "github.com/viant/datly/spec"
    "github.com/viant/xdatly/handler"
)
~~~~

The parent input can use body fields while the private input ordinarily uses query fields:

~~~~go
type CheckInput struct {
    TenantID int64 `json:"tenantId" parameter:"TenantID,kind=body,in=tenantId,required=true"`
    ID int64 `json:"id" parameter:"ID,kind=body,in=id,required=true"`
}
type LookupInput struct {
    TenantID int64 `parameter:"TenantID,kind=query,in=tenantId,required=true"`
    ID int64 `parameter:"ID,kind=query,in=id,required=true"`
}
~~~~

Inside the handler, bind invocation capabilities and pass an explicitly constructed `*LookupInput`. `Authorizer`, `LookupOutput`, and its record type are your real application interfaces/types, not SDK defaults:

~~~~go
deps := struct {
    Authorizer Authorizer `bind:"kind=records.authorizer,required"`
    Invoker dexec.ComponentInvoker `bind:"kind=component_invoker,required"`
}{}
if err := session.Binder().Bind(ctx, &deps); err != nil { return err }

value, err := deps.Invoker.InvokeComponent(ctx, dexec.ComponentRequest{
    Target: dexec.ComponentTarget{
        Component: spec.Key{
            Kind: spec.KindComponent,
            Scope: "example.com/private/records",
            Name: "Lookup",
        },
        Route: spec.RouteRef{Method: "GET", Path: "/internal/records/lookup"},
    },
    Input: &LookupInput{TenantID: input.TenantID, ID: input.ID},
})
if err != nil { return err }
lookup, ok := value.(*LookupOutput)
if !ok { return fmt.Errorf("unexpected lookup result type %T", value) }
// Apply your authorizer to the returned record and verify its identity.
~~~~

This is an internal typed component call, not an HTTP request. The exact input type is required. The request's HTTP method, query-string values, or arbitrary extra JSON fields do not replace these deliberately mapped fields. Supplied integer ID zero remains zero. Do not use this trusted input option to bypass application validation or let transport input select internal capabilities.

#### Embedding-host composition

When the application embeds Datly directly, its existing component loading/compilation produces the two registrations. The following adds the actual supplied authorization implementation and restricts exposure; it is not a developer-MCP command:

~~~~go
// Imports:
// druntime "github.com/viant/datly/runtime"
// provider "github.com/viant/datly/runtime/handler/provider"
// handler "github.com/viant/xdatly/handler"

registeredCheck.Providers = append(registeredCheck.Providers,
    provider.Static(handler.ValueKey("records.authorizer"), authorizer),
)
runtime, err := druntime.NewRuntime(
    []*druntime.RegisteredComponent{registeredCheck, registeredLookup},
    druntime.WithExposedPackages([]string{"example.com/app/check"}, nil),
)
if err != nil { return err }
~~~~

`registeredCheck` must have the public package scope `example.com/app/check`; `registeredLookup` has `example.com/private/records`. Both remain registered for dependency resolution. Use this **same runtime** for HTTP and MCP adapters, so both consult its exposure policy. Do not build an unfiltered second runtime for one transport. A static Authorizer is shared: supply a concurrency-safe implementation and do not store mutable per-request input on it.

If using a developer MCP server instead of direct embedding, require it to supply the equivalent named-factory/service registration, typed dependency identity and public-package selection. Discover its real tools/schema; this example does not define tool names or a deployment endpoint. Same-package component-level exposure needs an independently supported policy; package inclusion alone cannot distinguish two components in that package.

Verified SQLite/HTTP/native-MCP behavior for this pattern: typed `(tenantId=7,id=0)` reaches the lookup even when the POST URL has conflicting query values; a different tenant with the same ID is denied; a body property named `records.authorizer` cannot replace the registered service; denial retains the exact empty-message body below without private cause text; and the private lookup returns HTTP404, is absent from tool listings, and cannot be called as a public MCP tool even when its component declares tool metadata. Its internal invocation still succeeds.

## Explicit failure body

~~~~go
return &response.Error{
    Code: 401,
    Payload: map[string]any{
        "message": "",
        "error": map[string]any{"code": "DENIED"},
        "violations": []map[string]string{{"field": "id", "message": "not allowed"}},
    },
    Cause: privateCause,
}
~~~~

The empty message is intentional. A wrapper's private error text must not replace it. This emits the public body `{"message":"","error":{"code":"DENIED"},"violations":[{"field":"id","message":"not allowed"}]}`; a typed payload with no `omitempty` on Message is equally valid. Import `response` from `github.com/viant/xdatly/response`. Set `privateCause` to the private application cause, or nil; never put that cause into the public payload.

## Writes and completion

Request Data or narrow DML/Sequencer capabilities per invocation. Validate before mutation; keep original presence separate from resolved identity and classify against authorized Previous according to the chosen policy. Do not allocate every current zero ID. Flush of caller-owned work is not a commit.

Custom orchestration must explicitly preserve sparse identity, validation and transaction semantics. It is not automatically protected by all generated writer phases.

Use outcome-aware finalization for commit-dependent publication. Ordinary output Finalize(ctx) and Finalize(ctx,error) are separate supported contracts, not interchangeable signatures.
