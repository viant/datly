# Conditional component finalization

Use `handler.InjectorFinalizer` when an output conditionally needs another
component. The public SDK depends only on `context`; route authority, provider
composition, binding plans and transaction ownership remain in the runtime.

```go
import (
    "context"
    "github.com/viant/xdatly/handler"
)

type Detail struct { ID int; Name string }

type OrderOutput struct {
    ID int
    IncludeDetails bool
    Details []*Detail `bind:"kind=param,in=Data"`
}

func (o *OrderOutput) Finalize(ctx context.Context, lookup handler.InjectorLookup) error {
    if !o.IncludeDetails {
        return nil
    }
    injector, err := lookup(ctx, handler.Route{
        Method: "GET",
        URL: "/details?limit=10",
        Scope: "example.com/orders",
        Name: "Details",
    })
    if err != nil {
        return err
    }
    return injector.Bind(ctx, o)
}

// The registered Details component uses its ordinary canonical input plan.
type DetailsInput struct {
    ID int `parameter:"ID,kind=caller_output,in=ID,required"`
    Limit int `parameter:"Limit,kind=query,in=limit"`
}

type DetailsOutput struct { Data []*Detail }

var _ handler.InjectorFinalizer = (*OrderOutput)(nil)
```

`caller_output` resolves authored input fields from the calling output using
Bindly's native struct locator. `param` and `input` inside the child still refer
to its canonical input. The returned binder matches the child's typed result
onto destination `bind` tags using native Bindly source binding. There is no
field-copy loop, positional mapping or JSON roundtrip. Existing `output` response
metadata retains its meaning.

Lookup validates the exposed route but does not execute it. The first `Bind`,
or `Lookup(ctx, handler.ResultKey)`, invokes the selected component through the
canonical runtime ComponentInvoker. Subsequent operations on that binder reuse
the same typed result. `ResultKey` returns the complete result; unknown lookup
keys return `(nil, false, nil)`. Result binding exposes data, not the child's
sealed handler service capabilities. Acquire another binder for another call.

The URL is a local absolute path with optional query parameters; external URLs
are rejected. Omit Scope and Name for route-only selection. When naming a target,
supply its exact registered Scope and Name; the route must resolve to that
exposed component. Concrete path values and URL query parameters replace the
parent's URL values. Original request identity and the current protocol header
providers are retained. Headers, JWT verification and other inputs are resolved
only when the child declares them. JWT claims are verified from current headers,
not copied from an earlier output or injected as an ambient principal.

The hook runs once before root completion, after preparation of already-open
locally owned units when there is no handler error. Its child calls are imperative
components in the same recursion guard and unit of work. A parent with no Data
source still owns a neutral root so child writes cannot commit independently.
The engine drains work added by the hook before owned commit. Caller-supplied
transactions remain caller-owned; preparation preserves the existing external
transaction ordering, and completion leaves them pending for their owner.

A handler, lookup, child, binding or finalizer error vetoes owned commit. The
original cause and status-bearing error remain intact; additional errors are
joined. Ignoring a failed lookup/bind does not turn the root into a success.
Cancellation and panics also fail completion. A prepare failure suppresses the
previous output. Hook and binder contexts retain invocation values and accept
call cancellation; transaction-bearing contexts live through root completion.
Callbacks and binders expire when `Finalize` returns. Await every call and never
save these capabilities for background work.

Plain output finalizers remain success hooks after local completion. Nested plain
and MCP success hooks wait for root completion and are skipped on root failure.
ErrorFinalizer remains a once-only pre-completion hook. MCP runs after successful
ordinary finalization. Caller-pending completion is not proof of commit: publish
commit-dependent messages only from an OutcomeFinalizer that checks
`outcome.CommitConfirmed()`. Handlers implementing the existing result-aware
OutcomeFinalizer (including generated mutation programs) retain exclusive
ownership of their output lifecycle; the engine does not additionally call an
output InjectorFinalizer, plain/Error finalizer or MCP hook for those handlers.
