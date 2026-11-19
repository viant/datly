# Caller-dependent contracts in v0

The application API is the same as v1. A linked component input, output, domain
view, or nested composite implements:

```go
Contract(context.Context, bindly.Binder) (*contract.Policy, error)
```

Imports are `github.com/viant/bindly` and `github.com/viant/xdatly/contract` in both
versions. V0 adapts its existing session locators to this narrow interface. It does
not replace the v0 handler Binder or migrate ordinary input/output binding.

## Application example

```go
package campaign

import (
    "context"
    "fmt"
    "reflect"

    "github.com/viant/bindly"
    "github.com/viant/xdatly/contract"
)

type Campaign struct {
    Name   string  `json:"name" validate:"required"`
    Margin float64 `json:"margin"`
    State  int     `json:"state" choice:"0:draft" choice:"10:active"`
}

type Authorization struct {
    CanViewMargin bool
    Role          string
}

func (*Campaign) Contract(ctx context.Context, binder bindly.Binder) (*contract.Policy, error) {
    // The application chooses the state type and fields. Auth can instead be
    // verified JWT claims through the configured JwtClaim codec, or other
    // native locator/component data. Binding does not call this state's Init.
    var state struct {
        Auth   *Authorization `parameter:",kind=component,in=GET:/auth/context,required=true"`
        Method string         `parameter:",kind=contract,in=method"`
        Debug  bool           `parameter:",kind=header,in=X-Debug"`
    }
    if err := binder.Bind(ctx, &state); err != nil {
        return nil, err
    }
    if state.Auth == nil {
        return nil, fmt.Errorf("authorization context required")
    }
    // Compute permissions once. The name describes equivalent metadata edits,
    // not a caller identity. Include every decision that affects the result.
    showMargin := state.Auth.CanViewMargin
    creating := state.Method == "POST"
    name := fmt.Sprintf("campaign:%s:margin=%t:create=%t", state.Auth.Role, showMargin, creating)
    return &contract.Policy{Name: name, Field: func(parentPath string, parentType reflect.Type, field *contract.Field) (*contract.Field, error) {
        if parentType != reflect.TypeFor[Campaign]() {
            return nil, nil
        }
        switch field.Name {
        case "Margin":
            if !showMargin {
                return field.Hide(), nil
            }
        case "Name":
            if creating {
                return field.Required(), nil
            }
            return field.Optional(), nil
        case "State":
            return field.SetTag("enumAccept", "literal").SetTag("enumStyle", "ordinal"), nil
        }
        return nil, nil
    }}, nil
}

type Input struct {
    Body []*Campaign `parameter:",kind=body"`
}

type Output struct {
    Data []*Campaign `json:"data"`
}
```

The native dispatcher owns the `/auth/context` component's resolution and execution.
The target business handler does not run during schema discovery. JWT codecs use
the session's configured verifier; raw JWT decoding is not authorization.
`kind=contract` method/path values come from the registered target route, not the
outer OpenAPI GET or MCP POST request. Metadata-state bindings cannot replace them.
Unsupported Bindly options return an error rather than being silently ignored.
The native HTTP MCP host copies current transport headers into request scope;
custom hosts can use `gateway.WithMCPHeaders`. The server-authenticated token keeps
precedence over raw headers. Framing and content-type headers are not forwarded.

## DQL and named operations

V0 can express exact method mappings in its existing leading JSON settings header:

```sql
/* {
  "URI": "/campaigns",
  "Method": "POST,PATCH",
  "MCPRoutes": {
    "POST:/campaigns": {"Name": "CreateCampaign", "Description": "Create campaigns"},
    "PATCH:/campaigns": {"Name": "PatchCampaign", "Description": "Patch campaigns"}
  }
} */
```

Keep the component's existing input/output/handler declarations and executable DQL.
The fragment above is route configuration, not a complete writer implementation.
Only declared method/path targets can be mapped. Tool names must be distinct.
Mappings do not invent HTTP methods, change writer insert/update policy, or derive
operation authority from client arguments. V1 has its native `$mcp_route` directive;
the Go `Contract` source above is unchanged between versions.

For PATCH, `Optional()` affects exposed requiredness. Entity `validate` tags and
native presence-aware insert/update validation remain owned by the existing writer.
All schema fields are optional unless explicitly marked required. Optional and
nullable are independent; a scalar does not become nullable merely by being optional.
OpenAPI path parameters retain the specification's required-presence rule.

## Representation and composition

Only field names and tags can change. The linked storage layout, field types,
positions, offsets, embedding and methods remain intact. Hidden fields still exist
in application state: input decoding ignores them and output encoding omits them.
`Hide()` sets `json:"-"`, `internal:"true"` and `mcp:"-"`.

Traversal uses `viant/x/shape` and the shared Xdatly contract. Nested hooks are
found automatically through pointers, slices, arrays, map values, and embeddings.
Nested policies run first; parent policies have final authority. Hooks use fresh metadata receivers, not populated business instances; bind state
explicitly. Presence-holder
fields are infrastructure and are not passed to business field policies.

A nil policy or no effective field edits keeps the original linked path. Nil from
`Field` retains the field and continues traversal. `field.SkipChildren()` skips
descendants only for that policy; nested owners and other parent policies remain
independent. Tag helpers replace/add values; `RemoveTag` explicitly removes them.

## Consumers and codecs

HTTP binding, JSON output, MCP discovery/invocation, OpenAPI and view metadata use
request-local metadata. Registered components, shared schemas and views are not
rewritten. OpenAPI/view metadata responses use `Cache-Control: private, no-store`.
Dynamic MCP registrations contain only a conservative schema shell; caller discovery
uses `Registry.ListToolsFor(ctx)` and never falls back to the static inventory on errors. View metadata filters hidden columns/relations and includes an
`EffectiveSchema` when the view contract changes. Sub-view query selectors keep
master's existing canonical HTTP binding behavior; see [MCP selectors](mcp-query-selectors.md).

The default `content.StructologyJSONRuntime` retains native format settings,
exclusions and filters. `content.StandardJSONRuntime` uses standard JSON names,
omission and time semantics; native case/date/global-omitempty transforms do not
apply. It maintains native presence holders through opt-in standard decoder tracking.
Both engines keep linked decoder destinations and encoder values.

Configured engines/codecs can implement shared `contract.JSONEncoder` and
`contract.JSONDecoder` capabilities. Unsupported configured codecs, legacy JSON
interceptors and non-JSON dynamic formats fail explicitly. Raw handler responses
cannot apply dynamic output metadata. Complete custom JSON/Text methods own their
representation: edits inside those owners are unsupported, while hiding the whole
field or changing an ordinary wrapper is supported. Unchanged recursive types work;
recursive edits remain unsupported by the shared walker.

## Enum wire forms

Repeated `choice:"draft" choice:"active"` declarations use zero-based ordinals.
`choice:"0:draft" choice:"10:active"` supplies explicit ordinals. Both formats work.
`enumAccept:"literal"` controls input JSON/schema and `enumStyle:"ordinal"` controls
output JSON/schema independently. Each defaults from Go storage: integer means
ordinal, string means literal. Neither annotation accepts both wire forms. MCP checks the declared JSON form
before URL encoding; explicit empty literals and bare scalar JSON bodies are preserved.
Storage can differ from either wire type. Unknown cross-form conversions fail;
ordinary choices do not install implicit membership validation.

For the example above, input is `{"state":"active"}`, Go storage is integer `10`,
and output is `{"state":10}`. A policy can change either annotation and the schema
and configured codecs use the same effective tags. Large explicit integer ordinals
retain their exact values through the shared Tagly enum plan.

## Cache and validation

Each component generation owns bounded caches, partitioned by registered method,
path, direction and original type. Policy names include nested owner paths. Policy
selection still runs for each request; only immutable metadata is cached. Empty
names disable variant retention. An MCP invocation shares its selected policy with
its routed HTTP handler within that invocation, without caching authentication.

Tests cover native JWT verification and component-backed state, linked/no-op reuse,
nested-first composition, per-policy pruning, map/slice composites, multilevel
method-bearing embeddings, enum wire changes, configured codecs, presence tracking,
method-specific MCP discovery/calls, OpenAPI, view metadata and unchanged catalogs.
Benchmarks are in `service/contract/benchmark_test.go`; run with `-benchmem`.

See [validation details](dynamic-contract-v0-validation.md) for dependency revisions, test scope and baseline failures.
