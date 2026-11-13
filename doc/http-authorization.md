# Opt-in HTTP authorization

Datly v1 can apply public `github.com/viant/authz` policies before HTTP dispatch.
Use the [complete standalone example](examples/http-authorization.yaml) as a starting
point. Role names and API paths in that example are fictional. Replace the package
selection and policy bindings with your application's registered components.

Without `Authorization`, existing behavior is unchanged. With it, `Provider`,
`DefaultPolicy`, and valid local policy definitions are required. The default
covers component APIs, status, metrics, documentation, static files, warmup,
cache administration, unknown paths, and unsupported methods. The exact configured
health exclusion avoids identity lookup. Existing API keys, subnet rules, and
business/resource authorization remain active.

Standalone accepts transport settings grouped under `HTTP`, as shown, or its
existing flat transport fields. Mixing the two forms is rejected. When embedding
`application.Manager`, supply the same transport configuration in `application.Build.HTTP`.

## Policy selection

1. An exact method/path in `Exclude` skips only this gate.
2. An exact method/path override selects its named policy.
3. Otherwise the existing router resolves a component route template; an override
   for that canonical template selects its named policy.
4. All other requests receive `DefaultPolicy`.

Overrides bind policy to existing paths; they do not register routes. Exclusions
are exact paths, not prefixes, regexes, or templates. Query strings do not affect
selection. Paths use the configured native routing semantics; aliases are not
implicitly excluded. A duplicate match or an exact override/exclusion conflict
fails staging. Review literal exclusions alongside template overrides deliberately.
Framework endpoints can use exact bindings. New routes inherit the default.

An override replaces this gate's default policy, not other endpoint authorization.
`mode: public` permits admission without retrieving identity; `mode: deny` rejects
without retrieving identity. Protected policies require verified subject, issuer,
tenant, and a live expiry, even when their rule is only a negation.

`not` accepts exactly one child. `all` and `any` combine positive and negative
rules. The example preserves limited-role union semantics: ordinary users can
pass the default, a reviewer can access review APIs, and a user with both limited
roles can access both admitted surfaces. An unrelated administrator role does not
bypass a limited-role restriction. Profile ownership and row/entity permissions
remain the responsibility of the underlying component.

## Registering an identity adapter

Providers are trusted Go bindings, never executable configuration. Register them
on `command.Service.AuthorizationProviders`, `standalone.Options.AuthorizationProviders`,
or `gateway/http.Config.AuthorizationProviders` when using the gateway directly:

```go
service.AuthorizationProviders = map[string]gateway.AuthorizationProvider{
    "userinfo": func(ctx context.Context, req *http.Request) (context.Context, authz.Facts, error) {
        // The host binds and verifies req's credentials using its existing
        // identity integration. Only that integration can issue these facts.
        return identity.ResolveRequest(ctx, req)
    },
}
```

Here `identity.ResolveRequest` is an application-owned adapter, not a Datly API.
It may return a derived context containing an existing identity scope. Downstream
handlers can access the immutable verified snapshot with
`gateway.AuthorizationFacts(ctx)`; this is not an entity-access decision.
Do not accept role lists or tenant selection from unverified request payloads.

Return `gateway.ErrUnauthenticated` for missing/invalid credentials. Policy denial
maps to 403, authentication to 401, unavailable providers to 503, deadline to 504,
and cancellation to 408. Error bodies contain safe HTTP status text; existing
request diagnostics receive the original cause. Provider implementations must
preserve cancellation and dependency error identity.

## Lifetime, caching, and boundaries

The application generation owns copied rule trees, indexed override/exclusion
maps, and the selected provider binding. Failed staging does not activate the
replacement generation. Each protected request resolves identity once and builds
one prepared fact snapshot. No additional identity or decision cache is introduced.
Providers own cache isolation, source leases, role revocation, and concurrency;
never extend an identity source's lease or serve expired grants after refresh fails.
The authz evaluator retains its own runtime rule validation.

These are HTTP admission policies. Entity-bounded policies and persistent named
resource references are rejected here because admission cannot enforce their
returned row scope or revision fences. Local names in `Policies` are resolved
within the frozen configuration. Existing resource-level authz integrations remain
independent. MCP listeners retain their own authorization boundary; this gate does
not claim to protect a separate MCP server or other services.

CORS preflight is subject to the gate too. A browser integration must explicitly
choose any required OPTIONS exclusions; no implicit unauthenticated preflight is
added. Protecting all static files likewise requires the client to have a usable
credential before fetching those files.

## Validation evidence

The HTTP tests cover protected status and unknown routes, exact exclusions,
method/encoded-path differences, parameterized overrides, downstream business
checks, provider errors, configuration snapshots, positive/negative role rules,
and failed standalone reloads. The complete YAML example uses the native loader.
On one Apple M5 Max run, the full status-handler benchmark measured approximately
3.1 microseconds disabled and 7.7 microseconds protected with a local fixture
provider (14 versus 24 allocations). This measures HTTP handling and serialization,
not network identity latency, database retrieval, or deployed service behavior.

## In-process identity components

The same hook can bind a linked component instead of making an HTTP call:

```go
invoker := gateway.AuthorizationComponentInvoker(ctx)
result, err := invoker.InvokeComponent(ctx, exec.ComponentRequest{
    Target: exec.ComponentTarget{
        Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app/identity", Name: "Identity"},
        Route: spec.RouteRef{Method: "GET", Path: "/internal/identity"},
    },
})
```

The invoker is bound to the current request generation and supplies native header
and query providers. A component can declare a verified JWT input and ordinary
component/context dependencies; binding, validation and database access stay with
those native owners. The hook maps the typed result to `authz.Facts`. Mark an
identity-only component internal to omit it from public HTTP/MCP catalogs.
Invocation does not recursively enter HTTP dispatch, and it does not consume the
protected endpoint's request body. Body-dependent business authorization remains
with that endpoint. External identity providers continue to use the same hook.
