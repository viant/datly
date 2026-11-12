# Opt-in gateway authorization (master)

The master gateway uses public `github.com/viant/authz` to enforce a named policy
before dispatching HTTP routes. See the [complete YAML example](examples/http-authorization.yaml).
Master uses top-level `Authorization` alongside `Meta`, `RouteURL`, and other
existing gateway settings. It does not use the v1 standalone `HTTP` wrapper.

Without `Authorization`, existing behavior is unchanged. When configured,
`Provider`, `DefaultPolicy`, and valid `Policies` are mandatory. A missing provider,
unknown policy, malformed rule, or conflicting binding rejects router creation.
A failed router rebuild leaves the previously published router in service.

## Selection and coverage

- Exact `Exclude` method/path matches skip this gate, e.g. `GET /healthz`.
  Configure `Meta.StatusURI: /healthz` to serve the health handler there.
- Exact `Overrides` select a named policy; otherwise the existing route matcher
  supplies the canonical template used for template overrides.
- All other requests use `DefaultPolicy`, including unknown paths and OPTIONS.

The gate covers component APIs, status, documentation, configuration, static
content, metrics, warmup and cache administration routed through `Router.Handle`.
Existing subnet restrictions, API keys and business authorization remain active.
Exclusions use the escaped URL path exactly; encoded aliases, children and other
methods are not implicitly excluded. Overrides follow master route semantics.
An override replaces this gate's default, not resource-specific checks.
Separate MCP listeners, background jobs, and direct component dispatch retain
their own authorization owners.

`mode: protected` requires verified subject, issuer, tenant and unexpired facts.
Rules support `subject`, `role`, `group`, `exposure`, `entity`, `all`, `any`, and
`not`; negation takes exactly one child. Required scopes remain mandatory outside
OR branches. A public policy admits without identity lookup; deny rejects without
lookup. Admission cannot enforce returned row scopes, so entity-bounded policies
and persistent named-resource references are rejected. Local policy names are
resolved within the immutable configuration snapshot.

## Trusted identity hooks

Register an application adapter when constructing the gateway:

```go
service, err := gateway.New(ctx,
    gateway.WithConfigURL("gateway.yaml"),
    gateway.WithAuthorizationProviders(map[string]gateway.AuthorizationProvider{
        "userinfo": identity.ResolveRequest,
    }),
)
```

`identity.ResolveRequest` is your trusted function with signature:

```go
func(context.Context, *http.Request) (context.Context, authz.Facts, error)
```

It verifies the credential and obtains authoritative roles. Never accept roles
from client payloads. It can use `authz/oauth.NewHTTPProviders` for a configured
OIDC userinfo endpoint, preserving subject/account verification and source leases.
Bind the bearer using `oauth.WithBearer`; only the provider's verification makes
those facts authoritative. Missing/invalid credentials should return
`gateway.ErrUnauthenticated` or `authz.ErrIdentityDenied`.

Alternatively, invoke a linked component in-process from the same hook:

```go
invoker := gateway.AuthorizationComponentInvoker(ctx)
result, err := invoker.Dispatch(ctx,
    contract.NewPath("GET", "/internal/identity"))
```

The hook receives master’s existing `contract.Dispatcher`, with request header
and query bindings. Native session binding and component execution remain owned
by master. This does not recursively dispatch HTTP and does not consume the
protected endpoint's body. The adapter maps the typed result to `authz.Facts`.
Mark identity-only routes internal in their authored route configuration.
A hook must not retain the invoker beyond the request.

Downstream handlers can access `gateway.AuthorizationFacts(ctx)`, an immutable
prepared fact snapshot, not a business-resource decision. No private identity
package, business role name or service endpoint is embedded in public Datly.

## Errors, reuse and caching

Authentication returns 401, policy rejection 403, unavailable dependencies 503,
deadline expiry 504 and cancellation 408. Native typed HTTP statuses are preserved.
Client errors contain safe status text; wrapped errors retain their causes.
Authorization diagnostics include phase, status, duration and request trace ID,
without credentials or provider payloads.

Router creation validates and copies policy trees and builds indexed binding maps.
Each protected request resolves identity once and prepares fact membership sets
once. Existing authz evaluation validation remains active. There is no new global
identity or decision cache: providers own account isolation, concurrency, leases,
refresh and revocation. Never extend source expiry or serve stale grants after a
failed refresh. Database-backed policies remain the component owner's concern.

The tests use a real local HTTP identity endpoint and signed credentials, then
send HTTP requests through the gateway. They cover role admission/denial,
negation, template overrides, health exceptions, expired credentials, identity
mismatch, provider failure, cancellation, native dispatcher bindings, configuration
loading and snapshot isolation. No production identity service is contacted.

## Local qualification

The focused authorization suite passes against the published authz dependency.
It includes an actual native SQLite reader dispatch and verifies that the hook
leaves the endpoint body unread. The configuration example is loaded by master's
existing YAML loader in a regression test. Routing and component-locator tests
also run independently.

The broader gateway suite has pre-existing DQL metadata/body-binding failures,
and `gateway/router/marshal/json` has a map-marshaling panic. The same failures
were reproduced with the authorization changes removed through a baseline overlay.
They are not counted as passing or as production validation. No deployment or
live identity-provider test has been performed.

Application binaries that retain Datly's generation/translation CLI can register
providers without replacing its startup path:

```go
err := cmd.RunApp(version, os.Args[1:],
    gateway.WithAuthorizationProviders(providers))
```

The optional gateway options are forwarded through the existing command and
standalone builders. Calls without options retain their previous behavior.
