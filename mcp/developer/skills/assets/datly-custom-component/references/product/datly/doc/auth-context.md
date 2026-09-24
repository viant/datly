# Typed authentication context for Datly components

This is the required Datly 1.0 authoring contract for components that need a
remote authorization context. Ordinary HTTP/MCP component handlers are in
`runtime/handler/remote`; shared configuration and mapping live in
`runtime/remote`. Their provider and dependency integration suites pass.
Full release verification remains a separate gate.

## One explicit typed path

Verify a request JWT against the configured issuer/key source and bind its
claims through an explicitly declared input. Decoding a token or accepting a
client-supplied principal is not verification. Keep the existing, supported
`JWTValidator`/`JwtClaim` input and predicate examples in the skill references
when they apply; there are no ambient claims or name-based auth exceptions.

Declare the authorization context as an ordinary typed component/context input
dependency. Its output can be a registered application type such as
`auth.Context` with `userId`, `tenant`, `roles`, `exposures`, `validUntil` and
`allowedEntities` keyed by entity type, for example
`{"project":[101,102],"organization":["north"]}`. The dependency uses the
normal typed DI and lifecycle path. There is no `CaptureScopeBinding`, second
per-version auth registry, or special runtime behavior for a component named
"auth". Missing or empty IDs grant no rows; never interpret them as all IDs.

When the dependency returns an envelope with a `Context` field, declare the
component dependency as `Auth`, then explicitly derive `Context` from
`param/Auth.Context`. SQL can consume the bound context directly:

```sql
WHERE $criteria.In("t.project_id", $Context.Allowed.Entity)
```

Here `Allowed.Entity` is an explicitly declared typed ID slice on the
application's context contract, not a special runtime field. `criteria.In`
collects bound SQL arguments and produces a false condition for an empty list.
No custom predicate implementation is required for ordinary membership gating. A
registered predicate remains an option for more complex policy expressions.
The component route, output type, selector, and SQL gate must all be
declared; the field name `Auth` itself has no special meaning. This follows the
same dependency-and-predicate model as a local ACL component, regardless of
whether that component calls HTTP, MCP, or another native component.

Predicates consume fields from that declared typed input and return trusted SQL
structure with ordered bound arguments. Required authorization stays outside
optional OR filters. Reject missing, expired or denied context before protected
reads or writes; never interpolate IDs or claim text into SQL. Application DB
work belongs in generated Datly v1 readers/writers; custom components compose
them rather than adding a direct-SQL persistence path.

## Remote provider configuration contract

For an HTTP or MCP context provider, use a complex typed instance constant that
declares the destination URL, protocol, request/response mappings and outgoing
headers. Bind the incoming credential/header as an ordinary explicit component
input, then map it to the configured outgoing header. The default incoming
header name may be `Authorization`, but deployment configuration can choose
another valid header name. Nothing forwards a header implicitly, exchanges a
token, or refreshes credentials automatically. A user payload must not select
provider URLs or header authority.

Use one typed constant, `github.com/viant/datly/runtime/remote.Config`, bound with
`parameter:"Remote,kind=const,in=Remote"`. Bundle client configuration, request
mappings, response mappings and optional cache policy in that value.
The handler configuration explicitly selects exactly one of HTTP or MCP and
carries that protocol's typed options. Both or neither must fail validation.
Caller credentials are forwarded by
`RequestMapping{Input: "Token", Header: "Authorization"}`, not by the
server-resource `Credential` option. Response extraction uses JSON Pointer;
XML/XPath is not implemented.

Remote calls obey the same `xdatly/handler.Contract[I,O]` and handler DI injection
as user-defined handlers. `remote.Handler` receives an ordinary typed
input, selects the configured transport, and calls the shared mapper from `Exec`. There is no field-discovery
layer, privileged registration hook, or secondary binder. Static handler
dependencies bind once at registration; caller/config inputs bind per request. See
[ordinary remote handlers](remote-handlers.md) for the concrete contracts.

Inject the selected `xdatly/client/http.Provider` or
`xdatly/client/mcp.Provider` into the handler through native runtime DI.
Do not require both. Each public contract exposes `Client(context.Context,
Options)` returning its protocol-specific client: HTTP uses `Do(*http.Request)`
and returns an HTTP response; MCP uses `CallTool` with explicit call headers
and returns the native protocol result. Do not flatten them into a shared
request/response union. Provider
implementations are statically wired by the runtime; the declared constant
determines the configured client. Request payloads never choose implementations.
The runtime's composition layer owns provider/client shutdown; handlers borrow
them and never create an implicit registry or close an injected client. HTTP
callers still close individual response bodies, as required by the HTTP API.
Concrete HTTP/MCP implementations belong in Datly's internal client packages;
MCP uses `github.com/viant/mcp`.

The verified binding kinds are `http_client` and `mcp_client`. Declare only the
selected provider, for example:

```go
HTTP xhttp.Provider `bind:"kind=http_client,required"`
```

`runtime.NewRuntime` supplies default providers centrally and owns their
shutdown. `runtime.WithClientProviders(http, mcp)` supplies borrowed alternatives;
pass nil for an unavailable protocol. Per-component providers can replace a
default explicitly. The selected capability must be present; missing DI never
creates a client inside the handler. The dependency-to-`criteria.In` runtime
path has regression tests, including nested context pointers and caller override
attempts; rerun them after changing provider wiring.

Zero or omitted timeout, response-size, and session-count limits mean unlimited.
Positive values are enforced; negative values fail. Unlimited timeout still
honors caller cancellation. Caching remains opt-in with a named injected backend,
an explicit positive TTL, and an optional validity cap. Backend capacity is
configured when the backend is registered.

A configured five-minute cache may reuse a successful context only within the
same verified principal, tenant, request and provider configuration. Cap its
entry lifetime at `validUntil`; do not cache errors or denials. Verify cache
isolation, expiry, cancellation, wrong-header and empty-scope behavior on both
HTTP and MCP paths. One typed contract and normal dependencies make the
authorization path inspectable, give both protocols the same behavior, and
keep SQL binding native and safe.

Use the connected implementation as authority for registration and mapping
options. If a required capability is missing, report it rather than inventing
syntax or substituting a parallel auth framework.
