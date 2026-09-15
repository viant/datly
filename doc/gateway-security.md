# Gateway security migration

This release intentionally changes proven unsafe defaults inherited from original
Datly. Explicit public error bodies and transaction ownership remain supported.

## CORS

Original Datly and the earlier 1.0 default combined wildcard origins with
credentials, reflecting every requesting origin. With no CORS configuration,
1.0 now allows cross-origin reads **without credentials**. Browser requests that
need cookies or HTTP authentication require an explicit trusted-origin policy:

```yaml
CORS:
  AllowOrigins: [https://app.example.com]
  AllowMethods: [GET, POST]
  AllowHeaders: [Content-Type, Authorization]
  ExposeHeaders: [Content-Type]
  AllowCredentials: true
```

Replace the example with actual trusted origins. Effective wildcard origins plus
credentials fail handler staging; `AllowCredentials: true` alone cannot upgrade
the wildcard default. Route fields inherit configured global fields; explicit
false/empty values retain their meaning. The original rule restricting a route's
credentialed wildcard to its configured parent origins remains. `CORS: {}` or
`AllowOrigins: []` grants no cross-origin access. `DisableCors: true` disables
route CORS, including explicit route policies. CORS does not authorize requests.

## Diagnostic metrics

Original Datly exposed SQL/arguments for any `Datly-Show-Metrics: debug` header.
The header is now ignored unless the **operator** enables `Metrics`:

```yaml
Metrics: {}                 # SQL-redacted diagnostic headers
# Metrics: {AllowSQL: true} # also allow SQL/arguments for debug requests
```

Omit `Metrics` to disable all metric headers. SQL-redacted diagnostics still
contain execution metadata; enable them only where that disclosure is intended.
Go hosts may supply `MetricsConfig.Authorize func(*http.Request) error`, which
must authorize each request before any diagnostic header is emitted. `AllowSQL`
is an additional gate. Client headers cannot change either setting. Normal
route authentication continues to run; public routes have no inferred admin
authority. OTel `IncludeSQL` is a separate export policy.

## Listener timeouts and streaming

Original listeners had no timeout unless configured. HTTP and standalone MCP now
use `Endpoint.ReadHeaderTimeoutMs: 10000` and `Endpoint.IdleTimeoutMs: 120000` when
absent/zero. Negative values explicitly disable that deadline. Positive values
are preserved and duration overflow is rejected. Idle applies between requests,
not while a response is streaming.

Existing HTTP `ReadTimeoutMs`, `WriteTimeoutMs` and header-size settings retain
their values. No fixed response write deadline is added. Standalone MCP inherits
only the read-header/idle policy; it does not inherit the main HTTP write deadline.
For embedded MCP SSE/streamable servers, use `TransportConfig.ReadHeaderTimeout`
and `IdleTimeout` (`time.Duration`); zero chooses the same defaults and negative
disables. Callers can still configure the returned `http.Server` explicitly.

## Errors and private recovery diagnostics

Binding source conversion and malformed bodies carry native Bindly typed input
errors (400; unsupported named-body media 415). Internal provider failures are
not reclassified merely because their text resembles a client error. Internal
unknown failures return generic 500 responses and discard partial output.
Existing authored status/message policies remain; `${error}` intentionally
interpolates the cause and should only be used when that disclosure is intended.
An intentional `response.Error` keeps its exact public payload, including explicit
null/empty fields and public 500 bodies; its cause remains private.

Recovery boundaries retain `*exec.PanicError`, its `Cause()` and detached `Stack()`.
Use `errors.As` on a returned/wrapped error to inspect it in server diagnostics.
The standard server log records panic values/stacks; ordinary `Error()` and JSON
serialization do not include them. Panicking with `response.Error` is still a
panic and does not publish its payload. Return that error normally to intentionally
publish it. Recovery uses the existing transaction completion path: rollback,
caller-owned pending state and already-completed transactions are not rewritten.

HTTP logs failures with the configured logger, falling back to the standard log.
MCP execution errors retain private causes for native execution diagnostics;
protocol results contain the same intended public error policy, never panic
values/stacks. Restrict access and retention for server diagnostic logs.

Component, async, warmup, document and static API-key checks share the HTTP APIKey
comparison owner, which compares fixed-length SHA-256 digests in constant time.
Configured empty keys never authorize.
