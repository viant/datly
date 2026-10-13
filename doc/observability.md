# Native execution capture and optional OpenTelemetry

[All guides](README.md) · [Cache and warmup](cache-and-warmup.md)

Datly captures execution through its native SDK context and recorder. Optional
OpenTelemetry export consumes completed native records; it does not replace the
execution model or create an SDK span for every hot-path operation.

## Enable only the export you need

Native capture is present without OTel. Supply logging/reading policy and optional
export through `application.WithObservability(runtime.ObservabilityConfig{...})`
for a Manager, or `runtime.WithObservability` for an independently owned Runtime.
One Manager owns its observation/export lifetime across reloads. Stage-local
observation configuration is rejected rather than creating one exporter per
published generation.

Embedding fragment, with an application-supplied `sdktrace.SpanExporter`:

```go
option := application.WithObservability(runtime.ObservabilityConfig{
    OTel: &otel.Config{
        Enabled: true,
        ServiceName: "records-api",
        QueueSize: 128,
        BatchSize: 16,
        BatchTimeout: time.Second,
        ExportTimeout: 5 * time.Second,
        MaxSpans: 1024,
        Exporter: exporter,
    },
})
// Pass option to application.New with your canonical type catalog.
```

Imports: `application` and `runtime` from `github.com/viant/datly`,
`otel` from `github.com/viant/datly/observability/otel`, and `time`.
This configures Datly's adapter; exporter credentials/endpoints remain the
application's responsibility. No global OTel SDK registration or automatic OTLP
environment setup is implied. Standalone `Observation` supplies native logging and optional OTLP/HTTP wiring;
see [services](../standalone/SERVICES.md) for exact configuration fields.

## Capture, export and privacy

Completed native timing and parent relationships are projected into spans. The
adapter detaches a snapshot, bounds queue admission, and exports in an
application-owned worker. Queue overflow drops export work and increments
statistics; it does not delay a business request waiting for a telemetry service.
Exporter failures remain observable as export failures.

`IncludeSQL` is opt-in because authored SQL itself can contain sensitive literals.
Arguments, request headers/bodies and raw error messages are not exported by this
adapter. Native diagnostic capture is a separate surface with its own policy;
do not assume export filtering erases all local capture. No logger means capture
does not start printing capture summaries to stdout. Recovered panics are an
exception: recovery records the private cause and stack in the standard server
log (stderr by default), even without a configured invocation logger. HTTP
failures also fall back to that log when no HTTP logger is supplied.

HTTP `Datly-Show-Metrics` headers have no effect unless the operator configures
`Metrics`. `Metrics: {}` enables SQL-redacted headers; SQL and bound arguments
require `Metrics: {AllowSQL: true}` and a `debug` request. Embedders can additionally
set `MetricsConfig.Authorize` to restrict each caller. This gate is independent of
OTel `IncludeSQL`; clients cannot enable it. Enable SQL diagnostics only on
trusted routes/deployments: arguments and authored literals may be confidential.

Use `ExportStats` to inspect accepted, dropped, exported and failed counts.
Incomplete/invalid native records can fail export without changing the business
result. Trusted trace links may be supplied for cross-invocation correlation;
do not derive trace identity from job IDs, credentials or request data.

## Lifetime

Stop admission before shutdown. Accepted work completes native capture, then the
owner drains its exporter. Async notification and AFS post-processing finish
before exporter shutdown. A caller deadline does not detach cleanup; subsequent
shutdown callers join the same completion. Externally supplied services must
remain valid until their owning lifetime has drained.

## Performance claims and measurement

No throughput, latency, memory-capacity or “zero overhead” claim is made. Capture,
snapshotting, queueing, SQL hooks, caches, drivers and exporters all have costs
that depend on workload. The repository contains [adapter microbenchmarks](../observability/otel/benchmark_test.go);
they do not establish end-to-end API performance.

Measure cold/warm cache paths, projection width, row counts, relation batches,
partition concurrency, SQL latency and export queue pressure separately. Compare
capture-only, export-enabled and exporter-failure runs under the same data and
configuration. Include errors and cancellation. For grouped warmup, check
the [backend-specific acceptance](cache-and-warmup.md#full-projection-and-narrower-regular-requests)
before presenting a performance comparison as working cache reuse.

## Optional compatibility audit and trace profile

Standalone configuration accepts one optional root `Logging` profile:

```json
{"Logging":{"EnableAudit":true,"EnableTracing":false,"IncludeSQL":false}}
```

Omitting `Logging` preserves existing v1 behavior. An empty profile enables audit,
disables tracing and hides SQL. Each flag is optional; explicit false is retained.
Embedded applications select the same profile through
`runtime.ObservabilityConfig{Logging: &observability.Logging{...}}` and the existing
application/runtime options. The owner copies configuration once and survives
reloads. There are no public completion callbacks or request-selected log sinks.

Audit and trace records use the original stdout destination and are serialized
synchronously at the outer HTTP boundary, including early transport failures.
The committed HTTP status and observed write/encoding failures are retained;
handler return does not prove delivery to the client. Verified identity observation
copies only user ID, username, email and scope after JWT policy checks. Conflicting
nested identities suppress attribution. This observation never authorizes a
request. Native body metrics, HTTP metric headers and OTel remain separately
configured. SQL diagnostic formatting is being reconciled independently; enabling
this profile is not a claim of complete legacy observability parity.

### Server-side read failure diagnostics

The native invocation logger emits `datly SQL read failed` at ERROR with a
redacted `error` reason, `reqTraceId`, `view`, `rows`, and, when present,
`executionId`, `parentId`, and `elapsed`. View completion (`datly view read`)
also uses ERROR for failures and includes the redacted reason and `metricId`.
This covers preparation failures that have no SQL execution record. Successful
view completions remain INFO. An absent invocation logger does not emit these
native diagnostics or introduce a stdout fallback.

Available cache evidence is attached to the SQL failure itself:
`cache_type`, `cache_namespace`, `cache_set`, `cache_records`, `found_warmup`,
`found_lazy`, `cache_error_type`, and `cache_error_code`. Cache reads with an
error type or code also emit `datly cache read` at WARN. These fields describe
the captured evidence; a cache error or a cancellation does not by itself prove
the primary failure. Cache records do not currently carry a separate error
message. No cache keys, HTTP headers/bodies, or bound arguments are added to
these diagnostics.

For example (illustrative, not a production diagnosis):

```json
{"level":"ERROR","msg":"datly SQL read failed","reqTraceId":"example-trace","view":"campaign_flight","executionId":"example-execution","parentId":"example-parent","rows":0,"elapsed":"3ms","error":"failed to run query: [REDACTED], due to Error 1142 (42000): SELECT command denied to user 'reader' for table 'campaign_flight'"}
```

The optional compatibility `Logging` sink retains its stdout destination and
record prefixes, with the same redacted reasons and correlation/timing fields.
This intentionally changes legacy SQL diagnostic formatting: `IncludeSQL`
now controls SQL on the error line, and only parameterized text is emitted.
Expanded SQL, positional parameters, and cache keys are no longer printed on
these lines, including when `IncludeSQL` is true. The native invocation logger
always omits SQL. Audit/trace SQL selection, metric-header policy, OTel export,
and public HTTP/MCP error sanitization remain separately controlled.

Redaction removes known SQLX query/scan wrappers and query text, labelled
SQL/argument/HTTP payloads, credential assignments, bearer/basic tokens, JWTs,
URL credentials and MySQL DSN credentials. CR/LF are escaped in diagnostic
values to prevent extra records. Original metrics, arguments and cache evidence
are not modified. Redaction cannot infer an arbitrary unlabelled secret in a
custom error or SQL literal; keep custom error messages free of request
payloads and credentials, and enable SQL only for trusted operator diagnostics.


### Optional HTTP execution duration

HTTP `ServiceTimeHeader` is empty by default, so no duration header is generated.
Set it to `Datly-Service-Time` or a dedicated custom header name to opt in.
The value uses Go duration syntax. Measurement starts after routing and output
format checks and ends after execution/error processing, before final response
encoding and transfer. Earlier rejected requests do not acquire a timing header.
This policy is independent of logging and diagnostic metrics. Names must be valid
HTTP field names; choose a dedicated name, not a transport, cookie, CORS or
`Datly-Metrics-*` header. Application response headers retain existing precedence.
