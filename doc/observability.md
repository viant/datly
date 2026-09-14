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
does not start printing diagnostics to stdout.

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
