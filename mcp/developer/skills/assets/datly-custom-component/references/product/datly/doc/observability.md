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


> Packaging boundary: Exact maintained author-facing section: includes declarative configuration and behavior; excludes repository navigation, implementation/test evidence and unrelated authoring workflows. Other feature contracts remain in the canonical skill references.
