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


> Packaging boundary: Exact maintained author-facing section: includes declarative configuration and behavior; excludes repository navigation, implementation/test evidence and unrelated authoring workflows. Other feature contracts remain in the canonical skill references.
