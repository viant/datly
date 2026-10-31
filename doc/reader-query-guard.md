# Native reader query admission

Reader planning and admission share `github.com/viant/xdatly/reader.ParameterizedQuery`:

```go
type ParameterizedQuery struct {
    Query string
    Args  []any
}
```

`reader.QueryGuard` receives `(context.Context, *sql.DB, *ParameterizedQuery)` and
returns an error to deny execution. Query retains SQL placeholders and Args holds
the ordered bound values. Datly's `exec.ReadPlan` name is a deprecated alias;
new code uses the Xdatly type and its `Query` field directly. Async job persistence
retains its existing `Query`/`Args` JSON representation.

A trusted host may attach a concurrency-safe query guard with
`Execution.WithQueryGuard`. Like `WithRecorder`, it returns a detached registered
reader; the source execution and compiled graph remain unchanged.

The guard receives context, the selected source connection, final SQL and a copy
of the bound argument slice. It runs at the native row-read and cache-population boundaries before
SQLX opens a row reader. Root reads, relation batches, partition reads, derived
outputs, prepared projections and warmup retain their existing planning, selector,
cache, row-mapping, hook and transaction semantics. A guard error stops that
statement and propagates through normal reader failure handling.

This capability is appropriate for metadata-only source admission, such as a
BigQuery dry run. It does not rebuild query SQL, bind transport inputs, execute
stored SQL, or replace native readers. The guard must not mutate arguments or
execute the business query. Guard state must be safe when relation or partition
workers run concurrently. Source connections remain owned by the native runtime.

`PrepareQuery` alone does not execute SQL. Its projection executor retains the
guard and applies it to the final composed query when rows are read. A dry-run
plan remains distinct from per-statement admission; relation keys may depend on
parent results, so later statements are admitted using their actual bound keys.

Qualification: `go test ./sql/reader -run TestQueryGuard` uses SQLite to verify
root, partition, relation, composed-projection and warmup admission, rejection propagation
and preservation of the original execution.

## Disabled-path cost

The guard is an opt-in execution decorator. Ordinary Execution, Config and Session
layouts are unchanged; query/argument copies and the guard context are allocated
only when a guard is attached. Admission is checked once per SQL statement, not
per result row. Its context is scoped to the owning compiled component and does
not opt nested components in.

On the local Apple M5 Max qualification, the absent lookup used 0 B/op and
0 allocs/op: about 2 ns with a background context, 11–14 ns at eight context values,
and 81 ns at 32 values. Six alternating before/after runs of the existing
16-row BenchmarkParityReader retained 778 allocs/read and approximately 45.8 KB/read.
Full-reader latency varied substantially with host load; these measurements do
not establish a zero-latency overhead guarantee.
