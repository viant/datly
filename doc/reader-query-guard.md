# Native reader query admission

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
