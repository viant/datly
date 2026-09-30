# Scoped writer sequences

A scoped sequence allocates a numeric **non-identity** column independently for
one tuple of entity fields. For example, message IDs remain strings while each
turn numbers its own messages.

```sql
SELECT m.id, m.turn_id, m.sequence,
       type(m, 'Message'),
       CAST(m.sequence AS *int),
       sequence_scope(m.sequence, m.turn_id)
FROM message m
```

Use `datly transcribe patch|post|put`. The annotation is a standalone outer SELECT
annotation; target and scope columns must belong to the same view and appear in
its projection. Multiple scope columns are supported. Generated field metadata
is `sequenceScope:"turn_id"` (comma-separated physical columns for tuples).
Do not edit the generated DTO or implement allocation in application hooks.

The ordinary native identity sequencer and its dialect policy remain separate:
`sequence_strategy` controls identity allocation, not scoped numbering.

## Presence and allocation

- Only INSERT frames with an omitted sequence are eligible. An originally
  supplied value is preserved, including zero and explicit NULL.
- To preserve legacy nullable-sequence contracts, opt in with
  `tag(m.sequence, 'sequenceOnNull:"allocate"')`. Originally supplied NULL then
  qualifies for INSERT allocation; explicit zero remains supplied and UPDATE
  NULL still clears the value. Omission or `preserve` keeps the default. The
  policy requires a nullable target and is independent of presence markers.
- A business hook's already assigned nonzero value is preserved.
- UPDATE and DELETE frames never receive automatic sequence values.
- A nil/empty-string scope is unresolved and leaves the sequence unassigned.
- The invocation groups rows by physical table, target column and scope tuple.
  All supplied numeric values in each tuple are reserved before allocation.
  Values are allocated monotonically from the stored maximum/counter, skipping
  supplied values. A high supplied value does not unnecessarily push the same
  batch's generated values above it.
- Native identity/link allocation completes first. Scoped values are ready before
  `AfterSequence` and Queue; there is no allocation during source INSERT/Flush.
- Targets must be writable integer fields; scope columns are distinct mapped
  string, boolean or integer fields. Primary keys, concurrency tokens and
  auxiliary/read-only views cannot be targets.

## Transactions and storage

SQLX owns the database primitive (`io/sequence`); Datly owns immutable metadata,
framing, presence, grouping and lifecycle integration. SDK contracts acquire no
SQLX dependency. Application hooks do not receive raw database handles.

Counters live in `sqlx_scoped_sequences`, keyed by a digest of physical table,
column and canonical scope bindings. Scope values are SQL parameters, and table
and column identifiers are parsed/quoted. Counters and source writes use the
same root transaction. Rollback restores both; caller-owned transactions are
never committed or rolled back by the allocator.

SQLite acquires write intent before reading MAX/counter state. Its ledger can
be created transactionally on first use. Multiple independent connections
serialize allocation through database locking rather than a process counter.

**MySQL requires provisioning before business transactions.** DDL can implicitly
commit MySQL transactions, so it is not run inside allocation:

```go
err := sequence.Provision(ctx, db, "mysql") // github.com/viant/sqlx/io/sequence
```

Equivalent migration DDL:

```sql
CREATE TABLE sqlx_scoped_sequences (
    scope_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    value BIGINT NOT NULL
) ENGINE=InnoDB;
```

Provision each selected schema used by qualified source tables. The connection
must select that schema when using `Provision`. MySQL locks the tuple's counter
row until transaction completion. Both SQLite and MySQL are supported; other
products fail explicitly rather than falling back to MAX+1 without locking.

Do not reset counters while writers are active. A recreated source table using
an existing ledger retains its high-water marks; dropping source rows is not
implicit authorization to rewind numbering.

## Collision policy

Managed allocators prevent tuple collisions through the counter lock. If a
writer bypassing the allocator takes an automatic value, Datly can replay the
whole invocation after a **known, single-root owned rollback**:

- Typed duplicate-key evidence is required, followed by a SQLX read proving the
  generated scope/value now exists and the requested primary identity does not.
- Only values actually allocated by the native scoped phase qualify; supplied
  sequences and business-hook supplied values are not repaired.
- The original request/Has facts are rebound with fresh Current reads.
- Original INSERT identities are pinned across replay. A newly arriving primary
  identity fails instead of changing the request into UPDATE.
- Initial execution plus at most nine replays (ten attempts total) is allowed.
- Caller-pending, unknown commit/rollback, mixed units and nested reports are not
  replayed. Ordinary user `Recover` hook admission remains unchanged.

## Verification

`sqlx/io/sequence` tests cover independent tuples, supplied values, caller
rollback/commit, injection rejection and independent-connection races on SQLite
and live MySQL. `transcribe/TestGeneratedScopedSequenceSQLite` transcribes a real
writer, executes it on SQLite and optional live MySQL, verifies regeneration,
explicit zero/NULL, updates, empty scope, races, caller transactions, forced
external collision, exhaustion and primary-key winner preservation.

Set `SQLX_SCOPED_MYSQL_DSN` to an authorized **isolated fixture database** for live
checks. Tests create/drop `messages` and reset the fixture ledger; never point
this setting at application data. Generated writer tests remain authoritative
for schema/binding/transaction behavior; parser success alone is insufficient.
