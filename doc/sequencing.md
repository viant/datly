# Numeric sequencing strategies

Datly delegates allocation to SQLX. It does not choose an allocator from the
presence of a transaction. Stable IDs and links are established before Queue;
supplied-ID exclusion, sparse presence, defaults and invariants keep their
existing lifecycle. SQLX owns physical identity, integer bounds and allocation.

## DQL setting and defaults

```dql
#setting($_ = $sequence_strategy('transient'))
```

The canonical component setting accepts exactly one quoted `transient` or
`reservation` value, once, without modifiers. Unsupported values fail during
transcription. Omit the setting to use the native dialect default:

| Dialect | Absent setting / default | Explicit `transient` | Explicit `reservation` |
| --- | --- | --- | --- |
| MySQL | Original SQLX transient transaction mechanism | Original mechanism, unchanged | Optional provisioned allocator table |
| PostgreSQL 10+ | Exact native nextval values | Rejected: the original scalar-range path cannot represent native batches | Exact native nextval values |
| SQLite | Native transactional reservation | SQLX SQLite next-value implementation | Native transactional reservation |

`maxid` is not a supported managed-allocation setting. No process-local maximum
or implicit switch to the optional table is used to make a request succeed.
PostgreSQL 9 does not acquire PostgreSQL 10 sequence/identity capabilities.

The setting survives generated component tags and bootstrap into canonical
runtime settings. It configures the existing Data source, preserving its DB and
caller transaction identity. Before opening root Data, the source resolves and
freezes the version-matched SQLX dialect default. Children without a setting
inherit it; a child explicitly naming that same native default also succeeds and
reuses the root unit. An explicit differing child policy fails rather than
replacing an already-open allocator or creating another transaction/capability.
Declare shared policy at the root component.

Go applications can use `dml.Source{DB: db, SequenceStrategy:
dialect.PresetIDWithReservation}` or `dml.WithSequenceStrategy(...)` on NewData.
Canonical DQL settings override that source's configured strategy at the root.

## MySQL primary mechanism: original transient transaction

MySQL defaults to `PresetIDWithTransientTransaction`, matching original Datly's
sequencer. The implementation in `transient.go`, `handler.go` and `udf.go` is
byte-for-byte unchanged from original SQLX `24e180f`.

SQLX opens its own transaction/connection, uses its original advisory locking,
executes transient source INSERTs to advance AUTO_INCREMENT and rolls its own
transaction back. The value-list adapter invokes the original NextSequence path
and translates its genuine returned range; it does not replace the algorithm.
No extra allocator table is required or provisioned by the default path. Source
AUTO_INCREMENT advancement survives rollback and is visible to normal inserts.

The original constraints are part of this contract:

- A caller transaction remains caller-owned, but allocation runs on a separate
  connection. A one-connection pool already occupied by the caller cannot
  allocate; it waits until the allocation context expires. Pool sizing must
  allow allocator connections in addition to active caller transactions.
  With go-sql-driver/mysql, cancellation can invalidate the caller connection
  and transaction. Caller ownership means Datly did not commit or roll it back;
  it does not promise that a driver-cancelled transaction remains usable.
- The allocator can wait on locks held by its caller. An idle caller transaction
  with a spare connection works; caller-held source locks can cause a dependency
  cycle. Cancelling that wait may likewise invalidate the caller transaction.
  Datly does not switch strategies or restart the caller to hide it.
- Transient source INSERTs execute database defaults and triggers and must
  satisfy applicable constraints. Transactional source rows roll back, but
  nontransactional/external trigger effects need not. Use suitable transactional
  tables and a viable sample row; do not treat this as a side-effect-free probe.
- Original FK session toggling, cleanup, retries, lock-name construction and
  parameter-position assumptions remain unchanged. The original implementation
  treats the last bound argument as the transient identity. Composite/multiple
  identity shapes must satisfy that assumption; field selection alone cannot
  repair it. Advisory lock names also retain MySQL's length restrictions.

These are observed/documented limits, not repaired or optimized behavior.

## Explicit MySQL allocator-table option

```dql
#setting($_ = $sequence_strategy('reservation'))
```

Provision `sqlx_allocator.sequence_reservations` with native
`sequence.Store.Install(ctx, db)` outside writer transactions, using deployment
credentials. Allocation then locks the native metadata row, reads the current
source maximum and engine AUTO_INCREMENT metadata, applies increment/offset and
bounds, and advances the counter in the caller transaction. It needs no second
caller connection and executes no transient source INSERT or source ALTER.
Runtime permissions must cover the native metadata table and source access.

This option is never silently selected. Its transactional reservation can be
released by rollback. Unmanaged implicit source AUTO_INCREMENT generators do not
consult its metadata and must not be mixed with outstanding preallocated IDs.
Explicit supplied IDs retain ordinary database conflicts and SQL-mode rules.

## PostgreSQL and SQLite values

PostgreSQL native reservation returns each actual nextval result, including
cached/interleaved gaps and descending values. No contiguous range, MAX or
setval is substituted. SERIAL, identity and supported default/explicit sequence
authorities are resolved natively; invalid/non-generated/non-integer shapes are
diagnosed. The scalar range API cannot represent PostgreSQL batches. Sequence
values consumed before rollback/cancellation are not reclaimed.

SQLite retains its transaction-locked native reservation table and
AUTOINCREMENT coordination. Repeated allocations before Queue consume distinct
database ranges. Its single-writer rule serializes transactions across processes.
A stale caller snapshot can fail on upgrade; the caller must handle that outcome.
The implementation does not use MAX alone as a reservation.

Native value lists preserve composite field authority through the SQLX mapper.
Datly's zero sentinel and supplied values remain distinct. Neither allocation
nor completion unexpectedly commits/rolls back a caller transaction; normal
server/driver cancellation and transaction-abort behavior still applies.
