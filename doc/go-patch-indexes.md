# Generated Go PATCH identity and read indexes

Generated Go PATCH keeps client facts, initialized identity, and operation
selection separate. `Input.Init` may resolve an identity through already bound,
authorized reads. The initialized tuple is matched against a detached Previous
index; an ID value alone does not authorize an UPDATE. A missing match follows
the compiled missing-row policy.

Nonzero scalars and non-nil pointers (including pointers to zero) are candidates
regardless of their request marker. An absent default scalar zero needs explicit
presence, for example a generated setter. `state.Original` retains client values
and presence. Established key parts are frozen during frame preparation after
Input.Init/InitMCP, before entity Init and sequencing, including partial composite
keys. Pending allocator/parent parts retain their producer
rules; later values cannot select another Previous row. Scope, duplicate, final
reference validation, and transaction checks still apply.

## Automatic indexes

The standard generated input owns `PrepareReadIndexes(ctx)` and
`ReadIndexes(ctx)`. Capture prepares detached collections once per invocation,
before `Input.Init`. The same cached collection set is available in entity
`Validate` through ordinary input binding. The input package owns the support,
including when input, entities, and handler have separate destinations.

Automatic maps are limited to:

- Keyed maps for the canonical read identity (or the declared read primary key
  for an independent auxiliary read).
- Grouped maps for complete parent/child equality tuples from canonical relation
  and self-link metadata. A composite link is one tuple, not independently
  inferred groups for each part.

Field spelling, `Id` suffixes, and application source references do not select
automatic groups. Names, notes, dates, and other business fields are not grouped
unless they are actually part of a canonical link.

```go
reads, err := input.ReadIndexes(ctx)
if err != nil {
    return err
}

// Automatically prepared from the declared identity and relation links.
exists := reads.CurrentItemsById.Has(10)
children := reads.CurrentItemsGroupedByOrderId[1]

// Optional business work is explicit and typed.
byName := reads.CurrentItems.GroupByName()
uniqueNames, err := reads.CurrentItems.IndexByName()
```

Keyed constructors reject duplicate keys. Grouped constructors retain every row
with a valid key, including zero values; null key parts are not folded into zero.
All generated map types provide `Has`. Composite identity and relationship keys
have generated comparable struct types. Applications can keep constructor
results in their invocation-scoped lifecycle object when repeated use warrants
it. There is no process-global business-group cache.

The former automatic `Current…GroupedByName`/date/note fields are deliberately
not emitted when unrelated to canonical links. Use the corresponding typed
`GroupBy…()` constructor. Removing the field makes the opt-in explicit at
compile time rather than returning a misleading nil map.

## Evidence, authority, and cost

Every declared read field still requires actual loaded-field evidence before
preparation succeeds, even if it has no automatically prepared map or cannot be
a map key. An unused auxiliary field with missing evidence is an error; unloaded
defaults are never indexed. Failed preparation clears the invocation cache.

Public rows/maps are detached helpers. Mutating them cannot change the canonical
Previous snapshot or the operation selected from it. Normal business validation
is not moved, and Input.Init is not repeated.

Preparation still pays for evidence validation and cloning, plus the selected
key/link indexes. Optional constructors add only the work explicitly requested
by the application. Parent-scoped discovery can load more rows than a request-ID
lookup. Allocation measurements are not retained heap or end-to-end request
latency; use representative row counts, field widths, and cardinalities.

Named field comparability is resolved by native `shape.Resolver.IsComparable`.
Named slice/map types and unsafe interface-valued keys retain their row data but
receive no invalid map helpers. High-level generation supplies descriptor
authority automatically; authors retain the full package identity of named field
types in DQL/imported shapes.

## Linked input and reparenting boundaries

Foreign input types are not changed. Their generated component provides a typed
free builder and optional concrete-definition `ResolveIdentity` callback, after
capture and before normal Input.Init. Linked inputs must already declare their
read slots.

Default parent equality checks do not infer permission to reparent from a broad
read. An explicit semantic `AllowReparent` policy permits non-identity link
changes only; identity freezes and native final reference checks remain active.
An existing authorized parent can supply its stable key to a new child whose
compiled missing action is INSERT. If that completed child tuple already exists,
it remains INSERT and must fail/roll back, not become UPDATE.
