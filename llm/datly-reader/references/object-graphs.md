# Object and release graphs

Use this reference when deciding whether related reads/writes belong in one graph,
or when replacing application-side object assembly with native Datly behavior.
A useful boundary is one authorized aggregate or use case, not one SQL statement.

## Reader boundary and relation loading

For a catalog release, one reader can return the selected release with its items,
item revisions and related metadata. Declare the root and each related view in
DQL; the outer joins describe typed relations, while each named source owns its
executable SQL. Include every key needed to distinguish tenant, entity and
revision. A relation on entity ID alone can attach another tenant's or release's
rows even if each source is individually valid.

Outer graph fragment, to adapt within a complete reader's package, input/output,
connector and source declarations:

```sql
SELECT rel.*, type(rel,'Release'),
       item.*, type(item,'ReleaseItem','Items'),
       rev.*, type(rev,'ItemRevision','Revision')
FROM (${embed:sql/releases.sql}) rel
LEFT JOIN (${embed:sql/release_items.sql}) item
  ON rel.tenant_id = item.tenant_id AND rel.id = item.release_id
LEFT JOIN (${embed:sql/item_revisions.sql}) rev
  ON item.tenant_id = rev.tenant_id
 AND item.item_id = rev.item_id AND item.revision = rev.revision AND 1=1
```

The last marker selects a to-one holder; it does not prove uniqueness. Preserve
the database's real identity and constraints. Source queries must return relation
keys even when public projections hide them. Declare each view's actual connector;
native relations can batch across connectors without pretending that an outer
DQL join is a vendor SQL join across databases. Do not emulate this with Go loops
that invoke one point reader per parent or reconstruct ordinary joins in maps.

Set batch sizes within bind limits, accounting for complete composite tuples.
Let native loading attach all batches before `OnRelation`. Verify empty children,
multiple batches, same entity ID in two tenants, and same item at two revisions.
A multi-connector read is not a promise of a distributed snapshot or transaction.

## Selection belongs in the native query

Bind authorized scope, release identity, requested membership and revision policy
as native reader predicates. Push filtering into the views before paging, counts
and related expansion. Do not load all revisions then choose the requested/latest
revision or authorized members in Go. Keep required authorization AND-connected to
the entire optional filter expression; a client-supplied tenant is not authority.

Use registered `equal`, `in`, range and other native predicates as appropriate.
For composite membership, use `multi_in` when supported by the selected build and
dialect, with complete typed tuples and the registry's actual argument contract.
Independent `tenant_id IN (...)` and `item_id IN (...)` admit unrequested pairs.
Do not infer tuple-value encoding from a scalar predicate example: validate bound
SQL/arguments, empty input, duplicate keys and cross-tenant/revision collisions.
Keep latest/current revision policy explicit in the native source query when it
requires ordering, grouping or a correlated selection rather than simple equality.

Use Query Selectors against the canonical view identity for allowed ordering,
fields, pagination and related-view inclusion/projection. Keep native relation
keys available internally and selector allowlists explicit. A summary, detail or
export projection of the same authorized graph usually does not need another
reader. Preserve genuinely different authorization, data contracts or independent
operations as separate components; do not widen a graph solely to reduce file count.

## Writer and hook ownership

A generated mutation graph declares its body, Current and auxiliary read-only
views, complete identities/FK links, sparse presence, validation and lifecycle
types. Current is database state, not a replacement for original request presence.
Use native relations and typed read indexes for those reads; auxiliary views do
not become writable just because hooks consume them. Generated writers own
classification, sequencing, validation, diff/reconciliation, queued writes and
managed transaction ownership. Do not bypass a constraint or authorization check
to make a consolidated graph work.

Hooks implement business behavior at the supported phase: input `Init` prepares
prerequisites/normalization, row `OnFetch` transforms loaded values, `OnRelation`
uses the fully assembled selected relations, and output `Finalize` composes the
result. These are distinct contracts: use the documented signatures for the
selected type/phase; reader `OnRelation(ctx context.Context)` has no error return.
Entity mutation lifecycle hooks have their own typed lifecycle context and phase
rules; do not substitute a reader callback for writer validation or persistence.

Custom components and hooks invoke readers/writers with exact typed inputs for
persistence. Await invocation and preserve caller authorization, presence and the
shared transaction owner; a child must not commit its parent's work. Application
`sql.DB`/`sql.Tx` Query/Exec calls and Begin/Commit/Rollback wrappers are not a
substitute. Different connectors do not imply atomic distributed writes. Keep
commit-dependent publication in the supported outcome-aware finalizer.

Keep SQL in DQL views or adjacent SQL source assets. Stock `datly transcribe`
resolves embeds and owns generated/embedded assets; regenerate readers/writers
rather than editing generated Go or adding a parallel SQL extraction/check script.
Verify native execution, selector variations, denied rows, sparse updates and
rollback as applicable. Report an evidenced missing native capability instead of
silently replacing it with application persistence plumbing.
