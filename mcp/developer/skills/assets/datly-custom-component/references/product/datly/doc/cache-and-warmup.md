## Warmup query cases

The DQL parser supports a dedicated connector, an index column/parameter and case
values:

Declaration fragment; adapt to the [complete reader contract](../../llm/datly-reader/references/reader-examples.md#parameterized-dql-reader)
with the target view identity and existing input/output bindings.

```sql
#setting($_ = $cache_warmup('order_id', 'Connector=bq_metrics_prewarm', 'IndexParameter=OrderId', 'Period=today,yesterday', 'Granularity=hour,day'))
```

This fragment requires those canonical input parameters, index column and both
connectors to exist. The `Period`/`Granularity` values expand into combinations;
select limits deliberately rather than accidentally warming an unbounded product.
Configured `CacheWarmupSettings` additionally exposes `Name`, `Priority`,
`Limit`, `MaxCases`, `FieldNames`, `IndexColumn`, `IndexParameter`, `IndexMeta`,
`Connector`, `CaseRefs` and `Cases`. Per-case field names and excluded defaults
belong to the typed settings; do not invent DQL options for every Go field.

A cache may declare several warmups: the singular `Warmup` plus an ordered
plural `Warmups` list, with named reusable case sets in `SharedCases`. The
singular contract is unchanged and executes first; plural entries follow in
declaration order and each warmup owns its cases, connector, limits, projection
and index settings. `CaseRefs` expand per warmup ahead of inline cases with no
cartesian product across indexes and no shared mutable case slices. Duplicate
effective warmup names or index identities (an absent name derives from
`IndexParameter`, then `IndexColumn`) fail initialization, and an empty plural
list never shadows a valid singular warmup. A regular request selects the most
restrictive supplied index by explicit `Priority`; equal priorities let the
later, more specific declaration win. Required index inputs are omitted only
for the warmup that owns them.

`IndexMeta` selects related output queries for warmup too. Each target needs its
native cache service. Limits and counts describe completed warmup work, not a
promise that every later query shape can reuse it. Partial failure can leave
already-populated entries; warmup is not a cross-query cache transaction.

## Choose the warmup identity to match the SQL

For a query that always filters one ID, warm the concrete parameter case without
an index column:

```sql
#package('example.com/app/records/warmup')
#setting($_ = $input_type('RecordsInput'))
#setting($_ = $output_type('RecordsOutput'))
#setting($_ = $case_format('lc'))
#setting($_ = $route('/records', 'GET'))
#setting($_ = $connector('main'))
#define($_ = $ID<int>(query/id).Required())
#define($_ = $Records<[]*Record>(output/view))
#setting($_ = $cache_warmup('', 'ID=1'))
SELECT r.id, r.name, type(r, 'Record') FROM records r WHERE r.id=:ID
```

Configure `main`, `records(id,name)` and the native read cache for this reader.
`RecordsOutput.Records []*Record` receives the root rows as JSON `records`.
The explicit `ID` input supplies the exact warmup SQL/argument pair;
another ID can fill its own lazy cache entry. A successful warmup response alone
does not prove later reuse: verify the read after making the database unavailable.

Indexed warmup needs an all-row form of the query. Ordinary lookup clears the
configured index parameter when computing the warmup identity, then retrieves
its requested groups. The authored predicate must support that cleared-input
form, and warmup must use the same identity. Combining fixed `id=:ID` SQL with
`IndexParameter=ID` and a concrete `ID=1` warmup case produces different identities.
Use per-query warmup for that SQL, or author an optional predicate and warm its
whole-group form. Other inputs, such as tenant authorization and period, remain
part of the identity and must match.


> Packaging boundary: Exact maintained author-facing section: includes declarative configuration and behavior; excludes repository navigation, implementation/test evidence and unrelated authoring workflows. Other feature contracts remain in the canonical skill references.
