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
Configured `CacheWarmupSettings` additionally exposes `Limit`, `MaxCases`,
`FieldNames`, `IndexColumn`, `IndexParameter`, `IndexMeta`, `Connector` and `Cases`.
Per-case field names and excluded defaults belong to the typed settings; do not
invent DQL options for every Go field.

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
