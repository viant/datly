# View selectors, exact names and response formats

[All guides](references/product/datly/doc/README.md) · [Readers](references/product/datly/doc/readers.md) · [Caches](references/product/datly/doc/cache-and-warmup.md)

Selectors resolve authored column names and explicit mappings. Identifier comparison
is case-insensitive; punctuation and identifier parts remain distinct. The framework
does not invent snake/camel spellings, suffixes or join aliases. Duplicate output
column names are errors rather than silently overwritten or renamed.

For example, `b.id`, `b_id` and `bid` are distinct names unless the author explicitly
maps them. `name` and `Name` use the existing case-insensitive identifier comparison.
If joins produce two `id` columns, choose explicit unique output names.
An authored alias
must retain its source-column and public-field identity through query building,
projection, cache reuse and serialization.

## Scope each selector to a view

Use one-argument `.QuerySelector('inventory')` in DQL or
`querySelector:"inventory"` in Go. The target is the declared view identity,
not the output holder path or a guessed SQL alias. The selector property comes
from the declared logical control, such as Fields, OrderBy, Offset, Limit, Page
or Criteria. Do not use a two-argument selector form or invent a query parameter
name; binding locations are authored independently.

```sql
#define($_ = $Fields<[]string>(query/fields).Optional().QuerySelector('inventory'))
#define($_ = $Limit<int>(query/limit).Optional().QuerySelector('inventory'))
#define($_ = $Offset<int>(query/offset).Optional().QuerySelector('inventory'))
```

This fragment needs a view named `inventory` with the corresponding selectors
enabled and an explicit allowed-column/method policy. Child/sibling views have
independent controls. Offset requires a positive limit; page and limit follow
the view's supported pagination policy. Use bound values for Criteria and exact
authored order mappings; arbitrary SQL expressions are not request input.

Allowed source columns must actually belong to the selected query/view and the
authored public policy. A column's existence does not grant access. Omitted
relations should not execute just to populate hidden payloads. Keep complete
join keys internally for relation assembly, even when no parent scalar is
selected. Request-specific presentation must not mutate shared registered shapes.

## Formats and their bounded representations

The candidate's multiview v4 output uses native typed codecs and a per-request
presentation plan. JSON/CSV/XML/tabular/XLSX project only selected output fields.
This format proof is separate from acceptance of the active naming correction.

| Format | Shape and null behavior |
| --- | --- |
| JSON | Declared direct/enveloped typed output; omitted fields differ from selected nulls. |
| CSV | Selected headers and records; nested relation slices expand as a Cartesian table. Selected null is `null`, zero is `0`. A flat row cannot distinguish no child from one all-null child. |
| XML | Default `result` root and `row` for direct rows; named holders retain their names. Selected null holders/scalars use `nil="true"`; empty nonnil holders remain explicit elements. |
| Tabular | Native header/value arrays, including nested selected relations; zero/null remain `0`/`null`. Envelopes embed native tabular output. |
| XLS/XLSX | Native XLSX workbook bytes, selected headers/cells; numeric zero stays zero and null is a blank cell. Both supported route names use XLSX, not a separate binary XLS encoding. |
| Raw bytes | Application declares media type/status/headers and static documentation; raw bytes do not acquire inferred field selection or an MCP object schema. |

Configure format selection through the actual HTTP output policy. Do not derive
new format spellings or aliases. Test each exposed media type with the active
build, including empty data and relation-only projection.

## Singleton authority

A named `output/view` or `output/body` holder can identify a struct/pointer
singleton. Explicit `DataField` is another existing owner input. A direct root
requires cardinality One; a pointer alone does not establish reader authority.
The singleton becomes a typed zero-or-one row sequence for row-oriented codecs.

Nil named singletons yield header-only CSV and empty tabular rows while preserving
the surrounding envelope and siblings. A nil envelope retains empty CSV / null
tabular behavior. Arrays use typed slice carriers for native codecs. Selected
fields must not cause a child collection to be mistaken for the root result.
JSON envelope shape is preserved. The scoped singleton/body approval covers
these reader-versus-codec authority rules.

## Acceptance

Check exact valid names, rejected spelling variants, explicit aliases and duplicate
output errors after the correction is accepted. Exercise two views sharing Go
row types, independent selectors, complete keys, zero/null/empty relations and
concurrent requests with different projections. Validate native cache hits with
the source unavailable and cube measure narrowing with every dimension retained.
