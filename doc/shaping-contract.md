# Datly v1 shaping contract

DQL defines generated shapes. Explicitly linked Go contracts define application
shapes. Go identifiers, vendor SQL result columns, physical DML columns, and JSON
names are separate identities; preserve the mapping between them.

## Outer field rename versus inner SQL alias

The outer projection of a named view graph is component configuration. A direct
column alias there renames the generated Go field and keeps the original view
output in its SQLX tag. It does not introduce that alias into vendor SQL.

Syntax fragment; add the component's package, route, typed output holder and
connector declarations before transcription:

```sql
SELECT record.ID, record.NAME AS DISPLAY_NAME
FROM (SELECT ID, NAME FROM RECORDS) record
```

The generated field is equivalent to:

```go
DisplayName string `sqlx:"NAME"`
```

The exact type/nullability comes from the selected Go contract, outer CAST or
schema discovery. The executable projection selects `record.NAME`. Its SQLX
mapping is `NAME`, rather than `DISPLAY_NAME` or `NAME|DISPLAY_NAME`.

An alias **inside the view SQL** is a database result alias and remains intact:

```sql
SELECT record.STORED_LABEL AS DisplayName
FROM (SELECT NAME AS STORED_LABEL FROM RECORDS) record
```

Here the vendor result column is `STORED_LABEL`, and the outer field rename maps
`DisplayName` to that result column. Native source lineage and SQLX alternatives
continue to handle physical DML names when a real SQL alias requires them.
An ordinary SQL query without a surrounding named-view configuration projection
retains its own SQL aliases.

Renamed identity/join fields retain original vendor column mappings. Relation
predicates and writer Current/Previous queries use actual SQL result/source
columns; typed matching, setters and presence use the Go fields. A field rename
must not remove primary-key metadata, broaden row scope or change relation keys.

Writer Current projections follow explicit inner SQL aliases through named-view
wildcard wrappers. For example, a query exposing `pod_id` is read and filtered
through `pod_id`, even when its underlying physical column is `id` and its Go
field is `PodId`. Composite lookup helpers retain each SQL output name separately;
two tables exposing different aliases of `id` must not collapse to one key. Alias
resolution belongs to the compiled plan and does not inspect runtime row values.

## Public naming and types

- Outer direct-column `AS` renames a generated Go field.
- `format:"name=DisplayName"` controls public serialization naming. Global
  `case_format('lc')` then produces `displayName`; it does not change SQL columns.
- A nonempty `json` name is an exact public-name override.
- Standalone outer `CAST(view.column AS GoType)` supplies Go type authority.
  A database SQL CAST inside the view keeps its SQL meaning.
- `T`, `*T`, slices and maps retain their Go semantics. NULL differs from a
  non-null zero. Writer Has markers distinguish omission from supplied values
  and remain internal to the generated contract.
- `internal:"true"` hides a physical field from public output without making it
  transient. A logical non-DML field requires its explicit mapping policy.

## Regeneration and application customization

Generate through the v1 `datly transcribe get|post|put|patch` CLI or matching API.
Regeneration replaces generated files from current DQL/Go metadata, including
field names, type/nullability, tags, relations, presence and helper support.
Direct generated-file edits are overwritten and are the editor's responsibility.

Keep business logic and additional application fields in separate hook files or
explicitly linked Go contracts. Create-once lifecycle scaffolds and those linked
contracts remain application-owned. Changes to DQL fields can require updates to
application code that uses them. No `.datly-gen.json`, ownership flag or replacement
tracking sidecar is required.

Validate the generated shape **and the actual compiled vendor SQL**, then execute
it through the native reader/writer with realistic fixtures. A manually invented
aliased SQL query is not evidence of how the outer DQL configuration is lowered.
