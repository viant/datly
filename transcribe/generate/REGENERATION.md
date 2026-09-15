# Regeneration ownership

## Schema null constraints

Explicit table metadata can supply a NOT NULL validation constraint independently
of query-result nullability. Drivers can report different metadata for a result
set and its source table. This does not change an explicit Go CAST type: a
`*int` field can remain a pointer while native validation rejects a nil value.
Missing table metadata is not interpreted as a NOT NULL constraint. Authored
`sqlx:"...,required=false"` and transient mappings retain their authority.

Generated projection metadata follows the current canonical DQL/schema proposal.
Regeneration may add, change or remove invariant, validation and SQLX tags only
when the manifest records that field's prior generated type and tag, and the
current destination still matches both. Even a hand edit equal to the new
proposal is not accepted as previous generated source. Missing trustworthy field
ownership retains conflict protection. Authored `required=false` in the current
DQL remains authoritative; removing a generated constraint is now supported.

The same field evidence permits inferred type changes, including removing a
CAST and returning to discovered nullability. A changed generated type must
still match the prior emitted type. Existing explicit CAST policy preserves
unrelated authored tags when DQL has not changed those generated tags. It cannot
silently omit a requested metadata update or overwrite a conflicting edited type.
The native `x/shape.SourceParser` performs all exact type/tag edits, import
rewrites, and removals; Datly supplies only source-ownership authorization.
Relation holder tags retain their separate destination/cardinality protections.
An exact `on`-only transition may follow canonical projected key aliases when
the prior generated type/tag still match and the child type is unchanged; other
relation tags retain conflict protection.

Automatic UNIQUE discovery is not being expanded across drivers, per user
direction. Authored native UNIQUE tags remain the explicit contract; missing
driver metadata does not prove that a database constraint is absent.

`.datly-gen.json` version 5 records `sha256:` content fingerprints and exact
generated projection-field ownership for emitted files. A generated filename alone is not permission to overwrite its contents.

Regeneration replaces or removes an artifact only when its current bytes match
the last recorded generated fingerprint. An edited Go handler, Velty template,
router, or SQL resource causes an explicit conflict. Identical desired bytes are
safe to retain, even if an older fingerprint is unavailable. Conflict validation
runs during project preflight and again against the staged package before any
generated files are published.

Generated shapes keep unowned and customized type/tag conflict protection.
Canonical field ownership and explicit user authorities apply across reader, Go,
Velty and generic mutation writer transcription:

- Standalone `CAST(view.column AS *int)` changes that exact generated-owned
  field's type, including `int` to `*int` and reverse casts. Exact CAST type
  authority overrides inferred database nullability. SQL-aliased CAST remains
  executable SQL and grants no Go source edit authority. Conflicting CASTs in
  one source fail even though a CAST may override a prior column definition.
- Changing canonical relation cardinality changes that generated holder between
  `*Child` and `[]*Child`, including adding/removing the outer DQL JOIN
  `AND 1=1` marker or changing an explicit `cardinality(...)` directive. This
  applies to reader and Go/Velty writer generation. The field plan records
  relation authority separately from CAST. Persistence requires the holder to
  match its previously recorded generated type and tag, and permits only the
  slice wrapper to change. Authored edits, changed child types/tags, and missing
  trustworthy field ownership require explicit migration. Older manifests may
  bootstrap ownership only through the existing whole-file evidence rules.
- Removing a projected SQL column removes its previously generated field and
  associated generated presence fields. Current explicit logical declarations
  and projection helpers remain. Field order, unrelated authored fields,
  methods and comments stay destination-owned.

`spec.Column.ExplicitType` flows through generated fields and typed projection
helpers. Persistence passes exact type changes, obsolete owned fields, and
guarded projection metadata changes and narrow helper codec-reference changes to native `viant/x/shape.SourceParser`.
`EditStructFields` accepts exact requests; `AppendStructFields` and the existing
`UpdateStructFields` API keep their default behavior. No Datly source AST merger
or deletion walker is involved. Imports are adjusted only as required by these
field edits; grouped/embedded field changes fail closed when the selected field
cannot be isolated. External linked types cannot be rewritten and must already
match explicit CAST field types, including nested relation paths.

The manifest's `projectionFields` inventory records struct owner, field name,
canonical type and prior emitted tag from the raw generator proposal. It covers
view fields, generated presence fields, projection helpers and their generated
input references. It never adopts an existing unowned field just because a later
proposal happens to contain the same field. The `complete` flag records whether
prior generated-field ownership is exhaustive. A removed field whose type/tag
was customized fails closed; comments are preserved because the inventory does
not grant comment-deletion rights.

Generated helper field types follow the current view plan, ahead of stale
catalog snapshots. A helper still referencing a removed current field fails
before persistence. If its authored projection changes, its generated codec tag
may change only from the exact previously emitted tag and only when other tag
keys are unchanged. Customized or unrelated tags retain conflict protection.

Original Datly `repository/shape/xgen` preserves field positions and authored
type authority across discovery. The explicit new projection-removal requirement
diverges from its retained-removed-field behavior: old generated projection
fields now disappear, while unrelated application fields remain. This is a
user-authorized design change with reader/writer regression evidence.

Customized shapes retain their earlier trusted whole-file fingerprint (or its
absence), even after exact field edits. Untouched fingerprinted shapes can still
follow an intentional generated-to-linked transition; field ownership does not
grant permission to replace or delete customized files. User-owned hook files
remain outside the generated fingerprint inventory. Obsolete whole shape files
and type declarations retain the existing conservative retention policy; this
field-removal authority does not authorize deleting customized declarations.

Retained SQL assets keep their previous fingerprint. Keeping a manually edited
asset does not silently approve overwriting it in a later regeneration.

## Migrating version 2 or 3 manifests

Versions 2 and 3 remain readable. Prior field ownership can be bootstrapped when
the current file exactly matches its trusted generated fingerprint, or when its
entire bytes match the current raw generator proposal. These are concrete source
evidence; a filename, role, Go tag or similar-looking field is not evidence.

A customized older file without field inventory cannot prove which omitted
fields were generated. Projection removal fails closed when that uncertainty
affects the current projected shape. Regeneration can add provably new fields,
but does not silently acquire deletion authority over existing fields. Generate
the desired output separately and reconcile older customized source deliberately;
do not fabricate fingerprints or field inventories. Changes to helper references
without a trustworthy prior emitted tag also retain default conflict protection.

Removing a field may require updates to application code that directly refers to
that field. Application methods are preserved, not rewritten by the source
merger. The same exact-field validation runs in preflight and again in staging;
a conflict leaves the package and its manifest unchanged.

The package is staged separately. Before publication, the renamed original tree
is checked against the staging snapshot, including user-owned files; detected
concurrent edits abort publication and restore that tree. Other processes should
still coordinate writes during publication: an uncooperative writer holding an
open file descriptor cannot participate in an application-level filesystem lock.
