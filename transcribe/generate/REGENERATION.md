# Regeneration

DQL and explicitly linked Go contracts define a component. Every generation
entry point (`datly transcribe`, `Generator.Generate`, `Compiler.Transcribe`,
and project generation) uses the same publication path. Datly no longer writes
or reads `.datly-gen.json`. There is no manifest option, fingerprint ownership
mode, or replacement sidecar. Old sidecars are removed during publication,
including malformed ones; their contents have no authority.

## Generated code and application code

Generated shapes, presence markers, setters, indexes, component holders,
handlers and packaged SQL follow the current DQL/schema proposal. Regeneration
replaces their contents and removes obsolete artifacts in the affected package.
Direct edits inside generated files are overwritten; the editor is responsible
for those edits. Standard `Code generated ... DO NOT EDIT` comments identify
replaceable Go artifacts. A filename alone does not identify application code
as generated.

Put business behavior in separate application files, declared lifecycle hooks,
or explicitly linked Go contracts. Lifecycle scaffolds are created once and
remain application-owned. Linked contracts keep their Go authority, including
contracts named `input.go` or `output.go`. Application methods remain in their
separate files; incompatible signatures and duplicate declarations still fail
validation. When DQL removes a generated field, update application code that
uses it.

DQL owns projected field additions/removals, CAST/nullability, tags, relation
keys/cardinality, and generated helper shapes. Removed projections lose their
Has markers and setters too. Explicit schema constraints remain distinct from
query-result nullability: a nullable Go pointer can still have a NOT NULL
validation constraint. Missing metadata does not imply a required or UNIQUE
constraint. Authored native SQLX tags remain authoritative.

Generated struct tags use lexicographic outer-key order after field rewrites.
Tag values, explicit empty values, and option order inside each value are retained.
Replacing an existing tag preserves its key position until emission; replacing
it with the same value leaves its bytes unchanged. This normalization does not
rewrite application-owned contracts or source-selected members.

## Resources and reload

Generated holders expose `EmbedFS()` and `EmbedNamespace()`. Source discovery
uses the corresponding `*DatlyResourceNamespace` constants and
`*DatlyResources` embed declarations. SQL/static/MCP files are selected from
these declarations; namespaces, paths and missing files are validated.
A workspace reload snapshots current source assets, while linked holders supply
binary embed capabilities when source declarations are unavailable. Published
resource stores are immutable per generation. Obsolete resources are removed
only for the affected component; assets referenced by other namespaces survive.
Linked Go-only contracts retain their declared SQL URIs and source embed files.

Lazy component materialization and request-local writer `ReadIndexes()` use
in-memory component metadata and loaded reader results. Neither uses a package
manifest.

## Upgrading older generated packages

Regenerate once using the existing DQL destinations before changing filenames.
Old input/output/view/component scaffold comments identify existing generated
code. For older support/resource files without a standard header, the existing
component holder and matching current Go AST declarations establish the upgrade
at the declared destination. Receiver methods are matched by canonical receiver
and signature, including setter-only files. Package/comment-only support files
can match a current package-only artifact; imports, initializers and unrelated
application declarations are excluded. Regeneration adds the standard generated header and
removes the legacy sidecar. No application file needs to be edited for migration.
Subsequent filename changes retire the component's old generated artifacts.

Changing `#package` or moving contracts between packages is an application
migration: update imports and the application link selection, and retire the old
package explicitly. Generation does not guess that a same-named component in a
different package is obsolete. Separate components sharing a package must use
nonconflicting declarations and destinations.

## Publication

Packages are staged before publication. Source-authored handler factories build
against the real destination module using Go overlays, preserving workspace,
vendor, build-tag, target and internal-package rules. Validation executes no
application initialization or hook logic. A failed stage leaves the previous
package intact.

A transient content snapshot detects edits made during staging and restores the
original tree if publication detects a concurrent change. These hashes exist
only for the publication transaction and are never persisted as ownership data.
Other processes should coordinate filesystem writes during publication.

## Explicit source members on generated factory contracts

A source-authored generated factory may retain direct local application state with
`#setting($_ = $source_member('Request','origin'))`. The two literals select only
a local generated owner and direct member. Its current build-selected source
supplies the exact type, tag (including absence), ordering and file destination;
none is repeated in configuration. Selected fields must exist exactly once. The
same parsed source AST is shared with staged retention; changed source preimages
reject publication. Initial admission compares all remaining members against
independently rendered binding/SQL authority. Unknown fields, order, type or tag
drift fail that comparison. Regeneration uses fresh generated fields, discards
unselected extras and retains only selected state. Selections cannot claim `Has`
or presence markers. This supplies no SQL/schema or linked graph exemption and
creates no runtime parameter binding.

`#setting($_ = $source_input('Request'))` keeps that generated transport Input
free of synthesized Has, its companion and setters. It requires explicit source
members on the same local input; physical row presence behavior is unchanged.
When the input and selected body owner explicitly use the same generated
destination, their independently generated declarations are assembled into one
staged artifact. Functions and methods in that file survive. Missing selections,
duplicate ownership, foreign owner files, incompatible generated declarations and
staged Go build failures reject publication. No persistent ownership metadata is
created. Current DQL and current source/generated authority define ownership.

A source-selected generated factory can author a nonpersisted composition through
its exact Go body declaration and member joins. Each logical occurrence must be
proved from the same build-selected source snapshot; native SQLX row mappings
still require independently authored physical sources and existing six-argument
`borrow_sql_row` owner evidence. Logical occurrence proof is compilation-local,
never persisted as configuration. It permits a missing sparse marker only on
that exact nonpersisted occurrence; existing malformed markers and every physical
child remain subject to their ordinary checks. A logical root has no generated
physical writer companion. Its complete plan is validated before that companion
is skipped, and referenced native children keep their own helpers.

Reference-only native owner admission compiles the complete original owner,
checks its original generated artifacts byte for byte, and seals source and fresh
physical descendant facts through final staged publication. It does not transfer
owner readers, DML or generated-file ownership. Complete raw MySQL table seals
retain known nonunique, nonPRIMARY index prefixes exactly; generated physical-leaf
borrowing and unique/PRIMARY prefix rejection remain unchanged. These raw facts
establish no projection, full-column uniqueness, nominal type or relation proof.
