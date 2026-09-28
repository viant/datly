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
at the declared destination. Regeneration adds the standard generated header and
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
