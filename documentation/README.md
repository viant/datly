# Shared documentation dictionary

This implementation ports original Datly `view/docs.go`, `view/state/docs.go`,
`internal/translator/rule.go:loadDocumentation`, and
`gateway/router/openapi/schema_build.go:updatedDocumentation` into immutable
registration annotations. OpenAPI and MCP consume the same registered documentation
snapshot. Generated documentation, SQL, static assets, and MCP resource folders
share the package resource plan and embedded filesystem owner.

## Authoring and resource ownership

Register unchanged `embed.FS`, `os.DirFS`, or another `fs.FS` with the shared
Bindly `resource.Store`. Named stores always require `namespace:path` references.
Pass that same store to bootstrap and `application.Build.Resources`.

Global defaults use `application.Build.Documentation` (or
`bootstrap.ArtifactInput.Documentation` for direct compilation). Per-component
sources use `spec.Component.Documentation`. These are SDK `docs.Source` values:

```go
Documentation: docs.Source{
    DocURLs: []string{"dictionary:global.yaml", "dictionary:product.yaml"},
}
```

A real package holder can declare `docURL:"pkg:rule.yaml"`,
`docURLs:"[\"pkg:first.yaml\",\"pkg:second.yaml\"]"`,
`docBaseURL:"pkg:docs"`, and a JSON `docSubstitutes` dictionary. DQL declarations
use the existing setting parser:

```sql
#setting($_ = $DocGlobalURLs('docs/global.yaml'))
#setting($_ = $DocURLs('docs/rule.yaml'))
#setting($_ = $DocBaseURL('source:project'))
```

DQL references are packaged automatically by the existing package-resource owner.
Without an explicit store default, unqualified paths resolve relative to the
DQL source path through a scoped Bindly store. The generated Go package emits
`DatlyResources embed.FS`, its namespace, and the canonical asset manifest. Register
that FS with the same Bindly store used for bootstrap and application publication.

Global and rule YAML are retained in source order. Schema references are resolved
and validated by the native schema owner, then emitted as closed authored schema
resources. Original referenced schema files are retained too. The generated YAML
references the emitted schema resources through the generated namespace; it never
depends on the original source directory. The generated-binary acceptance removes
both original and generated resource files before publishing and reloading metadata.

Generated package tags preserve ordered global and rule source authority. `GlobalURLs` always precedes rule references. `DocURLs`, when nonempty,
replaces `DocURL`; it is never appended to it. Global sources precede rule sources.
`BaseURL` is a resource directory, optionally including its namespace. External
URLs belong to the registered filesystem authority, not a second Datly downloader.
The native AFS adapter uses object metadata to distinguish files from
directories and passes `fstest.TestFS`. External AFS files, `os.DirFS`, and
`embed.FS` use the same store. No fallback reader is installed.

```yaml
Columns:
  users:
    _: User records
    id: User identifier
    id$example: '7'
  orders:
    id: Order identifier
  users.code: User code
  id: Identifier fallback
Filter:
  Limit: Maximum rows
Parameters:
  Search: Search text
  Search$example: needle
Paths:
  /records: Search user records
  Rows.Note: Notes about the record
```

Each later document replaces a dictionary entry as a whole. Replacing `users`
does not deep merge its columns. Lookup is case insensitive; a single document's
duplicate or case-conflicting keys fail. Across documents, later normalized keys
win. Duplicate URLs are processed in order. Empty source lists are a no-op;
empty files, missing files, malformed YAML, unknown sections, multiple YAML
documents, and non-string descriptions fail. `{}` is valid. Empty strings retain
original fallback behavior. No precedence depends on iterating a Go map.

Nested table columns precede `table.column`, then bare column defaults. Holder
`_` and `$example` entries are supported. Nonempty explicit parameter/field
annotations win over dictionary values. SQLX aliases and canonical view/column
metadata supply field identity; schema projectors retain field traversal.
`Paths` currently uses canonical Go field paths; column aliases are supplied by
SQLX/spec, not guessed from JSON names. Schema instances are scoped per component and field path so rule annotations
cannot leak through shared Go types, including reuse across relation holders.
Native SQLparser compiles direct joined/aliased and safe wildcard column lineage;
computed or ambiguous projections do not acquire guessed table descriptions.
Private component inputs retain their own dictionary and explicit annotations in
both publishers. An explicit parent consumer wins duplicate transport-input
annotations while child requiredness and verified-auth evidence are retained.

`Substitutes` expands `${name}` and `$name` before parsing, longest names first,
with lexical ties. References/origins retain their resolved resource identity.

## Publication

Bootstrap resolves direct artifact annotations. Manager resolves a global snapshot
once, then applies rule overlays before constructing either publisher. Already
compiled artifacts retain their snapshots when Manager supplies no new globals.
OpenAPI and MCP read the same immutable snapshot; publication is one Manager
swap. Failed loads preserve the active generation. Metadata requests never load
YAML, execute SQL, invoke handlers, or verify JWTs. Schema/auth contracts stay
with their existing owners. Successful reloads update the current MCP registry;
the existing-session wire acceptance runs without `-short` in a listener-enabled parent.

## Transport-ready responses

A handler may declare `response.Response`, `*response.Buffered`, or a concrete
SDK response implementation. Its body is never used to infer documentation.
Author native OpenAPI response metadata under `Responses`, keyed by route path:

```yaml
Paths:
  /download: Download the authored response
Responses:
  /download:
    '201':
      description: Download ready
      headers:
        X-Result:
          schema: {type: string}
          example: ready
      content:
        application/octet-stream:
          schema: {type: string, format: binary}
          example: opaque
```

The native `openapi3.Responses` and `Schema` models are reused. No alternative
wire-schema model exists. These declarations cannot replace an ordinary typed
output schema. Missing generic-response documentation fails OpenAPI generation.
`HEAD` omits response bodies. The HTTP adapter's direct `io.Copy` path is unchanged.
MCP generic response bytes use native embedded blob content (including empty or
non-UTF-8 bytes), and are never parsed to infer `structuredContent`. Ordinary typed
MCP outputs retain their existing structured representation.
MCP exposes HTTP response documentation under tool `_meta["datly/httpResponses"]` with native schema definitions in
`_meta["datly/httpSchemas"]`;
it does not advertise arbitrary HTTP bytes as an MCP `outputSchema`. Ordinary
operation examples are media examples in OpenAPI and `datly/example` in MCP.

## Validation, prerequisites, and bounds

Acceptance covers real YAML and embed.FS, AFS/file resources, ordered
shallow overlays and conflicts, named fields, joined table columns, two relation
holders sharing one Go type, embedded/recursive input and output projection,
private-input ownership, failed/successful metadata-only reload, and generated
binary publication/reload after source removal. Generic response tests cover
byte-for-byte HTTP output and explicit MCP blobs without entity/schema inference.

Native prerequisites are isolated copies: approved X JSON field selection and
Structology wire projection, Structology annotation provenance, AFS file/directory
correctness, SQLparser compiled lineage, and the MCP protocol schema/ref/blob
owners. They are verified through explicit local replacements; no published
version is fabricated. Review and publication of these native prerequisites must
precede integration against published dependencies.

Authored response resources use OpenAPI 3.0-compatible draft-04 schema vocabulary.
Relative and namespaced file references, JSON Pointers, recursive references,
boolean additionalProperties, media types, statuses, headers, and examples are
validated. Unsupported keywords/dialects and alternate id/$id URI scopes fail
explicitly; there is no network or operating-system fallback outside resource.Store.
The actual Go/schema and auth contracts remain authoritative.

The shared documentation runtime and schema consumers passed independent review.
Acceptance includes actual HTTP/MCP listeners, metadata reload, and generated
binary publication after source removal. Static serving, developer MCP lifecycle,
and the skills extension have separate reviews; approval of this package does
not imply approval or publication of those features.
