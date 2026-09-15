## Shared YAML dictionary and response schemas

The candidate loads reusable global dictionaries followed by ordered per-rule
YAML overrides from files, AFS or embedded resources. One immutable resolved
snapshot enriches OpenAPI and MCP consistently. This is integrated in the local `v1` release copy; final regression remains
required. This is not a network upload endpoint.

The original Datly behavior is the baseline: table dictionaries, qualified
`table.column` lookup, bare-column fallback, `_` holder
descriptions and `$example` values; Filter, Columns, Parameters and Paths are
relevant documentation sections. Ordered documents merge with last-entry-wins
at dictionary-key level. Do not silently promise recursive deep merge or invent
a new YAML schema from those names.

The candidate DQL settings are:

Declaration fragment; adapt to the [complete reader contract](../../llm/datly-reader/references/reader-examples.md#parameterized-dql-reader)
with the target view identity and existing input/output bindings.

```sql
#setting($_ = $DocGlobalURLs('docs/global.yaml', 'docs/product.yaml'))
#setting($_ = $DocURLs('docs/rule.yaml'))
#setting($_ = $DocBaseURL('resources:documentation'))
```

`DocURL` accepts one rule document. `DocURLs` and `DocGlobalURLs` accept ordered
lists. Go component tags in the candidate use `docURL`, `docURLs`,
`docGlobalURLs`, `docBaseURL` and `docSubstitutes`. The YAML dictionary sections
are `Columns`, `Filter`, `Parameters`, `Paths` and `Responses`; unknown sections
fail. A response schema reference is authored as ordinary OpenAPI schema YAML,
for example:

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

Schema references are bundled by the native MCP/OpenAPI schema resource owner
and rewritten into generated package resources. Do not invent another YAML
shape, deep-merge policy, upload endpoint or runtime byte inspection rule from
the concept names. Before treating this feature as enabled, the integrated build
must establish all of these behaviors:

- Global sources load before rule-specific overlays with deterministic ordering
  and preserved origin; explicit authored descriptions/examples retain their
  defined precedence.
- One resolved immutable snapshot annotates both OpenAPI and MCP; table-qualified
  names avoid collisions between same-named columns in different tables.
- Resources use the canonical store and existing type/field traversal owners.
  Annotation does not change SQL projection, Go types or authorization.
- A successful metadata-only Manager reload updates both publishers consistently;
  failed document loading keeps the previous generation. Existing MCP sessions
  follow the supported publisher refresh behavior.
- Missing, malformed, empty, duplicate and case-conflicting entries have explicit
  tested policy rather than accidental map iteration behavior.

Generated-document and resource tests cover the integrated pipeline.
Name/alias behavior follows the [naming contract](selectors-and-formats.md);
do not infer spelling variations or apply a guessed alias while annotating fields.

## Custom serialization in OpenAPI

Custom JSON/text serialization does not require removing an otherwise valid
endpoint from OpenAPI. The schema uses the guarantees of the actual encoding
contract, without invoking application serialization methods to guess a shape.

- A value implementing `encoding.TextMarshaler`, such as `uuid.UUID`, is
  represented as a JSON string when that contract determines its encoding.
- A `json.Marshaler`, including `json.RawMessage`, may produce any JSON value.
  Its schema is unconstrained rather than inferred from private Go fields.
- Pointer-only custom methods can depend on value addressability. Their schema
  remains conservative where a fixed representation cannot be guaranteed.
- JSON body schemas follow custom unmarshaling contracts independently of output
  marshaling contracts. Query/form and other transport inputs retain their
  provider-specific conversion rules.

Global output casing applies to surrounding typed fields. It does not rewrite
keys inside a RawMessage or bytes emitted by a custom JSON marshaler. Dictionary
annotations can still supply descriptions and examples for these fields.
An unconstrained schema means that consumers must not assume an object, array,
string or numeric shape without an additional application contract.

Transport-ready `response.Response` bodies still require explicit authored
response documentation, as described below. Unsupported non-JSON values such
as channels remain errors; this support does not make them serializable.


> Packaging boundary: Exact maintained author-facing section: includes declarative configuration and behavior; excludes repository navigation, implementation/test evidence and unrelated authoring workflows. Other feature contracts remain in the canonical skill references.
