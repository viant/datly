# Output, conditional finalization and operations

Load this for direct bytes, output documentation, static content, conditional
component binding or operational services. Preserve the
[existing examples](references/product/llm/datly-custom-component/references/custom-examples.md) and
[verified JWT input/predicate pattern](references/product/llm/datly-custom-component/references/tags-and-interfaces.md#jwt-input-and-authorization-predicates).

## Direct bytes and authored documentation

Use `github.com/viant/xdatly/response.Response` and `response.Buffered` with
`WithBytes`/`WithBuffer`, status, headers and optional compression metadata. Do
not create another responseBuffer type or marshal/unmarshal already-shaped bytes
into an entity. The existing HTTP writer detects the canonical Response before
normal encoding and copies Body directly.

A Go function can construct a direct response with the public SDK:

```go
// Import "github.com/viant/xdatly/response".
func CSV() response.Response {
    return response.NewBuffered(
        response.WithStatusCode(200),
        response.WithHeader("Content-Type", "text/csv; charset=utf-8"),
        response.WithBytes([]byte("id,name\n1,first\n")),
    )
}
```

`WithBuffer` accepts an existing `*bytes.Buffer`. Do not mutate backing bytes
after transferring ownership. This constructs an SDK response; verify the linked
component's declared output registration independently.

The candidate includes authored generic output and static media/schema
documentation. Use the documented response contract and validate the exact
handler output shape; arbitrary interface output is not inferred from bytes.
MCP must retain an explicit protocol content/result representation.

Document arbitrary output using authored media types, status codes, headers,
body schema/reference and examples. Never invoke a handler or inspect runtime
payload bytes to infer a schema. Shared OpenAPI/MCP descriptions are a separate
metadata concern. HTTP response documentation is carried in MCP metadata under
`datly/httpResponses` and schema definitions under `datly/httpSchemas`; ordinary
typed MCP output retains its structured-content contract.

## Conditional injector finalizer

Use `handler.InjectorFinalizer` when an output conditionally obtains an injector
for a named external component and uses native Bind to automatically match the
current output with target input through authored parameter/type metadata. Verify
both condition branches; no lookup/binding should occur in the false branch.

Current SDK signature:

```go
func (o *Output) Finalize(ctx context.Context, lookup handler.InjectorLookup) error
```

Select the child with a local `handler.Route`. The child input can bind from the
caller output with `parameter:"ID,kind=caller_output,in=ID,required"`, and the
caller output can receive child data with `bind:"kind=param,in=Data"`. Do not
copy original internal session types into an application or substitute manual
field copying/JSON roundtrips for this automatic binding.

The feature retains route authority, declared request/JWT headers,
authorization, recursion/cancellation guards, canonical scope and caller transaction
ownership without using a completed Data scope. Resolve timing against precommit
error finalization, postcompletion success and result-aware outcome finalization.
Use [the SDK guide](references/product/xdatly/handler/injector_finalizer.md) as the
authoritative example. The candidate has runtime SQLite/auth/concurrency tests
and transcribed Go output discovery coverage; still validate the connected build
before presenting an application recipe as runnable.

## Documentation and static files

Current OpenAPI and MCP metadata consume declared types/descriptions. Only exposed
components enter public documents. Use `Info` or `OpenAPI` host configuration,
never both. Document-route access is a separate policy from endpoint auth; the
default aggregate route is `/v1/api/meta/openapi`, controlled by `Meta.OpenApiURI`.
Named resources use explicit namespaces and must resolve before staging succeeds.
Internal fields, Has markers and private error causes stay out of public schemas. The shared
YAML table/column dictionary with ordered global then per-rule overrides,
embedded/AFS resources, `Responses`, response schema resources and consistent
metadata-only reload is present in the candidate. DQL settings
are `$DocGlobalURLs(...)`, `$DocURL(...)`, `$DocURLs(...)` and `$DocBaseURL(...)`.
YAML sections are `Columns`, `Filter`, `Parameters`, `Paths` and `Responses`.
Preserve explicit annotation precedence and immutable snapshots; do not invent
a new loader schema or network upload endpoint. See
[API documentation](references/product/datly/doc/api-documentation.md).

Static folder/filesystem-to-URL-prefix serving is present in the candidate. DQL
settings are `$static_resource('site', 'public')` and
`$static_content('content-url', 'root')`; both arguments are quoted literals.
A manually returned byte response is not static root routing. Require root
confinement, path/index/GET/HEAD/range cases where supported, auth/CORS and
route collision acceptance before claiming deployment acceptance. See
[static content](references/product/datly/doc/static-content.md).

Gateway MCP folder resources are declared with
`$mcp_folder('docs', 'guide', 'skill://app-guide/')`. Embedded skills/resources
must be regenerated from final canonical source at integration; do not maintain
manual copies in generated assets.

## Services: use the actual host surface

- [Linked configuration](references/product/llm/datly-custom-component/references/project-build.md): `run`/`start` need
  compiled application exports. The stock binary does not execute arbitrary Go
  from source; `validate` is not generation/deployment. Current standalone
  services include CORS, APIKeys, warmup admin, OpenAPI startup exports and
  Observation/OTel. No invented watch command.
- [Async](references/mutation-messages.md#async-and-dry-run): original DATLY_JOBS schema, AFS events,
  explicit authorizer, native cache/metrics and known completion. Explicit HTTP async and canonical source/provenance replay are present in the
  candidate; standalone declarative async still needs authorization integration. Retain
  RUNNING/event state on pending/unknown completion; no blind retries.
- [Cache/warmup](references/product/llm/datly-reader/references/cache-and-operations.md): AFS/Aerospike are
  independent choices with explicit TTL. Warmup can use a dedicated connector
  while retaining the regular cache service. No threshold switching or handler
  invalidation architecture. Parent race acceptance passes AFS and live Docker Aerospike narrowing, regular/cube, groups, pagination and table-drop replay. Earlier author sandbox denial is not a capability blocker; no production-scale qualification is implied.
- [Observability](references/product/datly/doc/observability.md): native capture plus an
  optional bounded application-owned OTel exporter, with no fabricated performance
  or capacity claims. Export failure is not business transaction failure.
- [Mutation limits](references/writer-contract.md): stable IDs precede Queue;
  relation-produced FK deferral applies only to captured parent INSERTs, with final validation before Queue. Custom handlers must own
  their validation/sparse policy explicitly.

Acceptance distinguishes SDK construction, direct HTTP transport, authored
registration and full protocol behavior. Report missing support precisely rather
than presenting a requested contract as current implementation.

Use [project build](references/product/llm/datly-custom-component/references/project-build.md) for automatic traversal/internal
linking, explicit development pins/mappings and truthful source-backed deployment.
Use [selectors/formats](references/product/datly/doc/selectors-and-formats.md) for
view-specific pagination/filter/order, singleton/body and native CSV/XML/XLSX.
Public selectors use exact authored names: no inferred spelling variants, only
user-defined aliases, and duplicate output column names are errors. See
[developer MCP](references/product/llm/datly-custom-component/references/developer-mcp.md) for declared Final SEP-2640 skills,
list/get/read consistency and reproducible canonical bundle generation.

## Configured reports

Selected linked packages can enable `report=true` on groupable GET readers and
`reportCompose=true` for composition. Keep the source reader and its declared
JWT/non-query inputs. Explicit cube filters override source values; omitted cube
filters retain source binding. Composition masks omitted frame filters so they
cannot inherit unrelated outer request values. Composition uses declared SQL output aliases or explicit authored mappings,
not inferred Go/JSON name variants. Cache reuse must retain every warmed grouping
dimension and may narrow measures. Source/URI reload publishes the complete report
set atomically. API-key-only HTTP routes must set `reportMCPTool=false` and
`reportComposeMCPTool=false`, or declare an MCP-compatible authorization policy.
Verify actual configured HTTP and native MCP execution; metadata discovery alone
is not runtime proof.
