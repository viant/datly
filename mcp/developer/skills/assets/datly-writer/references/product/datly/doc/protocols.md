# HTTP and MCP from the same component contract

[All guides](references/product/datly/doc/README.md) · [Security](references/product/datly/doc/security.md)

HTTP and runtime MCP adapt the same exposed component catalog into protocol
operations. Binding, typed handler invocation, errors and output finalization
remain in the canonical engine. A component's public shape must not depend on
which adapter happened to call it.

## HTTP

Author an exact method/path and typed parameter sources. Path, query, header and
body declarations compile into binding plans; required status and authored safe
error messages survive protocol handling. The original `*http.Request` remains
available through the protocol boundary for declared providers.

The quickstart demonstrates JSON GET/POST and generated OpenAPI. Current HTTP
owners also implement configured CORS, API-prefix routing, output encodings,
explicit error payloads and direct transport responses. See
[custom handlers](references/product/datly/doc/custom-handlers.md) for bytes and [API documentation](references/product/datly/doc/api-documentation.md)
for schema publication.

CORS is not authorization. Absent global CORS uses the owner's defaults; explicit
empty CORS stays empty, with route policy handled by the existing HTTP owner.
Choose origins/methods intentionally. Do not assume a raw/custom response bypasses
configured final CORS/header policy.

## MCP exposure

MCP metadata can be attached to a component field or authored in DQL. A small
DQL exposure pattern is:

```sql
#setting($_ = $route('/things','GET'))
#setting($_ = $mcp('Things'))
#define($_ = $Id<int>(path/id).WithURI('/{id}'))
SELECT id FROM things
```

This is a source fragment needing the `things` schema and configured connector.
`WithURI` derives an alternative `/things/{id}` route and `ThingsById` exposure.
Verify the active inputs on each route; base and path variants can be enabled
separately through the supported metadata. Exposing an MCP tool does not install
a developer-MCP authoring service.

The [MCP server API](references/product/datly/mcp/server/config.go.txt) supports stdio, SSE and streamable
transports with native protocol/session handling. Configure either its concrete
Service or a generation Source such as Manager. The linked standalone host has
its narrower address/port/authorization configuration; do not infer every server
API option is a CLI field.

Gateway MCP resource folders are composed in the candidate. The exact
DQL setting is:

```sql
#setting($_ = $mcp_folder('docs', 'guide', 'skill://app-guide/'))
```

It maps a registered resource namespace/root to an MCP resource URI prefix after
validating the scheme, host and normalized path. The generated-binary test
reads `skill://app-guide/references/guide.md` after removing the source tree and
rejects traversal. This is integrated in the local `v1` release copy; final release regression remains required. Embedded authoring skills must be regenerated from the
final canonical `llm/` folders with the bundle tool; never edit generated skill
copies separately.

Use the actual published tool/resource schemas. Reader/report output is shaped
through the declared contract; MCP content and structured results must follow
the protocol. Arbitrary HTTP response bytes are not automatically a structured
MCP object. Opaque-output documentation still needs authored metadata.

## Exposure and component composition

Select public packages with the runtime's exposure policy. Private type imports
and component dependencies may remain registered without becoming endpoints.
Use the same filtered runtime/Manager for HTTP and MCP; a second unfiltered
runtime for one adapter can expose private operations accidentally.

An internal invocation targets registered component identity and route metadata,
not an arbitrary URL. It shares canonical scope and transaction ownership.
[Custom composition](references/product/datly/doc/custom-handlers.md) explains typed forwarding and the
injector-finalizer automatic binding contract.

## Errors and lifecycle

Preserve the public response selected by the application. An explicit empty
message stays empty and a private cause remains private. Both adapters use
shared finalization; do not call output hooks again inside the protocol adapter.
MCP-specific finalization follows the SDK contract and protocol context.

HTTP async scheduling/status/results use explicitly configured candidate routes
and canonical control inputs; see [async](references/product/datly/doc/async.md). [Warmup HTTP](references/product/datly/doc/cache-and-warmup.md) requires an explicit
administrator authorizer and server lifetime. Neither capability should be
invented as an unauthenticated route from its name alone.

## Developer MCP and Final SEP-2640 skills

Developer MCP is a separate authoring service. The candidate exposes seven tools:
`datly.transcribe`, `datly.validate`, `datly.run`, `datly.stop`,
`datly.components`, `datly.inspect` and `datly.reverseDQL`. Discover the connected
tool schemas before use. Runtime business tools are the application's exposed
components; they do not become developer tools merely by sharing MCP transport.

Folder publication and skill declaration are distinct. `resource.Folder.Skills`
explicitly declares relative skill directories (`"."` for the folder itself).
A descendant `SKILL.md` is only a supporting file unless its root is declared.
Use native `skills/list` and `skills/get` for declared skills, and `resources/list`
and `resources/read` for their files. Optional directory support is not advertised.

The native Final SEP-2640 compiler seals static file bytes, validates frontmatter
and derives digest/size inventory from that same snapshot. Static registration
installs metadata and readers together. Raw resource overrides cannot introduce
unlisted bytes beneath a sealed root. List/get/read must agree under the same
public/private and authorization policy. Metadata JSON alone is not static
conformance authority. Reading a supporting document does not activate a skill
or grant tool permissions.

The developer's three authoring skills are materialized from canonical `llm`
plus exact declared product imports. [Packaging](references/product/datly/doc/authoring-skills.md) explains
reproducible filesystem/embedded outputs. Custom resource bundles can be embedded;
this does not change the custom project's source-backed deployment contract.

Verify list/invoke behavior, hidden dependencies, parameter/error schemas,
cancellation, concurrent isolation and generation reload through the native MCP
client or real HTTP adapter. Successful in-process calls do not by themselves
prove network listeners, cloud ingress or OAuth deployment.

See the [developer service guide](references/product/datly/mcp/developer/README.md) for all seven tools, per-tool authority and the parent-approved authoring-root reporting correction.
