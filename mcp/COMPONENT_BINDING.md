# Exact component execution

An embedding deployment can supply `mcp.Config.LinkedArtifact` or
`standalone.Options.LinkedArtifact`. Its revision and SHA-256 content fingerprint
must identify an actual immutable executable artifact and its linked resources.
Datly does not infer release provenance from `Config.Version`, Git HEAD, a schema
hash, or the manager's whole-source-set reload counter. These options are trusted
host inputs; request payloads never set them.

Every artifact-backed tool publishes `viant.datly/component` in its MCP tool
metadata. The value has `kind`, `id`, `revision`, `contentFingerprint`, and
`schemaFingerprint`. For linked tools, `id` is the actual component key and
`schemaFingerprint` is SHA-256 of the compiled route target and public input and
output schemas. A component with multiple exposed routes therefore has a
distinct route contract pin for each tool.

Callers place the complete observed value in the same namespaced entry of
`tools/call.params._meta`. The handler compares every field against its immutable
compiled tool binding before argument binding and native invocation. Identity is
outside business arguments and grants no authorization. The tool's ordinary
input authorization and the transport's authorization still apply.

The check and invocation use the same admitted manager generation. A request
after publication of a different artifact rejects the former binding; it never
falls back to the active artifact. This capability does not provide a historical
artifact repository. An unavailable historical artifact is rejected. A request
already admitted into an owned generation retains that generation through its
existing lifetime contract.

`RequireComponentBinding` denies execution even when provenance is unavailable.
An artifact-backed tool always requires the full binding. Hosts with no declared
artifact and no requirement retain the existing ordinary MCP behavior. A trusted
consumer that classifies a service as a native producer must not downgrade it to
an ordinary tool call when its artifact transport is unavailable.

The generic MCP request metadata carrier must preserve namespaced extensions.
The companion mcp-protocol source change supplies that capability; deploying
against an earlier carrier that discards extensions will fail closed rather than
execute a required binding.
