# MCP catalog authorization

`Config.AuthorizeCatalogTool` receives the server-owned component target and an
action (`discover` or `describe`). `tools/list` includes a tool only when both
actions permit its metadata. Tool invocation continues to use `AuthorizeTool`.

`Config.AuthorizeCatalogResource` receives the resource URI and action. Resource
and skill lists filter metadata before skill pagination. Skill manifests also
require access to their supporting-file inventory. Both native skills operations
and the `skills/list` / `skills/get` compatibility tools use these checks.
Filtered skill cursors become invalid when the visible catalog changes.

`AuthorizeResource` now also runs before the protocol adapter reads sealed static
skill files. Static byte authority, digests, and generation ownership remain
unchanged. Authorization callbacks must use the deployment's verified identity
and server-owned resource policy. Skill frontmatter is descriptive content.

Nil catalog callbacks preserve public metadata behavior. Hosts exposing protected
components must configure catalog callbacks independently of execution guards.
Callbacks apply to each pinned generation and never mutate its registry.

## Namespace resource reads

`resources/read` is the standard MCP method. A registered namespace-extension
`resource/get` tool provides its equivalent JSON resolver for catalog resources.
When no native resource mapping exists, Datly invokes that tool's existing
compiled handler with the requested URI, preserving binding and authorization.
Mapped resource failures and authorization denials remain terminal. A failed
tool result never becomes successful resource contents. Actor-scoped bridge
responses are private and have no retained cache lifetime.

This is one resolver exposed through two protocol surfaces, not two resource
implementations. Existing native resources and servers without `resource/get`
retain their behavior.
