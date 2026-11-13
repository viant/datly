## Enable reports in component metadata

A Go component field can opt into report derivation and bounded composition:

```go
// Tag fragment on an xdatly.Component[I, O] field:
`component:"Things,path=/things,method=GET,report=true,reportCompose=true,reportComposeMaxCubes=12" mcp:"[{\"kind\":\"tool\",\"name\":\"Things\"}]"`
```

Configured standalone/custom builds discover the selected linked packages and
register the source reader, its internal POST `/cube` route and opt-in
`/cube/compose` route together. These are HTTP endpoints only when an HTTP
gateway is mounted. Enable `report=true` on a read-only reader with a groupable
output view; enable `reportCompose=true` for composition. Source route methods
(including POST) do not determine cube eligibility. A declared mutation is not
a cube source. MCP calls reuse the source contract without requiring REST
exposure; the derived route retains POST internally. Link the source input/output
and referenced types in the executable; derived report inputs are compiled during
publication and do not need an application-owned binder or report engine.

The source reader remains available. SQL URI/embed resources, scoped independent
view providers, declared JWT inputs, SQLX mapping and native cache services keep
their existing owners. Failed derivation or resource resolution leaves the previous
generation published. Go contract/method changes still require a rebuild.


> Packaging boundary: Exact maintained author-facing section: includes declarative configuration and behavior; excludes repository navigation, implementation/test evidence and unrelated authoring workflows. Other feature contracts remain in the canonical skill references.
