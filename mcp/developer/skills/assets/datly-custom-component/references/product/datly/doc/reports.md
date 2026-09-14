## Enable reports in component metadata

A Go component field can opt into report derivation and bounded composition:

```go
// Tag fragment on an xdatly.Component[I, O] field:
`component:"Things,path=/things,method=GET,report=true,reportCompose=true,reportComposeMaxCubes=12" mcp:"[{\"kind\":\"tool\",\"name\":\"Things\"}]"`
```

Configured standalone/custom builds discover the selected linked packages and
register the source reader, its POST `/cube` endpoint and opt-in `/cube/compose`
endpoint together. Enable `report=true` on a GET reader with a groupable output
view; enable `reportCompose=true` for composition. Link the source input/output
and referenced types in the executable; derived report inputs are compiled during
publication and do not need an application-owned binder or report engine.

The source reader remains available. SQL URI/embed resources, scoped independent
view providers, declared JWT inputs, SQLX mapping and native cache services keep
their existing owners. Failed derivation or resource resolution leaves the previous
generation published. Go contract/method changes still require a rebuild.


> Packaging boundary: Exact maintained author-facing section: includes declarative configuration and behavior; excludes repository navigation, implementation/test evidence and unrelated authoring workflows. Other feature contracts remain in the canonical skill references.
