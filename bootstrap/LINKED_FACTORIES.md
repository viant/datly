# Linked Go packages

Custom projects use a user-owned `internal/datlylink` package as explicit
import policy. It blank-imports selected component packages and has an empty
`init()`. `cmd/datly` blank-imports only this link package.

Generated component package `init()` functions remain empty. There is no
`link_gen.go`, checksum, aggregate contract registry, or generated `x.Registry`.
At startup bootstrap scans only `GoBootstrap.Packages`, finds holder declarations
and tags in source, then uses `xunsafe.PackageTypes` to match them to concrete
types already linked into the executable.

A holder for an explicitly custom component may implement the typed-handler
provider:

```go
DatlyHandler(name string) func() (handler.TypedHandler, error)
```

Standard generated writers do not use this method or emit a per-component
factory. Their component tag selects immutable mutation metadata interpreted by
the shared universal writer. A custom holder method is invoked only when that
component explicitly declares a custom handler.

Embedded package SQL and other generated assets are exposed through:

```go
EmbedFS() *embed.FS
EmbedNamespace() string
```

The same linked holder therefore supplies concrete component identity and
immutable files, while scanned source remains component metadata authority.

`x.Registry` remains available for genuinely dynamic structs and factories. It
is only a fallback when no directly linked typed handler was supplied and must
not replace linked Go authority.

This explicit link is necessary because Go reflection cannot discover free
functions or named types from an import-path string. Runtime typelinks supply
named types; the blank-import link package controls
what is compiled; `GoBootstrap.Packages` controls what bootstrap scans and
exposes. A scanned Go holder absent from runtime typelinks is rejected at startup.
