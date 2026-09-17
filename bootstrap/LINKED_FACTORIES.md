# Linked Go factories

Custom projects use a user-owned `internal/datlylink` package as explicit
default-import policy. Its `init()` calls `bootstrap.UseDefaultImports` with one
exported generated component holder per selected package. `cmd/datly`
blank-imports only this link package.

Generated component package `init()` functions remain empty. There is no
`link_gen.go`, checksum, aggregate contract registry, or generated `x.Registry`.
At startup bootstrap scans only `GoBootstrap.Packages`, finds holder declarations
and tags in source, then matches them to the selected concrete holder values.

A generated holder may implement the internal typed-handler provider:

```go
DatlyHandler(name string) func() (handler.TypedHandler, error)
```

The method returns the generated custom/mutation adapter for that holder's named
factory. Discovery does not execute the factory; artifact materialization invokes
it for the selected component. Full input/output type identity is retained by
the generic holder and adapter.

Embedded package SQL and other generated assets are exposed through:

```go
EmbedFS() *embed.FS
EmbedNamespace() string
```

The same linked holder therefore supplies concrete types, typed handlers and
immutable files, while scanned source remains component metadata authority.

`x.Registry` remains available for genuinely dynamic structs and factories. It
is only a fallback when no directly linked typed handler was supplied and must
not replace linked Go authority.

This explicit link is necessary because Go reflection cannot discover free
functions or named types from an import-path string. The link package controls
what is compiled; `GoBootstrap.Packages` controls what bootstrap scans and
exposes. A scanned Go holder absent from default imports is rejected at startup.
