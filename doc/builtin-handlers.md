# Shipped component handlers

These handlers use ordinary `xdatly/handler.Contract[I,O]`, typed input binding,
handler DI injection, input `Init` validation, and the existing component
factory mechanism. They have no special registration or execution privileges.

## HTTP proxy

`runtime/handler/proxy.Handler` forwards to an explicitly configured endpoint
through an injected `xdatly/client/http.Provider`. Its `Input.Config` binds from
the `Proxy` constant; the HTTP request uses the native `http_request` binding.

```go
factory := custom.Factory[proxy.Input, proxy.Output](proxy.New)
```

The typed constant declares `client` options, `headers` mappings (`From` and
`To`), `forwardQuery`, and `forwardBody`. Nothing is copied from the incoming
request unless its forwarding option is declared. Map `Authorization` to the
desired gateway header explicitly. Hop-by-hop headers are removed according to
HTTP rules; status, response bytes, and repeated end-to-end headers are retained.
The handler closes the individual response body, not the borrowed client.

## AFS LoadData

`runtime/handler/loaddata.Handler[T]` reads typed rows through an injected
`afs/storage.Opener`, registered using the existing native provider:

```go
factory := custom.Factory[loaddata.Input, loaddata.Output[Row]](loaddata.New[Row])
storageProvider := provider.Static("afs", filesystem)
```

Its `Input.Config` binds from the `LoadData` constant. Declare `url`, `format`
(`json`, `json-array`, or `ndjson`), optional `compression` (`none` or `gzip`),
and optional `maxBytes`. Format and compression are never guessed from filenames;
there is no alternate-URL or `.gz` fallback. A positive byte limit also bounds
decompressed data; zero means unlimited. Type errors and trailing JSON fail
without returning partial rows. The storage owner manages its lifecycle.

## Optional generic cache

`xdatly/cache` supplies the backend-neutral byte-cache and exact-name provider
interfaces. Runtime composition can create the built-in memory backend through
`runtime/handler/provider/cache.NewMemory(capacity)`, register it under an
explicit name with `cache.New`, and supply `cache.Providers(registry)`.

A handler declares `Cache xcache.Provider` with `bind:"kind=cache,required"`
and selects its configured name through `Cache.Cache(ctx, name)`. Unknown names
fail; lookup never constructs a backend. The registry borrows supplied backends,
so other implementations can be registered without changing handler code.

Values are copied on memory-cache reads and writes. Zero capacity is unlimited;
positive capacity uses FIFO eviction, reclaiming expired entries first. Positive
TTL expires entries; zero TTL means no expiry. Callers own authorization,
namespace/key construction, TTL, and invalidation policy. The generic cache is
not a replacement for Datly's SQL-reader-specific cache machinery.
