# Remote calls in a custom handler

Remote calls use the same `xdatly/handler.Contract[I,O]` as any other custom
handler. The runtime binds and initializes the typed input before `Exec`; it
then invokes the contract through the existing `custom.New` or `custom.Factory`
registration path. Static providers bind to the handler once at registration;
the remote configuration is a normal `kind=const` input field.

`runtime/remote` supplies `Config` and `Mapper`: protocol invocation,
request/response mapping, and optional result caching through a borrowed
named `xdatly/cache.Cache`. `runtime/handler/remote` contains only ordinary
typed remote contract. Mapping paths compile through the reusable
`github.com/viant/datly/transform` package; Bindly assigns typed output.
For an Authorization-header component:

```go
import (
    "github.com/viant/datly/runtime/handler/custom"
    remotehandler "github.com/viant/datly/runtime/handler/remote"
)

handler := custom.New(&remotehandler.Handler[MyOutput]{})
```

`remotehandler.Input` has the caller `Token` and typed `Remote remotecore.Config`.
The handler's tagged `HTTP xhttp.Provider`, `MCP xmcp.Provider`,
`Cache xcache.Provider`, and `Mapper *remotecore.Mapper` fields bind once
through native Bindly when the runtime registers the pointer contract. The
runtime supplies the shared `remote_mapper` static provider; `WithRemoteMapper`
can supply a configured mapper. The handler is immutable after registration
and has no per-request mutable state. One registered instance can serve
concurrent requests, each with its own bound input. It selects one
provider from `Remote.Client.Transport` and fails if that provider is absent.
Indexed/lazy registrations get the same one-time binding when loaded.
Request-scoped binding kinds such as `header`, `query`, `body`, `input`, `param`,
and `component` are rejected on handler fields even if a provider of that kind
was registered; they belong on the per-request input.
The component declares a `Remote`
constant containing the selected client options and mappings. A caller's
header or query parameter named `Remote` cannot replace that constant.

For another application input shape, define an ordinary contract with that
shape and pass the fields explicitly:

```go
import (
    remotecore "github.com/viant/datly/runtime/remote"
    xcache "github.com/viant/xdatly/cache"
    xhttp "github.com/viant/xdatly/client/http"
    xhandler "github.com/viant/xdatly/handler"
)

type AuthInput struct {
    Token  string            `parameter:"Token,kind=header,in=Authorization,required"`
    Remote remotecore.Config `parameter:"Remote,kind=const,in=Remote"`
}

type AuthContract struct {
    Mapper *remotecore.Mapper `bind:"kind=remote_mapper,required"`
    HTTP   xhttp.Provider    `bind:"kind=http_client,required"`
    Cache  xcache.Provider   `bind:"kind=cache"`
}

func (h *AuthContract) Exec(ctx context.Context, _ xhandler.Session,
    input *AuthInput, output *AuthOutput) error {
    return h.Mapper.HTTP(ctx, &input.Remote, h.HTTP, h.Cache, input, output)
}

handler := custom.New[AuthInput, AuthOutput](&AuthContract{})
```

The HTTP or MCP provider resolves a client using the configured xdatly options.
No client lifecycle belongs to the handler. Request mappings name typed input
paths and outgoing headers, query values, URL placeholders, JSON body pointers,
or MCP arguments. Response mappings name JSON Pointer paths in the remote
document and typed output paths. The mapper checks the configuration and
mapping paths before contacting a provider or endpoint. `DecodeConfig` is
available for applications that load a declaration outside the typed const
binder and need strict JSON decoding.

The same transformation capability is available outside remote handlers.
`transform.CompilePointer` plans RFC 6901 document paths, while
`transform.CompileSelector` plans exact struct/map paths with array brackets.
`transform.CompileFor(sourceType, destinationType, mappings)` builds an
immutable typed plan; `plan.Apply(ctx, source, &destination)` binds its selected
values through Bindly. A pointer can also assign into a JSON-like outbound
object. Missing and null required mappings fail, while a present empty slice
remains a valid value.

The codec factory at `runtime/handler/codec/transfer.Factory` ports the original
Datly `transfer` tag. Authoring selects its Go type through the normal type
catalog, for example `#import('transfer','github.com/viant/datly/runtime/handler/codec/transfer')`
followed by `WithCodec('transfer.Factory')`. The bare word `transform` is not a
discovery alias:

```go
type Context struct {
    UserID int64 `transfer:"from=User.ID"`
}
```

Its `New` compiles paths from the declared source and destination types; its
`Value` is safe for concurrent calls and never caches the first source shape.
An optional `codec=name` tag uses a caller-supplied named codec instance;
unknown names fail during compilation.

Caching is enabled only by a positive `cache.ttl` and a `cache.name` such as
`{"name":"auth","ttl":"5m","partition":["Token"]}`. Register a backend
under exactly that name with `cacheprovider.New`, and add
`cacheprovider.Providers(registry)` to the component's ordinary providers.
For example:

```go
backend, err := cacheprovider.NewMemory(2)
if err != nil { return err }
registry, err := cacheprovider.New(map[string]xcache.Cache{"auth": backend})
if err != nil { return err }
componentProviders := cacheprovider.Providers(registry)
```

`cacheprovider.NewMemory(capacity)` sets the backend entry cap; zero means
unlimited. The old declaration-level `maxEntries` field is removed and strict
`DecodeConfig` rejects it. A cache-enabled call fails before network activity
if its named provider is absent or unregistered.

Cache keys include configuration, concrete input/output types, mapped request
values, and explicit `cache.partition` values. Call
`mapper.Invalidate(ctx, &config, registry, values...)` to invalidate one
partition, or omit values to invalidate all entries tracked by that mapper;
it returns `(removed int, err error)`. Diagnostics use
`mapper.CachedEntries(ctx, &config, registry) (count int, err error)`.
Invalidation fences in-flight writes within that mapper. The public cache
contract has no cross-process partition scan or atomic generation operation;
distributed invalidation needs a backend with that additional capability.
Zero or omitted client timeout and byte limits mean unlimited; the caller's
context still controls cancellation.

Authorization output can feed a normal component dependency and parameter
binding, such as `Auth.Context` to `Context`, followed by SQL
`$criteria.In("t.id", $Context.Allowed.Entity)`. See the SQLite remote criteria
test for the complete route and forged input cases.
