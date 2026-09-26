# Custom Datly projects

`project/build.Service` owns project initialization and compilation. `datly init`
and `datly build` are thin CLI adapters; developer tooling may call the same
service methods.

Initialize an existing module with `datly init -dir /path/to/app`. A new module
requires an exact Datly pin or an explicit local mapping. Existing files and Go
module choices are preserved.

The scaffold contains `cmd/datly`, `internal/datlylink`, `dql`, `generated`,
`hooks`, `resources`, and `datly.yaml`. DQL must be transcribed before building;
the build command does not silently regenerate source.

## User-owned default imports

`internal/datlylink` is application-owned selection policy. The application adds
or removes one blank import for each component package it wants compiled into
the host:

```go
package datlylink

import (
	_ "example.com/app/generated/orders/reader"
	_ "example.com/app/generated/orders/writer"
)

func init() {}
```

`cmd/datly` blank-imports only this link package. Generated component packages
have an empty `init()` and never register themselves. New link files contain no
registration or init function.
At runtime `GoBootstrap.Packages` remains the source-scanning and exposure
selection. Bootstrap uses `xunsafe.PackageTypes` to match scanned holder
declarations to linked concrete types and embedded filesystems.

Run `datly link sync -dir /path/to/app` explicitly to scan selected project
packages for actual tagged component holders and predicate/codec interface
implementations, then add missing blank imports to the existing
`internal/datlylink/link.go`. `-tags` and optional Go package patterns use the
same selection inputs as `datly build`. Sync preserves every existing import
and authored line, never removes a package, and is idempotent. The scan uses
Go source/AST, not runtime reflection of unlinked types. If a selected package
lacks an explicit `init`, sync adds an empty one in a separate additive file.
A discovered type lacking a reachability anchor gets a `reflect.TypeFor`
reference there too, never a registry entry. Transcribe and build do not invoke
sync implicitly. The link file gets imports only; runtime discovery and
exposure policy remain unchanged.

`x.Registry` is retained for genuinely dynamic types and factories. Linked,
generated Go contracts do not populate it.

Generated packages expose embedded assets through the established interface:

```go
EmbedFS() *embed.FS
EmbedNamespace() string
```

An explicitly custom component holder may expose `DatlyHandler(name)`. Standard
generated writers instead use the shared metadata-driven writer and emit no
component-specific handler factory. Package SQL remains under its readable
namespace and path.

## Build

```sh
cd /path/to/app
go list -mod=mod -deps ./... >/dev/null
datly build -dir . -o bin/app -tags production
./bin/app run -conf datly.yaml
```

Build reports component count and the resulting binary digest; it does not
count or synthesize type/factory registrations. It inherits the caller's Go environment, including `GOWORK`, build tags,
GOOS, GOARCH, CGO and GOFLAGS. It compiles `./cmd/datly` without rewriting the
link package, generating a sidecar, or persisting a checksum. A failed build
leaves the previous executable intact.

The bootstrap remains source-backed: runtime compilation reads the selected Go,
DQL, and resource source files under `BaseDir`/`ModuleDirs`. Rebuild after link,
tag, dependency, or source changes.

Verification covers a second local model module, linked reader and writer
contracts, namespaced embedded SQL, typed custom handlers, native lifecycle
methods, add/remove package selection with build tags, real SQLite persistence,
and a real TCP executable.
