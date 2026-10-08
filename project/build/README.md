# Custom Datly projects

`project/build.Service` owns project initialization and compilation. `datly init`
and `datly build` are thin CLI adapters; developer tooling may call the same
service methods.

Initialize an existing module with `datly init -dir /path/to/app`. A new module
requires an exact Datly pin or an explicit local mapping. Existing files and Go
module choices are preserved.

The scaffold contains `cmd/datly`, `internal/dependencylink`, `dql`, `generated`,
`hooks`, `resources`, and `datly.yaml`. DQL must be transcribed before building;
the build command does not silently regenerate source.

Initialized application mains use the shared `cmd/developer.Service` dispatcher.
The same linked executable supports `transcribe`, `validate`, `link sync`,
`init/build`, and `run/start`; authoring resolves the project's native linked
predicate/component types without an overlay CLI or manual type registration.
The stock executable delegates to the same command implementation, including
the existing read-only schema discovery rules.

For an existing custom main, preserve its runtime injections:

```go
runtimeCommands := command.Service{/* existing providers, holders, auth and logging */}
status := (developer.Service{Runtime: runtimeCommands}).Run(ctx, args, stdout, stderr)
```

Imports are `github.com/viant/datly/cmd/command` and
`github.com/viant/datly/cmd/developer`. Source linking/exposure policy remains in
the project's existing link package and configuration. Discover/synchronize it
using the native link mechanism, rebuild the custom binary, then invoke its
operation-based `transcribe get|patch|post|put` command. Merely adding a predicate
import to another process does not link that type into the authoring executable.

## User-owned default imports

`internal/dependencylink` is application-owned selection policy. The application adds
or removes one blank import for each component package it wants compiled into
the host:

```go
package dependencylink

import (
	_ "example.com/app/generated/orders/reader"
	_ "example.com/app/generated/orders/writer"
)
```

`cmd/datly` blank-imports only this link package. Generated component packages
have an empty `init()` and never register themselves. New link files contain no
registration or init function.
At runtime `GoBootstrap.Packages` remains the source-scanning and exposure
selection. Bootstrap uses `xunsafe.PackageTypes` to match scanned holder
declarations to linked concrete types and embedded filesystems.

Run `datly link sync -dir /path/to/app` explicitly to scan selected project
packages for actual tagged component holders and predicate/codec interface
implementations, then add missing blank imports to
`internal/dependencylink/link.go` by default. Sync creates a missing directory
and link file without an authored seed. Use `-link-package pkg/componentlink`
(or another module-relative directory) with `datly init`, `datly build`, and
`datly link sync`. A bare name still means `internal/<name>`, including the
legacy `datlylink` shorthand. Existing non-main package declarations take
precedence over the directory basename. Absolute paths, traversal, nested
modules, vendor paths, and symlinked destination paths are rejected.
`-tags` and optional Go
package patterns use the
same selection inputs as `datly build`. Sync preserves every existing import
and authored line, never removes a package, and is idempotent. The scan uses
Go source/AST, not runtime reflection of unlinked types. If a selected package
lacks an explicit `init`, sync adds an empty one in a separate additive file.
A discovered type lacking a reachability anchor gets a `reflect.TypeFor`
reference there too, never a registry entry. Transcribe and build do not invoke
sync implicitly. The link file gets imports only; runtime discovery and
exposure policy remain unchanged.

Existing imports are checked only in the active build-selected files. Sync uses
an overlay, so discovery works even when an application already imports a missing
link package. It compiles the complete planned link package and reachability
helpers without executing application code. Discovery/validation failures leave
the working tree unchanged; module and workspace metadata are protected from
implicit Go updates. Update dependencies separately if Go reports missing sums
or an outdated module graph.

Retention uses a named package-level value for every discovered type category:
component holders, lifecycle hooks, predicates and codecs. The convention is
`var _anchorTypeName = reflect.TypeFor[TypeName]()`. Generated component files place these declarations directly below `init()`, at package scope. New link-support files place their anchors below their generated `init()`; additive link sync preserves existing declarations. No explicit `init` assignment
or registration call is required for the anchor. Independently generated
component files qualify shared lifecycle anchor names with the holder name to
avoid duplicate declarations. Existing named anchors remain recognized regardless
of their spelling; link sync does not rename authored declarations. Public
component instance variables support explicit wiring and are not sufficient
retention anchors by themselves.
Configured Go flags and automatic or explicit vendor selection remain intact.

Publication checks for concurrent destination edits and atomically replaces each
file, preserving existing permissions. Helpers are published before the link file.
This is not a multi-file transaction: a filesystem failure or cancellation during
publication may leave completed replacements and newly created directories.
The error identifies the failed path and the files already published. No rollback
or stale-import removal is attempted.

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
