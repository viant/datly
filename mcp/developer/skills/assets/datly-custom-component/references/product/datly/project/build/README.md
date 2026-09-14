# Custom Datly projects

`project/build.Service` is the shared initializer/build owner. `datly init` and
`datly build` are thin CLI adapters; a developer MCP tool can call the same
`Init(ctx, InitRequest)` and `Build(ctx, Request)` methods.

Initialize an existing Go module with `datly init -dir /path/to/app`. A new
module requires its name and an exact Datly pin, or explicit development mappings.
There is no assumed public Datly 1.0 version. The `v1` release copy pins the
published SDK `v0.5.4-0.20260914204318-08752d9972c1` and native modules.

```sh
datly init -dir /path/to/app -module example.com/app \
  -local github.com/viant/datly=/path/to/datly \
  -pin github.com/viant/xdatly@v0.5.4-0.20260914204318-08752d9972c1
```

For published dependencies, `-pin module@vX.Y.Z` accepts exact canonical versions
and pseudo-versions; `latest`, branches and partial versions are rejected. Local
mappings pair local-only `v0.0.0` requirements with explicit replacements. Existing
workspace/module choices are retained; conflicting requests fail during new or
incomplete initialization. If `pkg/dependency` or `internal/datlylink` already
exists, init returns without adding files or updating dependencies; use Go module
commands for later changes.

Go does not inherit transitive replacement directives. Carry the required gRPC
alias from the release manifest into the application module. The public quickstart
setup script copies the exact dependency graph and sums and maps only the main
Datly module to its checkout. Changed native/SDK checkouts may be selected for
explicit development, but release validation uses the published graph.

The scaffold contains `cmd/datly`, `dql`, `generated`, `hooks`, `resources`, and
`datly.yaml`. Existing files are left byte-for-byte intact. Put generated Go
components under `generated` (or any normal Go package), and authored lifecycle
methods/factories in the owning package or an imported hook package. Configure
connectors and package exposure in `datly.yaml`. DQL should be transcribed to Go
component holders/shapes with the established transcription workflow first;
this build service does not implicitly regenerate authored DQL.

```sh
# After adding imports, update dependency metadata using ordinary Go tooling.
cd /path/to/app
go mod tidy
datly build -dir . -o bin/app -tags production
./bin/app run -conf datly.yaml
```

Build inherits `GOWORK`, `GOOS`, `GOARCH`, `CGO_ENABLED`, `GOFLAGS`, the selected
Go toolchain, and the caller's other Go settings. It never forces workspace-off
or changes dependency pins. Missing module requirements/checksums are ordinary
Go errors; resolve them in the project's module/workspace context. API callers
may supply a complete `Request.Env` for an explicit target. Optional positional
build arguments are Go package patterns; the default is `./...` in the project
module. Select another workspace module explicitly to discover its components.
Imported types and hook dependencies are available without exposing their routes.

Go's package selection feeds `x/module.BuildSelection`; `bootstrap.PackageDiscovery`
reads only selected Go files. `x/loader/ast` and `x/shape` retain metadata/type
ownership. The build follows component input/output shapes and their named
structural references, linking native types (and therefore their lifecycle
methods). Component holder packages are linked even when their contracts are
in another package. Handler metadata must name an exported, zero-argument factory
returning `handler.Contract[I,O]`, `handler/mutation.Definition[I,O]`, or runtime
`TypedHandler` (optionally with `error`). Contract/Definition bridges are checked
against component type arguments by the Go compiler. TypedHandler's reflected
contracts, nil results and factory errors are checked at runtime registration.
Discovery does not execute factories, package initializers, or user generators.

The generated private `internal/datlylink` code registers into the existing
`x.Registry` and supplies its Go selection to the existing standalone/transcribe
pipeline. Authors need no `init()`, `Register()`, blank-import list, or aggregate
registration function. Adding/removing component packages refreshes linking on
build. A Go overlay hides the previous linker during discovery, so an old import
cannot keep a removed package alive. The generated command is scaffolded once;
user edits remain owned by the user.

Package resource manifests (`.datly-gen.json`) and `fs.FS` ownership are unchanged.
Named cross-package SQL uses e.g. `sql:"uri=my_namespace:queries/read.sql"`;
relative URIs belong to the component's package. Existing `go:embed` declarations
remain in compiled packages. This workflow is source-backed: its generated
selection contains absolute source/module paths, and runtime compilation still
reads selected metadata and resource files. It is not a relocatable, source-free
binary bundle. Source/tag/dependency changes require rebuilding; concurrent source
editing during a build is unsupported.

Build fails for inaccessible/private structural declarations, uninstantiated
shape generics, missing or unsupported factories, malformed component metadata,
and native compiler errors. Arbitrary concrete factory return types are not
adapted by guessing method signatures. The compiler reports Go internal-package
visibility and target/CGO restrictions. A generated-file hash prevents overwriting
an edited linker. An exclusive build lock prevents overlapping builds. Failed
compilation restores the previous linker and retains the previous executable;
an interrupted process may leave `.build-lock`, which must be removed only after
checking that the build is no longer running.

Verification includes a real workspace with a second local model module, an
initialized/compiled command, an authored SQLite mutation hook, native lifecycle
methods, namespaced SQL assets, addition/removal of a separate component package,
build tags, GOOS/GOARCH file selection, and repeat-build preservation. Parent
acceptance launches the generated executable and passes real TCP read/mutation
endpoints plus package add/remove rebuilds. The documentation author's restricted
sandbox also passed the shared in-process HTTP fixture; its listener denial is
an environment-specific observation, not an outstanding parent TCP acceptance
gap. Neither result changes the source-backed deployment contract above.
