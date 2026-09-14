# Initialize and build a custom project

[All guides](README.md) · [Quickstart](quickstart.md)

The local `v1` release copy provides `datly init` and `datly build`.
This guide does not imply a published Datly 1.0 CLI version.
The [build owner](../project/build/README.md) documents the complete API.

For an existing Go module with its selected dependencies:

```sh
datly init -dir .
datly build -dir . -o bin/app
./bin/app run -conf datly.yaml
```

`datly` here means the CLI compiled from this checkout. The main module is
still local; the SDK and native dependencies are pinned published modules.
The README [setup script](examples/prepare-demo.py) carries the full selected
graph, checksums and the required gRPC alias into a disposable project. It only
maps Datly itself to the local checkout.

For explicit initialization of a new module:

```sh
datly init -dir ./app -module example.com/app \
  -local github.com/viant/datly=/absolute/path/to/datly \
  -pin github.com/viant/xdatly@v0.5.4-0.20260914204318-08752d9972c1
```

Relative `-local` paths resolve from the new project directory. Go does not
inherit replacements from dependency modules: preserve the gRPC alias from the
release manifest in the application module as well. The setup script handles
this automatically. Additional local mappings are only needed when deliberately
developing changed dependencies. Do not add them to Datly's release `go.mod`.

`-pin module@EXACT_VERSION` accepts verified canonical versions and
pseudo-versions. Mutable `latest`, branches and partial versions are rejected.
Local-only requirements use `v0.0.0` (or the module's semantic major) paired with
explicit local mappings. Existing pins, replacements and authored files are
preserved; conflicting requests fail for new or incomplete initialization.
If a custom Datly module and its dependency package already exist, `init` exits
successfully without adding files or updating dependencies. This recognizes
`pkg/dependency` and the generated `internal/datlylink` package. Subsequent
dependency changes belong to Go module commands.

## Automatic traversal and linking

Init scaffolds `cmd/datly`, `dql`, `generated`, `hooks`, `resources` and
`datly.yaml`. Build defaults to `./...` in the project module and follows
component inputs/outputs and reachable named types. Normal Go imports express
actual code dependencies. No user-maintained `init()`, `Register()`, blank-import
list or aggregate registration function is required. The private generated
linker registers into the existing registry internally.

Go selects files using the real workspace, tags, GOOS, GOARCH and CGO settings.
Additional positional Go package patterns select other workspace modules when
needed. Imported dependency types do not expose their routes by themselves.
Factories must be exported zero-argument functions returning direct
`handler.Contract[I,O]`, `mutation.Definition[I,O]` or runtime `TypedHandler`,
optionally with `error`. Discovery does not execute factories or initializers.
Native compilation validates typed bridges; runtime registration checks dynamic
handler contracts and factory failures.

After adding dependencies, resolve the build graph with
`go list -mod=mod -deps ./...` in the application module. The pinned Structology
dependency currently blocks `go mod tidy` through one of its own test imports;
see [dependency tooling](status.md#dependency-tooling). Build preserves module choices and reports Go's errors. Add/remove
component packages and rebuild to refresh linking. Previous output is retained
on ordinary compile failure; edited generated-linker conflicts fail explicitly.

Parent acceptance passes real TCP reader/mutation endpoints and add/remove
package rebuilds for the custom-build fixture. The documentation author's sandbox
run was restricted; it does not leave that parent acceptance blocked. Custom
builds retain the source-backed deployment requirements described here.

## Deployment contract

This custom executable is source-backed. Its build selection records source and
module paths, and runtime compilation still reads metadata and declared resource
files. Deploy the corresponding source/modules, SQL, templates, configuration
and resource manifests at those paths, or rebuild for the deployment layout.
Preserve target/tag/dependency choices. Copying only this executable is insufficient.

Generated resource products can embed SQL, YAML documentation, static files and
MCP resources when their own generation workflow declares them. The standalone
resource-deletion tests prove those particular embedded assets survive removal;
they do not prove that automatic project builds embed every metadata/source input.
Do not label a source-backed custom build a standalone source-free bundle.
