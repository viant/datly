# Your first typed API

[All guides](README.md) · [Runnable setup](../README.md#try-a-typed-api-locally)

Run the README's snapshot demo. It uses automatic project discovery, the canonical
binding engine, SQLX typed reads and buffered DML against a disposable SQLite DB.
The command prints discovery counts and the binary path. The generated command
uses build-owned linking; adding an application `Register` function is unnecessary.

## What declares the API

The demo copies the project fixture's component, hook and model packages. Its
component metadata declares GET `/records/{id}` and POST `/records`:

```go
type Component struct {
    Read xdatly.Component[Input, Output] `component:"Read,path=/records/{id},method=GET,connector=main,view=records"`
    Write xdatly.Component[hooks.Input, hooks.Output] `component:"Write,path=/records,method=POST,connector=main,handler=hooks.NewWrite"`
}
```

`xdatly` is `github.com/viant/xdatly`; `hooks` belongs to the application.
The reader binds required `ID` from path `id`. Its `Rows` holder selects the
`records` view and `sql:"uri=build_records:queries/read.sql"`. The package resource
manifest gives that SQL an explicit namespace; the query binds `$ID` as a value.
The writer binds `data` from the body, runs input `Init`, queues an insert using
the scoped DML capability and returns output finalized by the engine.

The fixture sources are [component](../project/build/testdata/app/records/component.go),
[hook](../project/build/testdata/app/hooks/write.go) and
[model](../project/build/testdata/app/models/record.go). The setup script changes
the model import to the demo's own module; it does not change their behavior.

## Validate and rebuild

From the Datly source module, with the demo directory from README:

```sh
go run ./cmd/datly validate -dir "$DATLY_DEMO_DIR" -format json example.com/buildapp/records
go run ./cmd/datly build -dir "$DATLY_DEMO_DIR" -o bin/records
```

Read `completed` and `skipped` in validation output. Static validation does not
prove database execution. `validate -h` lists exact schema options. Schema
inspection needs an explicitly selected connector, linked driver and authorized
schema. Go/type/hook changes require a rebuild and restart of this demo.

## Your own project

Use [datly init/build](project-build.md) with your module and real dependency
choices. Put generated or authored component packages anywhere selected by Go's
normal package traversal. DQL transcription is an explicit step before building
its generated Go product; build does not implicitly run transcription or user
generators. Route exposure can select a package pattern independently of imported
private dependencies. SQL/template/document resources retain their manifests.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Missing Datly module | Use the setup script to map this local checkout; SDK and native dependencies use published pins. |
| Dependency lookup/checksum failure | Resolve the exact requirements in the app module/workspace; replacements are not inherited from dependency modules. |
| SQLite compiler error | Use a working C compiler and CGO-enabled Go build. |
| Port in use | Change `Endpoint.Address` in the demo config and curl URLs together. |
| Route missing | Check build package selection, runtime exposure, method/path, then rebuild. |
| Missing source/resource | Preserve the recorded source and resource paths; copying only the binary is insufficient. |
| Generated linker conflict | Preserve authored edits and inspect build ownership; do not overwrite edited generated files blindly. |

The demo demonstrates readers, a custom writer, native lifecycle methods and
OpenAPI. It does not provision authentication, cache backends or async services.

Parent acceptance passes real TCP reader/mutation endpoints and add/remove
package rebuilds for the custom-build fixture. The documentation author's sandbox
run was restricted; it does not leave that parent acceptance blocked. Custom
builds retain the source-backed deployment requirements described here.
