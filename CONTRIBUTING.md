# Contributing to Datly 1.0

Develop the new implementation on `v1`; the existing `master` checkout and its
release history remain independent. See [RELEASING.md](RELEASING.md) for module
identity and publication order. No 1.0 release tag is implied by this branch.

Keep contracts typed and preserve supported behavior. Original Datly provides
the behavior and service-boundary reference. The intentional 1.0 changes separate
immutable metadata from invocation state and use explicit scoped capabilities.
[Architecture](doc/architecture.md) describes ownership: Bindly owns binding,
SQLX owns typed reads and native caches, `viant/x` owns generic type mechanics,
and the separate xdatly SDK owns public authoring contracts.

Keep parser, compilation, artifact generation, registration and execution concerns
separate. Prefer focused request/service objects to global orchestration helpers.
Changes need realistic owner-level coverage, including failure paths. Reuse
`internal/testharness` for SQLite and generated-module fixtures. Review correctness,
regressions, API shape and package hygiene in a second pass before submission.

Use the pinned release graph when validating:

```sh
export GOWORK=off GOTOOLCHAIN=go1.25.8
go test -run '^$' ./...
go test ./internal/testharness ./internal/cmd/skillpack
go test ./project/build ./mcp/...
# Run affected generated integration paths before the final full regression.
go test ./...
```

Some integration tests compile applications, open loopback listeners, or require
explicitly configured external services. Record actual commands, skipped cases
and environmental restrictions. Do not equate a compile check with runtime proof.
The final release gate includes complete regression and affected race tests.

Update public guides when behavior changes. Authoring skill sources are distributed in this repository
under `llm/`; product references read sibling `datly` and `xdatly` checkouts.
Use the exact SDK commit documented in the packaging guide. Follow [skill packaging](doc/authoring-skills.md), regenerate with
`go run ./internal/cmd/skillpack -write`, then run the same command without
`-write` to check exact reproducibility. Never edit generated embedded copies.
Preserve licenses and notices, and keep internal working histories, local
workspaces, credentials, caches and generated executables out of the release.
