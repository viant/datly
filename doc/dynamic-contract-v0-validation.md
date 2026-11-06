# Dynamic contract v0 validation

Validated on 2026-11-06 on macOS arm64, Go 1.26.1. The module's Go requirement
remains 1.25.8. Dependency revisions are pinned in `go.mod`/`go.sum`.

## Dependency revisions

- Xdatly main: `f15dd254a94a8f957238f700e51197c436092d57`
- Structology main: `906c12343bcc3ff5de4d5e57f8fd8a37daef9acd`
- MCP protocol main: `951fd3d1d7e0b2ca3643be48af0aa9d7c98e7059`
- Datly v1 shared-fix pin: `974db304`

Unpublished dependency commits were packaged directly from their committed Git
trees into a local Go module proxy. Tests also ran with `GOWORK=off`, so the pins
and checksums were exercised without local source replacements. Push dependencies
before consuming these revisions from another machine. The grpc compatibility
replacement names a published module and matches the existing v1 workaround.

## Feature coverage

- Shared SDK: complete root-module suite against published prerequisites.
- Structology: complete module suite and JSON race suite, including direct-interface
  map/pointer wrappers and opt-in standard decoder presence tracking.
- MCP protocol: complete module suite, caller-specific metadata failure behavior,
  and independent input/output enum wire selection.
- Datly: contract composition/cache/pruning, native locator/component/JWT binding,
  configured codec capabilities, native HTTP prepared-input/Init lifecycle,
  standard/native JSON, OpenAPI, MCP discovery/calls and view metadata.
- Additional regressions cover named body merging, flattened body aliases,
  multilevel method-bearing embeddings, forged presence markers, metadata generation
  replacement, request-scoped headers, canonical query binding and strict MCP enum
  JSON forms before query-string conversion.
- DQL route settings preserve explicit POST/PATCH `MCPRoutes` names.
- V1 contract, registry, HTTP, OpenAPI, all MCP packages and output passed with the
  new shared dependency pins.

## Broader checks

`go test -p 2 ./... -run '^$'` passed for the entire v0 module.

The full gateway and repository suites are not green on the starting master:

- Gateway: 11 failing tests on both the feature tree and a clean archive of
  `af8cb8e7f38da4826bca0cc9966a5af6765ec012` with its original dependencies.
- Repository subtree: 23 failing tests on both trees, with identical failing test
  names. They concern existing type-context, reporting and shape/code-generation
  fixtures. No new failing test names were found.
- Other focused package suites passed, including gateway/router, OpenAPI,
  service/session, service/contract, repository/content, repository/contract and MCP.

The existing vet findings are the Session copy in `service/session/state.go`,
unreachable code in `service/session/expr.go`, and zero-argument appends in
`gateway/router.go` and existing patch e2e tests. The new metadata binding scope
does not copy synchronization primitives.

A broad parallel build initially exhausted local disk space. Generated Go build
cache cleanup recovered about 70 GB; the complete compile check and broad
comparisons were rerun with lower parallelism. Repository sources and downloaded
module sources were not removed.

## Benchmarks

`service/contract/benchmark_test.go` measures linked, named-cache-hit and uncached
composite policy resolution with a real Bindly header-binding fixture. It is not
an end-to-end JWT/database/network benchmark. The earlier largest-platform-type
measurement remains recorded in the design proposal: AdorderView has 153 direct
fields and 430 visited fields, and uses the same shared `viant/x/shape` walker.

Final composite benchmark sample (three runs, median, Apple M5 Max):

| Resolution | Time/op | Bytes/op | Allocs/op |
|---|---:|---:|---:|
| Linked/no edits | 3.9 µs | 5,236 | 73 |
| Named cached variant | 5.4 µs | 6,389 | 87 |
| Uncached rewrite | 28.0 µs | 24,701 | 391 |

The final feature race suite passed after all changes. Full suites also passed for
view/state and its locator package. Bare scalar JSON bodies retain their declared
decoder destination and enum conversion, and enum JSON typing is checked before
MCP query-string encoding can erase the original JSON type.
