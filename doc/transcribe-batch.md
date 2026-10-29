# Batch transcription in Datly 1.0

Use the operation-based `datly transcribe get|patch|post|put` command on the
`v1` line. The existing single-component command remains supported:

```sh
datly transcribe get -dir . -source list.dql example.com/shop/dql/orders
```

Use `-batch` to select a directory or a module-qualified package pattern. Local
paths are relative to `-dir`; directories recurse. Package patterns retain the
existing discovery syntax, including `/...` for recursive selection:

```sh
datly transcribe get -dir . -batch ./dql/readers
datly transcribe patch -dir . -batch \
  -exclude 'example.com/shop/dql/writers/archive/...' \
  'example.com/shop/dql/writers/...'
```

`-exclude` is repeatable and matches **module-qualified package paths**, not
filenames. The module's existing `.goignore` also applies. Ignored directories,
embedded SQL resources without a route, and unselected sources are not generated.
Batch discovery selects DQL/SQL sources; use the existing single-component path
for Go-only contracts.

Pass multiple files to specify an order (multiple arguments enable batch mode):

```sh
datly transcribe get -dir . ./dql/readers/catalog.dql ./dql/readers/orders.dql
```

Selections run in argument order; files within each directory or package pattern
run in sorted path order. Overlapping selections process each file once.
Use `-batch` when passing just one file. Existing `-source` filters by basename;
`-source-name` still requires exactly one selected source. Put all flags before
the source arguments.

Generate owners before rules that import their generated types. Each rule is
compiled and published before the next rule is compiled. The CLI's operation
applies to the entire batch; it does not infer operations from directory names
or HTTP methods. For mixed writer/reader chains, Go callers can use
`Discovery.TranscribeBatch` with ordered `BatchRequest.Rules`, setting each
`BatchRule.Operation` to override `BatchRequest.Generator.Operation`.

A batch stops at its first error and identifies the failed source. Successfully
generated earlier rules remain on disk; the batch is not a transaction. CLI
output reports completed rules even when a later rule fails. An empty selection
is an error. Schema/connector flags work as they do for a single rule.

One invocation reuses workspace resolution and dependency package syntax.
Changed Go source bytes, file additions/removals, and module declarations cause
the affected package to be parsed again. Per-rule catalogs and dependency
closures are rebuilt from that syntax so later rules see newly generated types.
Borrowed authority still uses its sealed source snapshots. Packages containing
Go embed directives use ordinary loading to retain resource freshness. Nothing
is cached across invocations.

The Go result reports completed `Sources` and `Packages`, plus
`DependencyParses` and `DependencyReuses`. These count dependency package AST
loads only; they do not count every source scan, generated-package parse, or Go
build. They are useful for measuring reuse, not as a total compilation profile.
