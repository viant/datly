# Constants and instance files

Use one authored project for E2E and production by selecting a trusted instance
constant file. Instance values are never read from HTTP, MCP, or other invocation
input. SQL identifiers are rendered in a private DB-bound copy; DQL, generated SQL
resources, and authored constant defaults retain their placeholders and values.

## Declare defaults

```sql
#package('api/records')
#setting($_ = $route('/records', 'GET'))
#setting($_ = $const('project', 'authored-default'))
#define($_ = $ID<int>(const/ID).Value('7'))
SELECT id, name FROM `$project.ds.records` WHERE id=:ID
```

`const` parameters still use Datly's normal typed binding. `:ID` is a value bind.
A standalone `$ID` emitted by a SQL template also remains a value bind. Binding a
value does not make it a table or project identifier.

## Load an instance file

A file is one flat mapping, in YAML or JSON. These are equivalent examples:

```yaml
# e2e.yaml
project: shop-e2e
ID: 0
```

```json
{
  "project": "shop-e2e",
  "ID": 0
}
```

A production file can contain only the changed key:

```yaml
# prod.yaml
project: shop-prod
```

Here E2E binds `ID=0`; production retains the authored default `ID=7`.

The CLI accepts `-const` on `validate`, `transcribe`, `run`, and `start`:

```sh
datly validate -dir ./app -const ./e2e.yaml \
  -schema -connector main -driver sqlite3 -dsn '${Database}' \
  example.com/app/source

datly transcribe get -dir ./app -const ./e2e.yaml \
  -schema -connector main -driver sqlite3 -dsn '${Database}' \
  example.com/app/source

datly run -conf ./app.yaml -const ./e2e.yaml
```

For the SQLite commands, include `Database: /absolute/path/to/existing.db` in the
file and use SQLite-compatible authored SQL. Schema discovery retains its
existing read-only SQLite connection policy. BigQuery rendering does not require
a cloud connection; actually querying BigQuery requires the linked driver and
connector appropriate to that deployment.

Standalone configuration can select the file itself:

```yaml
ConstURL: e2e.yaml
BaseDir: ./app
GoBootstrap:
  Packages:
    - example.com/app/records
Connector: main
Connectors:
  - Name: main
    Driver: sqlite3
    DSN: '${Database}'
ContentURL: 'https://assets.example.test/${Stage}/'
```

`Database` and `Stage` must be present in the selected file for those paths.
`ConstURL` is relative to the standalone config location. CLI `-const` overrides
that selection and is relative to the process working directory. The file URL
itself is literal: constants cannot select their own file.

Only one file is loaded. Repeating `-const` selects the last flag value; it does
not merge files. The frozen file snapshot lasts for the server's lifetime.
Source reloads reuse it; start a new instance to change the file or connectors.

## Precedence and validation

- A present file key overrides the corresponding authored constant. An absent
  key retains its default. Zero, `false`, and `""` are present values.
- Names match case-insensitively. Duplicate names, including case-only duplicates,
  are errors. Names must be identifiers. SQL context names `Unsafe`, `View`,
  `predicate`, `criteria`, `SQLBindings`, and `SQLInstanceConstants` are reserved.
- Files accept string, number, and boolean scalars. Null, objects, arrays, YAML
  aliases, custom tags, and multiple YAML documents are rejected. Use a quoted
  string when the existing typed constant converter expects encoded text.
- Numeric text is retained without an intermediate floating-point conversion;
  large integers retain their precision. Native Bindly conversion validates the
  declared constant type. A value that cannot convert fails before discovery or
  standalone connection opening.
- The file may also define names used solely by configured resource paths.
  Unknown referenced path or identifier names fail rather than selecting a
  fallback. A file name that conflicts with a non-constant parameter is rejected.
- Duplicate or conflicting **authored** declarations still fail before an
  external override is applied. A deployment override is not a second authored
  declaration.

## SQL identifier and quoting boundaries

A qualified emitted selector rooted in a constant is an identifier reference:

```sql
SELECT * FROM $project.ds.records
SELECT * FROM `$project.ds.records`
SELECT * FROM `${project}.ds.records`
SELECT * FROM [$project.ds.records]
SELECT * FROM [${project}.ds.records]
```

The SQL template and column-discovery paths use the same renderer. Registration
compiles immutable constant slots; evaluation emits their values into DB-bound
SQL. No expanded SQL template is stored during registration. Each replaced
fragment must be nonempty and contain only ASCII letters, digits, underscores,
or hyphens. Values supply fragments, not SQL syntax or enclosing quotes. Authors
choose the identifier quoting required by their database.

Single-quoted strings, double-quoted text, SQL comments, and dollar-quoted strings
retain their contents. Velty conditions, calls, loop variables, `$predicate`,
`$View`, declared query selectors, and normal scalar binds retain their existing
owners. Replacement is not recursive.

**Native grammar boundary:** the pinned SQL parser rejects unquoted
`${project}.ds.records` during DQL compilation. Use `$project.ds.records` or the
quoted forms above. The DB-bound renderer can render that braced spelling, but
this does not make it supported throughout the DQL compilation pipeline.

## Existing `$Unsafe` behavior

`$Unsafe.Table` and `${Unsafe.Table}` retain their existing raw SQL-template
semantics. They can emit an entire trusted table string, including its quoting;
ordinary SQL parameters remain bound. This is an explicit raw-text escape hatch,
not the project-fragment mechanism above. Quoted `$Unsafe` text remains quoted
SQL data. Existing authored constant-table metadata resolution is retained. For a direct
table source, discovery reads constraints from the evaluated table target, so an
instance override of a raw table constant also selects that table's metadata.

## Resource paths and ownership

Constants resolve when accessing SQL URI/embed resources through the same named
Bindly filesystem. The wrapper changes the access argument, keeps the original
URI and bytes, and adds no content cache or default namespace. Existing `fs.FS`
path validation and `os.Root` restrictions still apply.

Standalone creates a detached access configuration for module directories,
connector DSNs and secret URLs, dependency URLs, static content URLs, job storage
and notification URLs, JWT certificate URLs, OpenAPI export URLs, MCP folder
paths/URIs, and OTLP endpoint URLs. Authored strings retain placeholders. Read
cache locations resolve on a detached cache configuration.

SQLX owns read caching. Standalone adds the resolved connector identity and
instance-value identity to native cache namespaces; query keys use the rendered
SQL and bound arguments. No second resource or row cache is introduced.

There is no separate substitution-map/profile configuration, global replacement
pass, reverse replacement, or automatic `constants.yaml` dependency loading.
Documentation dictionary `docSubstitutes` retains its separate documentation role.
