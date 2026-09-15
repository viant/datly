# DQL: syntax, declarations and grammar

[All guides](README.md) · [Authoring and transcription](authoring.md) · [Hook flows](hooks.md) · [Formal EBNF](dql.ebnf)

## Scope

DQL declares a typed SQL view graph, request bindings, view controls, Go types and business hook metadata. This is the high-level authoring grammar. SQL dialect expressions and Go types are delegated to their parsers; the EBNF does not implement every SQL dialect. Application behavior belongs in Go hooks.

Use this reference to author declarations and view controls, then validate and generate pure Go with the matching Datly build. Syntax, installed capabilities and execution are separate checks; see [release status](status.md) for current boundaries.

## A shared view structure for readers and writers

Readers and writers use the same DQL structure, but each component owns its own
DQL. Their projections, relations, filters and destination packages may differ.
The outer query declares named
Datly views and their relationships. Each view subquery contains its database SQL:

```sql
#package('example.com/shop/orders/read')
#setting($_ = $input_type('OrdersInput'))
#setting($_ = $output_type('OrdersOutput'))
#setting($_ = $route('/orders', 'GET'))
#setting($_ = $connector('main'))
SELECT orders.*, items.*, type(orders, 'Order'), type(items, 'Item')
FROM (
    SELECT o.ID, o.WINDOW_START, o.WINDOW_END FROM ORDERS o
) orders
LEFT JOIN (
    SELECT i.ID, i.ORDER_ID, i.QUANTITY FROM ITEMS i
) items ON items.ORDER_ID = orders.ID
```

`orders` and `items` are the view names. `o` and `i` are local table aliases
inside their respective SQL queries. Datly annotations such as `set_limit`,
Go-type `cast` and invariant declarations belong in the outer projection and
address those named views or their projected columns. Keep them out of the SQL
inside each view. Ordinary database expressions, including a database SQL
`CAST`, remain part of the inner SQL.

For a writer, author the graph needed by that operation, using this view structure
with its own destination package. Writer hook and invariant annotations extend the outer
metadata; they do not introduce a different query language inside the views.

## Lexical conventions

- Parameter names start with an ASCII letter, followed by letters/digits/underscore. Leading underscore is reserved for declaration machinery.
- SQL names and quoting follow the selected dialect. Preserve quoted dots and case-sensitive names.
- DQL string arguments use single or double quotes. Put a valid double-quoted Go tag inside an outer single-quoted DQL argument.
- Balance nested parentheses, brackets, type arguments and comments. Commas inside a type/string/call do not split outer arguments.
- Never interpolate parameter values inside SQL quotes/comments.
- #package and #import are complete line directives. Prefer one directive per line.
- Use canonical #setting declarations; do not invent directive spellings.
- A semicolon terminates a query; metadata declarations are not DB commands.
- A successfully parsed token is not proof its provider/type/codec is installed.

## Package and import grammar

~~~~sql
#package('example.com/app/records')
#import('model', 'example.com/app/model')
~~~~

Use full module/package identity, not a filesystem path. model.Record, *model.Record, []*model.Record and model.Page[model.Record] resolve through the alias. Do not substitute a same-short-name local type.

High-level DQL generation requires a nonempty `#package` destination inside the
project module. Use its module-qualified path, or a module-relative package such
as `api/orders`; `-dir` and the source package do not supply this declaration.
Declare `$input_type('OrdersInput')` and `$output_type('OrdersOutput')` to choose
contract names, and `type(orders,'Order')` to name an entity/view shape. These
naming settings have distinct roles; contract/entity names can otherwise be
derived by the generator. Reader and writer components use separately authored
DQL and destination packages.

## Component settings

Canonical form:

~~~~sql
#setting($_ = $directive(arguments))
~~~~

| Directive | Arguments / meaning |
| --- | --- |
| route | quoted absolute path, zero or more quoted HTTP methods; default GET |
| api_key | configured header and value; no real secrets in examples |
| connector | connector name |
| input_type, output_type | Go type expression |
| dest, input_dest, output_dest, router_dest | shape and route destinations |
| file_prefix, handler_dest, lifecycle_dest, mutation_dest, resources_dest, links_dest, template_dest, support_dest | [generated filenames and destinations](#generated-filenames-and-destinations) |
| meta | description resource path |
| mcp | tool name, optional description, optional description resource |
| report, cube | optional linked input type, dimensions field, measures field, filters field, orderBy field, limit field, offset field, MCP-tool boolean |
| cubeCompose | exactly one boolean |
| cache | boolean or cache name, optional TTL; fluent modifiers below |
| cache_warmup | index column, then quoted name=value1,value2 cases/options |
| marshal | MIME type and Go marshaller type; JSON authoring |
| unmarshal | MIME type and Go unmarshaller type; JSON/XML authoring |
| format | output format; use the explicitly configured canonical name |
| date_format, case_format | format value |
| ignoreEmptyQueryParameters | exactly one boolean; absent differs from false |
| output_exclude | one or more field paths |
| output_omit_empty | one boolean |
| output_title | one title |
| const | explicit identifier and value; reject conflicting declarations |
| DocGlobalURLs, DocURLs | ordered quoted documentation resource paths |
| DocURL, DocBaseURL | one quoted rule resource or base URL |
| static_resource, static_content | quoted resource/prefix or content URL/root |
| mcp_folder | quoted folder, name and resource URI |

One route can list multiple methods. Conflicting multiple route directives are not a route list. Placeholder names occupy entire segments, for example /records/{id}, not /records/prefix-{id}.

Use authored cube settings; do not confuse metadata declarations with executable assignments.

Cache modifiers: .WithProvider(value), .WithLocation(value), .WithTimeToLiveMs(integer).
Warmup options: connector=..., indexParameter=... (also index_param/indexparam), indexMeta=true|false (also index_meta), and parameter value lists. Discover installed providers.

## Parameter and view declarations

Explicit declarations are available when the application needs a custom binding. Standard `transcribe` writer workflows derive body and Current bindings automatically; do not add them as required boilerplate.

~~~~sql
#define($_ = $ID<int>(path/id).Required())
#define($_ = $Name<*string>(query/name).Optional())
#define($_ = $Authorization<string>(header/Authorization).WithStatusCode(401))
#define($_ = $Body<[]*model.Record>(body/))
#define($_ = $Rows<?>(view/Rows) /*
 SELECT id, name FROM records
*/)
~~~~

Head: `$_ = $Name[<InputType[,OutputType]>](kind/location)`.
Question mark means infer a type, not Go any. The second type is meaningful for conversion/codec authoring.

#define establishes declared authority. Use `.Value(...)` for declared defaults. Reject conflicting definitions.

Common sources: query, path, header, cookie, form, body, http_request, view/data_view, param, const and output. Additional kinds must be registered and advertised. Header parameters default required, query parameters optional; write explicit requirements where important.

Independent view/data_view declarations use the declaration holder as canonical lookup identity. A trailing /* SELECT ... */ is declaration SQL. An implicit declaration omits (kind/location), must carry SQL, and uses supported inference; prefer explicit sources in new components.

### Complete fluent option catalog

Write the canonical option names shown here. Do not infer input, field, column or selector spelling variants. Aliases are solely user-defined; duplicate output column names are errors. Options occur once except predicates and distinct column/facet settings.

| Option / aliases | Arity | Meaning |
| --- | --- | --- |
| Required(), Optional() | 0 | suppliedness requirement |
| WithType(type) | 1 | parameter type |
| WithTag(tag), Tag(tag) | 1 | Go field tags |
| WithCodec(name,args...) | 1+ | installed codec and ordered args |
| WithStatusCode(code) | 1 | failure code, 100–599 |
| WithErrorMessage(message) | 1 | authored safe failure message |
| Cacheable(bool) | 1 | input cache policy |
| QuerySelector(view) | 1 | selector binding |
| WithPredicate(...), Predicate(...) | 1+ | optional leading group number, name, args; repeatable |
| ApplyWhenAbsentPredicate(...) | 1+ | predicate active when absent |
| When(condition) | 1 | activation condition |
| Scope(scope) | 1 | scope |
| Of(group) | 1 | owning group |
| Value(value) | 1 | declared/default value |
| Embed() | 0 | anonymous/embedded shape |
| Cardinality('One'|'Many') | 1 | result shape |
| Async() | 0 | async intent; require execution capability |
| Output() | 0 | expose declared value in output |
| WithURI(uri) | 1 | URI activation or resource reference |
| WithMCP(bool), WithPathMCP(bool) | 1 each | MCP activation policy |
| MinAllowedRecords(n), MaxAllowedRecords(n), ExpectedReturned(n) | 1 each | nonnegative bounds/exact count |
| WithConnector(name), Connector(name) | 1 | view-only connector |
| WithTypeName(type), TypeName(type), Type(type) | 1 | view-only row type |
| WithDest(path), Dest(path) | 1 | view-only destination |
| WithCache(name), WithLimit(n) | 1 each | view cache/nonnegative limit |
| WithColumnType(column,type), ColumnType(...) | 2 | view column type |
| WithColumnTag(column,tag), ColumnTag(...) | 2 | view column tags |
| WithColumnGroupable(column,bool), ColumnGroupable(...) | 2 | grouping policy |

View options do not apply to ordinary request parameters. A declaration SQL comment is last; unrecognized trailing syntax is invalid.

Canonical QuerySelector controls: Fields, OrderBy, Offset, Limit, Page, Criteria. Preserve each declared binding location and exact view identity. The selector still needs an allowed-column/method policy.

~~~~sql
#define($_ = $IDs<[]int>(query/ids).WithPredicate(0, 'in', 'r.id'))
~~~~

Predicate catalogs define actual argument contracts. Common families include equal/not-equal/comparison, in/not-in/composite membership, contains/like, null, exists, criteria, between, duration and presence conditions. A custom predicate may use canonical input and scoped component dependencies.

## SQL and view-control projections

The SQL layer supports ordinary SELECT projections, tables/subqueries, CTEs/recursive CTEs, JOIN/ON, WHERE, GROUP BY/HAVING, ORDER BY, LIMIT/OFFSET, UNION and dialect expressions. Validate actual parser/dialect support; an opaque vendor expression is not necessarily valid structural metadata.

Controls target the named views in the outer DQL graph, not local table aliases
inside their SQL. They must be standalone outer SELECT projection items, not
WHERE terms, inner SQL annotations or nested function arguments, and cannot be
the entire projection.

| Control | Syntax |
| --- | --- |
| connector/cache | use_connector(alias,name), use_cache(alias,name), cache_warmup(alias,name); quoted names or supported identifiers |
| ordering/limit | order_by(alias,'expression'), set_limit(alias,integer) |
| null/grouping | allow_nulls(alias), groupable(alias), grouping_enabled(alias) |
| order allowlist | allowed_order_by_columns(alias,'column,alias:column,...') |
| cardinality | cardinality(alias,'One'|'Many') |
| self relation | self_ref(alias,'Holder','ChildKey','ParentKey') |
| row type/file | type(alias,'GoType'), dest(alias,'file.go') |
| batching | batch_size(alias,integer), batch_concurrency(alias,integer) |
| relation concurrency | relational_concurrency(alias,integer) |
| parent publication | publish_parent(alias) |
| partitioning | set_partitioner(alias,'Type'[,integer]) |
| match policy | match_strategy(alias,'read_all'|'read_matched'|'read_derived'); related view only |
| mutation lifecycle | entity_hooks(view,'package.OrderLifecycle') |
| invariant group | invariant(view.column,'GroupName') |

Numeric control arguments are unquoted, nonnegative integer literals. set_limit(alias,0) removes the view limit; it does not erase an explicitly authored SQL LIMIT. Controls are consumed as metadata rather than sent to the DB. Allowed-order declarations can repeat without ambiguous mappings.

~~~~sql
SELECT records.*, children.*, batch_size(children,100), batch_concurrency(children,2)
FROM (SELECT r.* FROM records r) records
JOIN (SELECT c.* FROM children c) children
  ON children.record_id=records.id AND children.tenant_id=records.tenant_id
~~~~

Use explicit equalities for composite links. Parenthesized physical sources in mutation intent, such as JOIN (lookup_table) l ON ..., represent auxiliary/nonmutating data. This is distinct from a writable table and from (SELECT ...).

SELECT r.* EXCEPT INTERNAL_NOTE is Datly visibility syntax. Internal backing data may still be fetched for joins/hooks; do not confuse it with a dialect set-difference operation.

## Rich CAST, pseudo fields and tag customization

An inner Datly view can contain CTEs, nested queries and database-specific
expressions. Its result-column metadata comes from the database driver and may
be less precise than table metadata. Keep that SQL intact. Outer DQL CASTs supply
explicit Go type authority where inference is insufficient; accepting a typed
result must not depend on reconstructing its base-table expression.

Typed projection and tag annotations:

~~~~sql
#import('model', 'example.com/app/model')
-- Projection annotations:
CAST(r.bounds AS model.Bounds)
tag(r.bounds, 'sqlx:"-"')
tag(r.BOUND_UNIT, 'internal:"true"')
tag(r.name, 'validate:"required"')
~~~~

Prefer an outer CAST to declare the intended Go type, especially for rich hook-populated fields:

~~~~sql
#import('model', 'example.com/app/model')
SELECT orders.*, CAST(orders.pseudo_column AS model.GoShape),
       tag(orders.pseudo_column, 'sqlx:"-"')
FROM (SELECT o.*, '' AS pseudo_column FROM ORDERS o) orders
~~~~

The inner view SQL may contain database-specific expressions, nested queries or CTEs. SQLX result metadata owns output existence and names. An explicit outer CAST supplies the Go type even when the driver cannot report a database type; Datly does not need to infer that expression's provenance. Missing or duplicate result outputs still fail. Undeclared outputs with unknown types do not silently become strings.

For simple literal projections, `'' AS pseudo_column` defaults to Go `string` and `0 AS pseudo_column` defaults to Go `int`. These are optional syntax-based defaults, checked against the database result label and ordinal. Use outer `CAST(view.column AS int)` or `CAST(view.column AS *int)` when the exact type matters; CAST overrides literal defaults and inferred nullability. Opaque CTE/computed expressions should use an explicit CAST when the driver has no type metadata.

CAST does not imply transient DML mapping. Add `sqlx:"-"` explicitly for a logical field populated by `OnFetch` and translated into physical columns by writer `Init`. Physical codec-backed fields retain their authored SQLX mapping.

The application shorthand tag(r.name,'validate:required') expresses the same validation annotation; generated Go tags must be valid validate:"required". Prefer quoted Go-tag spelling in examples. Explicit tags refine the corresponding metadata without deleting unrelated json/sqlx tags.

A standalone custom Go CAST is a type declaration. Ordinary CAST(expression AS SQLType) AS result_alias remains executable SQL. Preserve physical codecs. Resolve model.Bounds through the import; never generate a duplicate empty Bounds type.

Internal physical columns remain SQL/DML-mapped and hidden from clients. The logical pseudo field is populated by OnFetch, translated by writer Init, and omitted from physical DML. Nested Has flags decide which backing fields change.

## Bound SQL values and declarative query context

- Named values: :ID, :Name with declared bindings.
- Declared input references such as $Name resolve to bound values.
- Input values become bound parameters. Do not quote or concatenate them into SQL.
- Embedded SQL uses ${embed:sql/shared.sql} or ${embed:namespace:path}; resources must exist before execution.
- $View.NonWindowSQL preserves the parent query without view pagination, including its arguments.
- $View.Limit, $View.Offset and $View.Page are scoped values.
- Composition uses $CubeSQL1 through $CubeSQLN for requested frames. Validate all references and resource/parameter budgets.

These fixed query-context references preserve bindings and pagination semantics. Declare optional filters through typed predicates and relation membership through JOIN keys; the generator derives parent membership and all bound query fragments.

## StructQL declaration queries

StructQL derives typed projections/index helpers from an input graph rather than a physical DB source.

~~~~sql
#define($_ = $Keys<?>(param/Keys) /*
 SELECT Id FROM /Events
*/)
~~~~

Use the actual declared graph path and supported StructQL conventions. Nested /Rows/Children paths are not database table names. Preserve explicit projection aliases, all composite-key parts and package/type identity.

Declaration queries contain plain SQL or StructQL. Express requiredness with `.Required()` or `.Optional()`, and type authority with the declaration type/fluent options; do not prefix SELECT with optional/required markers.

## Operation-based graph generation

Use separately authored reader-like graphs for `transcribe get`, `patch`, `post`
or `put`, selecting pure Go output. The operation must agree with route metadata.
`get` generates reader contracts and query resources. Writers derive request/output
shapes, authorized Current/Previous reads, internal Has markers, SyncPresence,
validation, sequencing, relation links and transaction orchestration. Authors
supply graph metadata and application Go hooks.

```sql
#package('example.com/app/records/write')
#setting($_ = $input_type('RecordsInput'))
#setting($_ = $output_type('RecordsOutput'))
#import('hooks', 'example.com/app/recordhooks')
#setting($_ = $route('/records', 'PATCH'))
#setting($_ = $connector('main'))
SELECT records.*, children.*, lookup.*,
       type(records, 'Record'), type(children, 'Child'), type(lookup, 'Lookup'),
       entity_hooks(records, 'hooks.RecordLifecycle'),
       invariant(records.START, 'Schedule'),
       invariant(records.END, 'Schedule'),
       tag(records.END, 'validate:"gtfield(Start)"')
FROM (SELECT r.* FROM records r) records
JOIN (SELECT c.* FROM children c) children ON children.record_id = records.id
JOIN (SELECT l.* FROM (lookup_values) l) lookup ON lookup.id = records.lookup_id AND 1=1
```

`invariant` places Start and End in one cohesive invariant group; the comparison rule
uses the exact generated Go field name `Start`. Use schema-resolved date/time
columns, or explicit linked Go date types. For sparse updates, the generator
backfills omitted group members from authorized Previous values without setting
Has, then validates the effective date interval. Application Go `Init` and
`Validate` hooks express additional business rules. Auxiliary rows remain readable
and never enter mutation traversal. The generator owns Body/Existing/Data
plumbing; authors do not construct it for standard generation.

High-level `transcribe` is available in the `v1` source tree. Check the installed CLI
or connected developer MCP capabilities before invoking it; an older release or
unconfigured server may not expose this workflow. Do not substitute lower-level
translation or hand-authored writer plumbing when generation is unavailable.

## Generated filenames and destinations

Use `datly transcribe get|patch|post|put` with a DQL `#package('api/orders')`
destination. Defaults are prefix-free and have no `_gen` suffix. Only roles
needed by the component are emitted; transcription does not create empty files.

| Role | Default | DQL setting |
| --- | --- | --- |
| View and helper shapes | `views.go` | `$dest('rows.go')`; individual `dest(alias,'row.go')` remains supported |
| Input / output contracts | `input.go` / `output.go` | `$input_dest('request.go')` / `$output_dest('response.go')` |
| Component routes | `router.go` | `$router_dest('routes.go')` |
| Generated Go handler or Velty factory, when present | `handler.go` | `$handler_dest('execute.go')` |
| Create-once application lifecycle, when requested | `lifecycle.go` | `$lifecycle_dest('custom.go')` |
| Generated mutation definition | `mutation.go` | `$mutation_dest('policy.go')` |
| Embedded resource filesystem, when needed | `resources.go` | `$resources_dest('sql_resources.go')` |
| Factory registration, when needed | `links.go` | `$links_dest('register.go')` |
| Velty template, when selected | `handler.velty` | `$template_dest('templates/main.velty')` |

Optional `$file_prefix('orders_')` applies to default filenames for both readers
and writers: `orders_input.go`, `orders_router.go`, `orders_mutation.go`, etc.
The prefix must start with an ASCII letter and contain only ASCII letters,
digits, underscores or hyphens. Without this setting the prefix is empty.
Exact per-file overrides take precedence and are never prefixed:

```sql
#package('api/orders')
#setting($_ = $file_prefix('orders_'))
#setting($_ = $input_dest('contracts/request.go'))
#setting($_ = $lifecycle_dest('business.go'))
#setting($_ = $support_dest('frames','state.go'))
```

This emits `request.go` in `contracts`, `business.go` in the component package,
and `state.go` for mutation frames. Other emitted roles use the explicit prefix.
`dest`, `input_dest`, and `output_dest` retain their package-splitting behavior.
Other Go destinations are visible package-local `.go` filenames; directory
traversal, hidden files and `_test.go` destinations are rejected. Template
resources may use a relative subdirectory. New settings require nonempty quoted
arguments; duplicate role settings and unknown support roles are errors.

Use `$support_dest('role','filename.go')` for separate support products. Supported
roles are `entities`, `entity_methods`, `types` (cross-package shape aliases),
`frames`, `previous`, `layout`, `actions`, `mutation_output`, `validation`, `hooks`,
`invariants`, and `indexes`. Their defaults are `<role>.go`. Generated `hooks.go` contains
mutation hook adapters; application edits belong in create-once `lifecycle.go`.
`mutation_output.go` contains mutation result logic; `output.go` owns the output
contract. `$support_dest('type:CubeInput','cube.go')` selects the filename for a
separately generated named type; otherwise its snake-case type name supplies the
default filename. Support overrides apply in each package that owns that role,
including relocated entity methods.

Multiple components in one Go package must explicitly choose distinct prefixes
or per-file destinations when files conflict. There is no inferred prefix or
collision fallback. Distinct filenames also do not resolve Go declaration-name
conflicts.

The `.datly-gen.json` manifest owns generated paths and fingerprints. Filenames
and suffixes do not establish ownership. Regeneration removes replaced,
manifest-owned files only with trusted unchanged contents; edited or unowned
files cause an error before publication. Existing shapes with authored edits
retain the normal field-merge rules at the same destination. A filename move
requires the old file to be unchanged and its declarations to have destinations.
Cross-package moves still require explicit migration. Application lifecycle
files never enter generated ownership and are never removed or overwritten.
When migrating an existing `orders_hooks.go`, keep it with
`$lifecycle_dest('orders_hooks.go')`, or move it yourself and select its new name.

Readers using the registered reader need no generated handler or lifecycle.
Mutation handlers and custom handlers retain their separate implementation roles.
SQL resource files are emitted only when the component needs packaged resources.

## Semantic checks after grammar

Resolve names, Go imports/types, provider kinds, codecs, predicates, resources, route/tool identities, constraints, relations, cardinalities, hook signatures and dialect budgets. Reject ambiguity and unknown references. Validate generated behavior, not only balanced syntax. An unavailable capability must be reported explicitly rather than silently reinterpreted.


## To-one outer JOIN shorthand

For reader and writer DQL generation, append `AND 1=1` to a relation's outer
`JOIN ... ON` equality links to declare a single holder:

```sql
JOIN (...) product
  ON product.id = inventory.product_id AND 1=1
```

The real key equalities remain required, including composite keys. The marker
affects only that relation; an unmarked sibling remains many. Explicit
`cardinality(...)` controls retain precedence. Physical joins inside an inner SQL
query keep their SQL behavior. The marker does not create a database uniqueness
constraint.

Regeneration updates generator-owned holders between `*Child` and `[]*Child`
when their recorded type and tags remain unchanged. Edited fields, changed child
identity, or untrustworthy ownership require an explicit migration; do not delete
authored code to bypass that guard.
