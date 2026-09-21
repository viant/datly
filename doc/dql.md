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
#setting($_ = $case_format('lc'))
#setting($_ = $route('/orders', 'GET'))
#setting($_ = $connector('main'))
#define($_ = $Orders<[]*Order>(output/view))
SELECT orders.*, items.*, type(orders, 'Order'), type(items, 'Item')
FROM (
    SELECT o.ID, o.WINDOW_START, o.WINDOW_END FROM ORDERS o
) orders
LEFT JOIN (
    SELECT i.ID, i.ORDER_ID, i.QUANTITY FROM ITEMS i
) items ON items.ORDER_ID = orders.ID
```

The root `orders` binds to `OrdersOutput.Orders []*Order` through
`output/view`; its JSON key is `orders`. `type(orders, 'Order')` names each
root row, and `type(items, 'Item')` names the related rows. The output type
setting alone does not specify a result field. `case_format('lc')` applies
lowerCamel naming to the envelope and nested fields through the runtime
Structology JSON marshaler. Use this global policy for ordinary naming.
For a deliberate rename, prefer `format:"name=CustomerName"`; the serializer applies `lc` to that name and emits `customerName`. An explicit
nonempty `json` name is an exact override of format-name/global casing, not the
recommended rename mechanism.

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
- Metadata integer options accept decimal text, including quoted text where the option uses `parseIntArg`; outer numeric view controls require unquoted integer literals. There is no float-valued view limit syntax.
- Boolean metadata options using `strconv.ParseBool` accept `true`, `false`, `1`, `0`, `t`, `f`, `T`, `F`, `TRUE`, `FALSE`, `True` and `False`, with optional quotes. Prefer `true`/`false`. Cache's first argument and warmup booleans have their separate rules below.
- Balance nested parentheses, brackets, type arguments and comments. Commas inside a type/string/call do not split outer arguments.
- Never interpolate parameter values inside SQL quotes/comments.
- #package and #import are complete line directives. Prefer one directive per line.
- Use canonical `#setting` declarations. The same case-insensitive scanner also accepts `#settings`; both enter the same setting parser (`transcribe/dql/directive_scan.go`).
- A semicolon terminates a query; metadata declarations are not DB commands.
- A successfully parsed token is not proof its provider/type/codec is installed.

## Package and import grammar

Syntax fragment; adapt within the [complete reader contract](dql.md#a-shared-view-structure-for-readers-and-writers).

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
naming settings have distinct roles. In complete reader examples, also declare
a named, typed `(output/view)` holder matching the root row type, as above;
do not rely on generated default names or an implicit `Data` field. Reader and writer components use separately authored
DQL and destination packages.

## Transcription CLI and source layout

Use the high-level command from the project module:

```sh
datly transcribe get -dir . example.com/shop/source/ordersread
datly transcribe patch -dir . example.com/shop/source/orderswrite
```

These are CLI invocations. The positional argument is the **module-qualified
source package** containing one component, not the destination package, a DQL
filename or a Go API expression. Put flags before that argument. Source discovery
loads the DQL and imported Go package authority from the configured workspace;
`#package` selects the destination within the project module. For example,
`source/ordersread/Orders.dql` can declare `#package('api/orders/read')`.
Imports resolve package identities through the workspace/module resolver, not
relative to the DQL file as arbitrary filesystem paths. Existing imported types
must resolve; future generated local shapes use their declared destination.

Select `get`, `patch`, `post` or `put` explicitly. The operation must agree with
the route. GET is a reader; the others generate mutation graphs. HTTP route
metadata also accepts DELETE, HEAD, OPTIONS, TRACE and CONNECT, but those are
not additional transcription operations. Go is the default target. The command
requires exactly one component in the selected source package. Both `.dql` and `.sql` sources are discovered; route metadata identifies component sources. Schema discovery
uses configured connector/driver inputs; it is not permission to guess column
types. See `cmd/datly/generate.go`, `cmd/datly/validate_schema.go` and
`transcribe/discovery_imports.go` for the CLI and discovery owners.

## Component settings

Canonical form:

Syntax fragment; adapt within the [complete reader contract](dql.md#a-shared-view-structure-for-readers-and-writers).

~~~~sql
#setting($_ = $directive(arguments))
~~~~

The following inventory covers the component-setting switch and the separate
route parser. Counts describe **actual parser acceptance**, including permissive
branches. `N+` means no upper bound is enforced here, not that surplus arguments
have useful meaning. Author only the meaningful arguments listed. Names are
matched case-insensitively; use the spelling below. Quote textual values.

| Setting | Accepted count | Consumed arguments / checks |
| --- | --- | --- |
| `route` | 1+ | absolute quoted URI, then quoted HTTP methods; default GET; one route only |
| `api_key` | 2+ | header, value; further arguments ignored |
| `connector` | 1+ | last argument is default connector |
| `sequence_strategy` | exactly 1 | quoted `transient` or `reservation`; singleton, no modifiers; omitted selects native dialect default |
| `input_type`, `output_type` | 1+ | last argument is contract type |
| `dest`, `input_dest`, `output_dest`, `router_dest` | 1+ | last argument is nonempty destination |
| `file_prefix`, `handler_dest`, `lifecycle_dest`, `mutation_dest`, `resources_dest`, `links_dest` | exactly 1 | nonempty quoted value; no fluent tail; duplicate setting fails |
| `support_dest` | exactly 2 | quoted role and filename; no tail; duplicate role fails |
| `meta` | 1+ | last argument is nonempty description resource path |
| `mcp` | 1+ | name (nonempty), optional description, optional description path; arguments after third ignored |
| `mcp_folder`, `mcp_skill_folder` | exactly 3 | namespace, root, URI prefix; no tail; skill variant selects `Skills: ["."]` |
| `report`, `cube` | 0+ | first eight: linked input type, dimensions, measures, filters, orderBy, limit, offset, MCP-tool boolean; later arguments ignored |
| `cubeCompose` | 1–5 | enabled; optional MCP-tool flag, max cubes, max limit, timeout milliseconds |
| `cache` | 1+ | enabled boolean or name, optional TTL; later positional arguments ignored; fluent options below |
| `cache_warmup` | 1+ | index column, then name=value options; repeated calls merge cases with conflict checks |
| `marshal`, `unmarshal` | 2+ | MIME and Go implementation type; later arguments ignored |
| `format` | 1+ | last argument; `tabular_json` normalizes to `tabular` |
| `date_format`, `case_format` | 1+ | last argument; date layout and global name policy respectively |
| `ignoreEmptyQueryParameters` | exactly 1 | boolean; absent differs from explicit false |
| `output_exclude` | 1+ | all arguments are output field paths; repeated calls append |
| `output_omit_empty` | exactly 1 | boolean |
| `output_title` | exactly 1 | title |
| `const` | 2+ | identifier and value; later arguments ignored; names must be Go identifiers and unique case-insensitively |
| `DocGlobalURLs`, `DocURLs` | 1+ | all nonempty documentation resource references; no tail |
| `DocURL`, `DocBaseURL` | exactly 1 | nonempty rule reference or base URL; no tail |
| `static_resource`, `static_content` | exactly 2 | quoted namespace/root or content URL/root; one static declaration; no tail |

Sequence defaults are original MySQL transient allocation, PostgreSQL 10+ exact
native nextval values, and SQLite native reservation. The MySQL allocator table
is available only with explicit `reservation`. `maxid` and unrecognized values
are rejected during transcription. Generated settings configure the root Data
owner; nested components inherit it and conflicting overrides fail. Original
MySQL allocation needs a separate connection, can wait on caller-held locks,
and executes source defaults/triggers before rolling its own transaction back.

Target-specific settings `useTemplate` and `template_dest` are documented only
in the [final DQL + Velty section](#dql--velty-separate-language-layer).
`#setting` is canonical and `#settings` is an accepted spelling; the parser also recognizes `#set` for `cube` and
`cubeCompose` metadata. This exception does not make arbitrary settings valid
under `#set`.

Except for the explicitly rejected duplicates above, scalar settings generally
replace the previous value. `output_exclude` appends paths, and warmup calls
merge cases. Declare cache configuration before its warmup calls: a later
`cache(...)` replaces the component cache object. Do not treat repeated scalar
settings as a list.

One route can list multiple methods. Placeholder names occupy entire segments,
for example `/records/{id}`, not `/records/prefix-{id}`. Names start with a letter
and contain letters, digits or underscores. Methods are normalized and deduplicated.

`marshal` installs the JSON implementation only for `application/json`.
`unmarshal` recognizes `application/json` and `application/xml`; other MIME strings
do not install an implementation through these settings. These calls do not
register missing Go types or extend the protocol's supported media types.
The output plan currently recognizes `json`, `tabular`, `csv`, `xml`, `xls` and
`xlsx`; unsupported formats fail at that owner. `case_format('lc')` selects
global lowerCamel names; the output compiler validates other case-format names
through its text-format owner. `date_format` passes through the date-format to
Go-layout converter. Per-field tags and explicit JSON names retain their
separate precedence (`runtime/output/plan.go`, `formats.go`, `wire.go`).

### Cache settings and warmup options

Declaration fragments to add to an existing typed reader:

```sql
#setting($_ = $cache('orders', '5m').WithProvider('afs').WithLocation('cache/orders'))
#setting($_ = $cache_warmup('ID', 'IndexParameter=ID', 'ID=1,2', 'Connector=main'))
```

The three recognized cache modifiers are `WithProvider`, `WithLocation` and
`WithTimeToLiveMs`. Recommended arity is one each; their parser accepts zero or
more, consumes the last if present, and does not reject unknown modifiers or
an unparsable millisecond value. This is a validation gap, not extra syntax to
rely on. TTL validation occurs in `bootstrap/cacheconfig/cache.go`: authored caches
require a location, a positive duration or milliseconds, and both forms must
agree if set. Milliseconds must fit a Go duration (at most 9223372036854 ms).
The configured provider is AFS (empty or `afs`) or an explicit `aerospike:` URL;
other providers need a supplied native service. Aerospike timeout/retry/failure
fields are host settings, not additional cache modifiers.
A cache name must resolve to settings or a native SQLX cache service. Per-view
`use_cache(view,name)` selects that name; it does not create the service.

Warmup consumes quoted options after the index column:

| Option | Value / behavior |
| --- | --- |
| `Connector` | nonempty connector name |
| `IndexParameter`, `index_param`, `indexparam` | nonempty canonical parameter name |
| `IndexMeta`, `index_meta` | true/1/yes/on or false/0/no/off, case-insensitive |
| Any other name | comma-separated, nonempty parameter values; produces a case dimension |

Parameter names refer to existing canonical inputs. Each call adds a case;
values within a case expand as combinations. Repeated calls must agree on index
column, index parameter, connector and index-meta policy. The first argument may
be `''` when only concrete parameter cases are wanted. `cache_warmup(view,name)`
in the outer projection is a different, exactly-two-argument named view binding.
Neither form adds automatic cache invalidation to writer execution.

## Parameter and view declarations

Explicit declarations are available when the application needs a custom binding. Standard `transcribe` writer workflows derive body and Current bindings automatically; do not add them as required boilerplate.

Syntax fragment; adapt within the [complete reader contract](dql.md#a-shared-view-structure-for-readers-and-writers).

~~~~sql
#define($_ = $ID<int>(path/id).Required())
#define($_ = $Name<*string>(query/name).Optional())
#define($_ = $Authorization<string>(header/Authorization).WithStatusCode(401))
#define($_ = $Body<[]*model.Record>(body/))
#define($_ = $Rows<[]*model.Record>(view/Rows) /*
 SELECT id, name FROM records
*/)
~~~~

Head: `$_ = $Name[<InputType[,OutputType]>](kind/location)`.
`model.Record` is a linked row type from the declared import; name row types
explicitly in authored examples. The grammar also accepts `?` for inference
(it is not Go `any`). The second type is meaningful for conversion/codec authoring.

#define establishes declared authority. Use `.Value(...)` for declared defaults. Reject conflicting definitions.

The source kind is an open provider identifier, not a closed grammar enum.
Parsing a kind does not install a provider. Standard transport and component
bindings are:

| Source | Location / authority |
| --- | --- |
| `query`, `path`, `header`, `cookie`, `form` | exact request name; path names must agree with route placeholders |
| `body` | empty for whole typed body, or a provider-supported body path |
| `http_request` | original HTTP request through the protocol provider |
| `view`, `data_view` | independent typed read; canonical identity is the declaration holder |
| `param` | canonical input graph value; declaration SQL may derive it with StructQL |
| `const` | authored constant/provider value |
| `output` | output binding slot, e.g. `view` for the reader result or `body` for a handler response |

Additional kinds require registered providers and their own location contracts.
Header parameters default required, query parameters optional; write `.Required()`
or `.Optional()` explicitly where important. Input field name, source location,
SQL column name and JSON name are separate identities. Do not automatically
rename aliases or manufacture casing/spelling variants to repair a failed lookup.

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
| WithDescription(text), Description(text) | 1 | generated HTTP/MCP parameter description |
| WithExample(value), Example(value) | 1 | illustrative documentation/test value; not a runtime default |
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
| WithURI(uri) | 1 | leading `/` selects route activation; otherwise a resource reference |
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

### Selector view scoping

`QuerySelector(view)` recognizes declaration names `Fields`, `OrderBy`, `Offset`,
`Limit`, `Page` and `Criteria` (case-insensitive). It does not recognize arbitrary
names such as `OrdersLimit`. The argument selects a registered prepared-view name;
the request binding remains the authored `(query/location)`. For example:

```sql
#define($_ = $Limit<int>(query/ordersLimit).QuerySelector('Orders'))
```

For a component named `Orders`, this fragment binds the query key `ordersLimit`
to that component's root selector. It does not rename the DQL field `Limit`.
`sql/reader/view_index.go` indexes prepared `Spec.Name`, `Key.Name` and the
component name, case-insensitively; unknown or ambiguous names fail. An outer
SQL namespace alone is not an additional selector alias. View-control targets
use SQL namespaces, so verify the registered view identity rather than assuming
those two naming surfaces are interchangeable. Selector parameters default noncacheable unless
explicitly configured. Scope inputs deliberately for multiple views; do not add
all selectors to every view. The selected view still needs allowed columns and
methods. Fields must be a string slice, OrderBy/Criteria a string, and
Offset/Limit/Page signed integer types (pointers are unwrapped). One property
cannot be bound twice to the same prepared view. Order aliases are explicit `allowed_order_by_columns` entries, not
inferred SQL or wire-name conversions. Pagination, field selection and criteria
are validated by the reader/compiler and SQL builder after declaration parsing.

### Predicate metadata and group references

Syntax fragment; adapt within the [complete reader contract](dql.md#a-shared-view-structure-for-readers-and-writers).

~~~~sql
#define($_ = $IDs<[]int>(query/ids).WithPredicate(0, 'in', 'r', 'id'))
~~~~

`WithPredicate`, `Predicate` and `ApplyWhenAbsentPredicate` accept a predicate
name followed by its catalog-defined arguments. If there are at least two
arguments and the first parses as an integer, it is the group number; otherwise
the group is 0 and the first argument is the name. Multiple predicates may use
one group or one input. The metadata parser does not impose a nonnegative group
constraint or a fixed predicate-name enum. Catalog compilation resolves names,
argument counts and types; do not guess a custom predicate's signature.

For built-ins such as `in`, alias and column are separate arguments (`'r', 'id'`),
not one qualified string. They refer to the SQL where the predicate is expanded;
the alias is not automatically rewritten to the outer view namespace. Other
predicates have their own catalog signatures.
`Of(group)` is a parameter owning-group reference, distinct from the numeric
predicate group. Requiredness and record-count constraints are also independent
from predicate activation. Absent inputs normally contribute no predicate;
`ApplyWhenAbsentPredicate` permits evaluation when absent. Presence markers,
when available, distinguish an explicitly supplied zero/false from omission.

See [Reader predicates](reader-predicates.md) for the predicate catalog and
reader examples. SQL fragment composition, group references in expressions and
WHERE/AND placement belong to the [final DQL + Velty section](#dql--velty-separate-language-layer).

## SQL and view-control projections

The outer query is the Datly view graph. Its named sources, projections and
relation links are structural metadata. Inside each named view, ordinary CTEs,
recursive CTEs, subqueries, joins, CASE, aggregates, windows, database CASTs and
other dialect expressions remain SQL. Their acceptance belongs to the pinned
SQL parser and database dialect. The EBNF deliberately treats inner SQL as
opaque: it neither restricts SQL to a toy SELECT grammar nor claims exhaustive
vendor grammar support. Driver result labels/ordinals, not guessed expression
provenance, identify outputs. See `transcribe/compile/reader_source.go` and
`transcribe/column/query.go`.

### Accepted reader SQL versus the preferred named graph

Named outer sources are the recommended form for explicit multi-view authoring,
not a requirement imposed on every reader. The reader compiler also accepts
no-FROM scalar reads (`SELECT 1`), unaliased physical sources (`SELECT ID FROM
records`), outer WITH/CTE reads, and single-view DISTINCT or UNION reads, subject
to SQLparser structural validation and the database dialect. These are SQL
fragments, not complete component contracts. An explicit FROM alias supplies
the namespace; without one the compiler uses the terminal source name. A scalar
read with no FROM has no source namespace. That rule does not rename authored
SQL aliases or invent a namespace for a source that has none.

When compilation produces relations, the multi-view decomposition owner rejects
outer UNION and DISTINCT. It also requires root GROUP BY/HAVING inside the root
source. Outer root WHERE/ORDER BY terms must belong to the root namespace;
child-owned terms belong inside that child's SQL. These restrictions apply to
the decomposed outer graph, not indiscriminately to joins, aggregates or set
operations inside a view. CTE-backed root/child sources retain their WITH clause
when decomposed. See `transcribe/compile/reader.go` (`queryNamespace`) and
`reader_source.go` (`decomposeReadSources`, `canonicalRootSource`).

The EBNF separates the preferred named graph skeleton from the broader accepted
reader-query boundary. That boundary delegates SQL syntax to its real parser;
it does not enumerate every vendor expression. Parser-supported source envelopes
and expression islands are covered in the final DQL + Velty section.

Controls target the named views in the outer DQL graph, not local table aliases
inside their SQL. They must be standalone outer SELECT projection items, not
WHERE terms, inner SQL annotations or nested function arguments, and cannot be
the entire projection.

| Control | Exact arity and syntax |
| --- | --- |
| connector/cache | use_connector(alias,name), use_cache(alias,name), cache_warmup(alias,name); 2 each; quoted names or supported identifiers |
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
| mutation lifecycle | lifecycle_type(view,'package.OrderLifecycle') |
| invariant group | invariant(view.column,'GroupName') |
| selector permissions | selector_fields(view,bool), selector_order_by(view,bool), selector_criteria(view,bool), selector_limit(view,bool), selector_offset(view,bool), selector_page(view,bool) |
| selector defaults | selector_default_order(view,'expression'), selector_default_limit(view,integer), selector_no_limit(view,bool) |
| selector scope | selector_namespace(view,'name'), selector_filterable(view,'field,path,...') |
| selector criteria methods | selector_sql_methods(view,'[{"name":"lower","args":["string"]}]') |

Numeric control arguments are unquoted, nonnegative integer literals. set_limit(alias,0) removes the view limit; it does not erase an explicitly authored SQL LIMIT. Controls are consumed as metadata rather than sent to the DB. Every listed
control has exactly 2 arguments except the four flags (`allow_nulls`, `groupable`,
`grouping_enabled`, `publish_parent`: exactly 1), `self_ref` (exactly 4) and
`set_partitioner` (2 or 3). String values are quoted; only connector/cache/warmup
names also accept a bare identifier. All controls are singleton per target except
`allowed_order_by_columns`, whose repeats must not create ambiguous mappings.
`groupable` and `grouping_enabled` share one singleton slot. `tag` and `invariant`
are separate column annotations, exactly 2 arguments each; rich CAST is a
column/type pair in CAST syntax.

Selector permission booleans are unquoted. `QuerySelector(view)` binds a request
field to the named view; it does not itself grant permission. Selector policy
calls grant each capability independently. `selector_default_limit` is both the
fallback and cap for a positive requested limit while `selector_no_limit` is
false. A positive `set_limit` sets a base view limit and clears no-limit mode;
`set_limit(view,0)` clears that base limit and enables no-limit mode. Criteria
methods use a quoted JSON array of method names and Go argument type expressions;
malformed or duplicate methods are rejected. JSON shape is validated during DQL
compilation; argument type expressions resolve later through the reader compiler's
canonical type authority. `set_limit` and `selector_no_limit` cannot both target
the same view because their overlapping no-limit semantics would otherwise be
source-order dependent.

~~~~sql
#package('example.com/app/records/read')
#setting($_ = $input_type('RecordsInput'))
#setting($_ = $output_type('RecordsOutput'))
#setting($_ = $case_format('lc'))
#setting($_ = $route('/records', 'GET'))
#setting($_ = $connector('main'))
#define($_ = $Records<[]*Record>(output/view))
SELECT records.*, children.*, type(records, 'Record'), type(children, 'Child'),
       batch_size(children,100), batch_concurrency(children,2)
FROM (SELECT r.* FROM records r) records
JOIN (SELECT c.* FROM children c) children
  ON children.record_id=records.id AND children.tenant_id=records.tenant_id
~~~~

Use explicit equalities for composite links. Parenthesized physical sources in mutation intent, such as JOIN (lookup_table) l ON ..., represent auxiliary/nonmutating data. This is distinct from a writable table and from (SELECT ...).

SELECT r.* EXCEPT INTERNAL_NOTE is Datly visibility syntax. Internal backing data may still be fetched for joins/hooks; do not confuse it with a dialect set-difference operation.

### Complete reader with ordinary inner CTE SQL

```sql
#package('example.com/shop/api/orders/bycustomer')
#setting($_ = $input_type('OrdersInput'))
#setting($_ = $output_type('OrdersOutput'))
#setting($_ = $case_format('lc'))
#setting($_ = $route('/customers/{customerID}/orders', 'GET'))
#setting($_ = $connector('main'))
#define($_ = $CustomerID<int>(path/customerID).Required())
#define($_ = $Orders<[]*Order>(output/view))
SELECT orders.*, type(orders, 'Order'), CAST(orders.ID AS int),
       CAST(orders.Total AS float64)
FROM (
    WITH totals AS (
        SELECT o.ID, SUM(i.QUANTITY * i.PRICE) AS Total
        FROM ORDERS o JOIN ITEMS i ON i.ORDER_ID = o.ID
        WHERE o.CUSTOMER_ID = :CustomerID
        GROUP BY o.ID
    )
    SELECT ID, Total FROM totals
) orders
```

The application supplies `main` and the ORDERS/ITEMS schema. `OrdersInput`
contains required `CustomerID int`. `OrdersOutput.Orders` is `[]*Order`, with
`ID int` and `Total float64`; global `lc` determines wire casing. `totals`, `o`
and `i` are inner SQL names. Only `orders` is the Datly view namespace.
The outer CASTs declare Go result authority without changing the database SUM.
The named value convention is described with SQL-source bindings in the final
section. This example does not claim every database uses identical numeric
metadata or supports every possible CTE feature.

## Rich CAST, pseudo fields and tag customization

An inner Datly view can contain CTEs, nested queries and database-specific
expressions. Its result-column metadata comes from the database driver and may
be less precise than table metadata. Keep that SQL intact. Outer DQL CASTs supply
explicit Go type authority where inference is insufficient; accepting a typed
result must not depend on reconstructing its base-table expression.

### Column types and general declaration types are different surfaces

Parameter type brackets, `WithType` and contract type settings use general Go
expression/type authority, subject to resolution and suitability. Outer rich
CAST and fluent `WithColumnType`/`ColumnType` instead share
`transcribe/dql/declarations_view.go:ColumnType`:

- The terminal is an identifier (including a built-in or named application type)
  or a single package-alias selector such as `model.Bounds`.
- Parentheses may wrap the type. At most one pointer marker and one slice marker
  are accepted. Common forms are `T`, `*T`, `[]T` and `[]*T`.
- The normalizer also accepts `*[]T`, recording the same pointer/cardinality
  metadata as `[]*T`; do not interpret it as preserved Go pointer-to-slice layout.
- Repeated pointers, nested slices, fixed arrays and inline map/struct/interface/
  function/channel/generic-instantiation expressions are rejected here.
  This does not forbid a resolvable **named** rich type whose underlying Go shape
  uses one of those forms. It limits the column annotation's expression syntax.
- Qualified names require the declared import alias; `time` also has an explicit
  standard-package fallback in this owner. Prefer explicit imports for clarity.

The broader declaration grammar must not be used to promise inline rich-CAST
syntax that this owner rejects. Conversely, column restrictions do not narrow
all parameter/input/output Go types. Ordinary executable SQL CAST has its own
database type syntax. The EBNF names these `declaration_type`, `column_type`
and opaque database SQL separately.

Typed projection and tag annotations:

Syntax fragment; adapt within the [complete reader contract](dql.md#a-shared-view-structure-for-readers-and-writers).

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
#package('example.com/app/orders/read')
#setting($_ = $input_type('OrdersInput'))
#setting($_ = $output_type('OrdersOutput'))
#setting($_ = $case_format('lc'))
#setting($_ = $route('/orders', 'GET'))
#setting($_ = $connector('main'))
#import('model', 'example.com/app/model')
#define($_ = $Orders<[]*Order>(output/view))
SELECT orders.*, type(orders, 'Order'), CAST(orders.pseudo_column AS model.GoShape),
       tag(orders.pseudo_column, 'sqlx:"-"')
FROM (SELECT o.*, '' AS pseudo_column FROM ORDERS o) orders
~~~~

The inner view SQL may contain database-specific expressions, nested queries or CTEs. SQLX result metadata owns output existence and names. An explicit outer CAST supplies the Go type even when the driver cannot report a database type; Datly does not need to infer that expression's provenance. Missing or duplicate result outputs still fail. Undeclared outputs with unknown types do not silently become strings.

For simple literal projections, `'' AS pseudo_column` defaults to Go `string` and `0 AS pseudo_column` defaults to Go `int`. These are optional syntax-based defaults, checked against the database result label and ordinal. Use outer `CAST(view.column AS int)` or `CAST(view.column AS *int)` when the exact type matters; CAST overrides literal defaults and inferred nullability. Opaque CTE/computed expressions should use an explicit CAST when the driver has no type metadata.

CAST does not imply transient DML mapping. Add `sqlx:"-"` explicitly for a logical field populated by `OnFetch` and translated into physical columns by writer `Init`. Physical codec-backed fields retain their authored SQLX mapping.

The tag normalizer accepts shorthand such as `validate:required`; generated Go tags must be valid `validate:"required"`. Prefer quoted Go-tag spelling in examples. Explicit tags refine the corresponding metadata without deleting unrelated json/sqlx tags.

A standalone custom Go CAST is a type declaration. Ordinary CAST(expression AS SQLType) AS result_alias remains executable SQL. Preserve physical codecs. Resolve model.Bounds through the import; never generate a duplicate empty Bounds type.

Internal physical columns remain SQL/DML-mapped and hidden from clients. The logical pseudo field is populated by OnFetch, translated by writer Init, and omitted from physical DML. Nested Has flags decide which backing fields change.

## StructQL declaration queries

StructQL derives typed projections/index helpers from an input graph rather than a physical DB source.
This fragment assumes the `model` import, a linked `model.EventKey` row with
`Id`, and an input graph exposing `/Events`; declare those in the containing contract.

Syntax fragment; adapt within the [complete reader contract](dql.md#a-shared-view-structure-for-readers-and-writers).

~~~~sql
#define($_ = $Keys<[]*model.EventKey>(param/Keys) /*
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
#setting($_ = $case_format('lc'))
#define($_ = $Records<[]*Record>(output/body))
#setting($_ = $route('/records', 'PATCH'))
#setting($_ = $connector('main'))
SELECT records.*, children.*, lookup.*,
       type(records, 'Record'), type(children, 'Child'), type(lookup, 'Lookup'),
       lifecycle_type(records, 'hooks.RecordLifecycle'),
       invariant(records.START, 'Schedule'),
       invariant(records.END, 'Schedule'),
       tag(records.END, 'validate:"gtfield(Start)"')
FROM (SELECT r.* FROM records r) records
JOIN (SELECT c.* FROM children c) children ON children.record_id = records.id
JOIN (SELECT l.* FROM (lookup_values) l) lookup ON lookup.id = records.lookup_id AND 1=1
```

`lifecycle_type(records, 'hooks.RecordLifecycle')` explicitly binds an existing
struct in the imported package. Its methods must match the entity/parent types.
For a new local scaffold, declare `lifecycle_type(records, 'RecordLifecycle')`
under the destination `#package`; pure Go transcription creates empty methods
once. Missing foreign types fail. Omitting the declaration keeps ordinary writes
hookless, and auxiliary views cannot declare mutation lifecycles.
`lifecycle_type` requires generated Go mutation dispatch for PATCH/POST/PUT.
Readers reject it: input initialization and output finalization belong to the
explicit contract types, with row `OnFetch` a separate hook. It is not a generic
reader hook registration mechanism. The spelling
`entity_hooks` is unsupported and produces a diagnostic.

`invariant` places Start and End in one cohesive invariant group; the comparison rule
uses the exact generated Go field name `Start`. Use schema-resolved date/time
columns, or explicit linked Go date types. For sparse updates, the generator
backfills omitted group members from authorized Previous values without setting
Has, then validates the effective date interval. Application Go `Init` and
`Validate` hooks express additional business rules. Auxiliary rows remain readable
and never enter mutation traversal. The generator owns Body/Existing/Data
plumbing; authors do not construct it for standard generation. The declared
response is `RecordsOutput.Records []*Record`; each Record carries the named
children collection and to-one lookup holder. Confirm generated input/output
contracts for the selected operation rather than assuming a reader contract is
also a writer request. The schema and imported lifecycle in this example must
exist in the application.

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
| Generated Go handler, when present | `handler.go` | `$handler_dest('execute.go')` |
| Create-once application lifecycle, when requested | `lifecycle.go` | `$lifecycle_dest('custom.go')` |
| Generated mutation definition | `mutation.go` | `$mutation_dest('policy.go')` |
| Embedded resource filesystem, when needed | `resources.go` | `$resources_dest('sql_resources.go')` |
| Factory registration, when needed | `links.go` | `$links_dest('register.go')` |

Optional `$file_prefix('orders_')` applies to default filenames for both readers
and writers: `orders_input.go`, `orders_router.go`, `orders_mutation.go`, etc.
The prefix must start with an ASCII letter and contain only ASCII letters,
digits, underscores or hyphens. Without this setting the prefix is empty.
Exact per-file overrides take precedence and are never prefixed:

Syntax fragment; adapt within the [complete reader contract](dql.md#a-shared-view-structure-for-readers-and-writers).

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
traversal, hidden files and `_test.go` destinations are rejected.  The exact-arity filename settings require nonempty quoted
arguments; duplicate role settings and unknown support roles are errors.

Use `$support_dest('role','filename.go')` for separate support products. Supported
roles are `entities`, `entity_methods`, `types` (cross-package shape aliases),
`frames`, `previous`, `layout`, `actions`, `mutation_output`, `validation`, `hooks`,
`invariants`, and `indexes`. Role names are case-sensitive; a `type:` role requires
an exported Go identifier after the colon. Their defaults are `<role>.go`. Generated `hooks.go` contains
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
SQL resource files are emitted only when the component needs packaged resources. The optional alternate-language artifact is described in the final section.

## Semantic checks after grammar

Resolve names, Go imports/types, provider kinds, codecs, predicates, resources, route/tool identities, constraints, relations, cardinalities, hook signatures and dialect budgets. Reject ambiguity and unknown references. Validate generated behavior, not only balanced syntax. An unavailable capability must be reported explicitly rather than silently reinterpreted.


## To-one outer JOIN shorthand

For reader and writer DQL generation, append `AND 1=1` to a relation's outer
`JOIN ... ON` equality links to declare a single holder:

Syntax fragment; adapt within the [complete reader contract](dql.md#a-shared-view-structure-for-readers-and-writers).

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

## Constants and instance-specific substitution boundary

See [Constants and substitutions](constants-and-substitutions.md) for typed DQL
defaults and per-instance YAML/JSON overrides. `datly transcribe ... -const`
and standalone `ConstURL` load one trusted flat mapping. A present override,
including zero, false or an empty string, wins; an omitted name retains its DQL
default. Runtime request data cannot override constants.

Constants can select qualified table roots and resource paths across E2E and
production instances. `$project.dataset.table` and braced table roots such as
`${project}.dataset.table` are supported. Quote the whole BigQuery or legacy identifier
when required by that database. Expansion is transient before database discovery,
SQL execution or resource access. Authored DQL, reusable resources and generated
source retain the unexpanded spelling.

## Resources, static content and documentation

Resources use one canonical Bindly store shared by source discovery, SQL,
StructQL and documentation. Standard `fs.FS`, including `embed.FS`, supplies the
bytes. Named stores require an explicit namespace; registration order does not
select an implicit default. Keep resource paths distinct from Go import paths,
source package paths and generated file destinations. A declared resource must
resolve before execution/publication. SQL resource-reference spelling appears
in the final language section with the other SQL-source substitutions.

`meta('docs/component.md')` selects a description resource.
`mcp('orders','List orders','docs/orders.md')` declares tool exposure and an
optional description resource. `mcp_folder('docs','public','docs://orders/')` exposes
an explicit namespace/root/URI prefix; `mcp_skill_folder` additionally marks the
root as a skill folder. Folder validation is owned by `spec/resource_folder.go`:
namespace must be nonempty without `:`, `/` or backslash; root must be a valid
relative fs path (or empty); URI prefix needs a scheme and host, without user
info, query, fragment or path escapes. Do not infer arbitrary folder discovery
from these three arguments.

Static routes support GET and HEAD. Static declarations form separate components, not extra SQL view controls.
`static_resource('site','public')` selects a registered namespace/root;
`static_content('content-url','root')` selects content-backed assets. They reject
SQL, parameter/handler mixtures and incompatible component metadata. Route/API
key metadata supplies HTTP exposure. Missing assets fail staging. Filesystem
root authorization belongs to the hosting configuration, not a DQL argument.

Shared YAML dictionaries load `DocGlobalURLs` before rule-specific `DocURL` or
ordered `DocURLs`, resolved against `DocBaseURL` where supplied. Recognized
sections are `Columns`, `Filter`, `Parameters`, `Paths` and `Responses`. Dictionary
keys support qualified table/column names, bare-column fallback, `_` holder
entries and `$example` values. Overrides use dictionary-key precedence, not an
invented recursive deep merge. Response entries use ordinary OpenAPI response
and schema shapes. The same resolved metadata enriches OpenAPI and MCP; it does
not change binding, SQL columns or wire aliases. Source owners are
`transcribe/description.go`, `transcribe/shared_documentation_test.go` and the
SDK documentation service. Shared schema references remain resources.

## Limits and semantic validation

| Limit / policy | DQL surface and owner |
| --- | --- |
| Source/destination | one selected CLI component; explicit package inside project module; `cmd/datly/generate.go`, `transcribe/generator.go` |
| Parameter errors | status 100–599; counts nonnegative; the binder checks the bound collection count; `transcribe/dql/declarations_options.go`, registration/binding validation |
| View counts | nonnegative integer literals for limit/batching/concurrency; no fixed global maximum in directive parser; `transcribe/compile/view_directives.go` |
| No-limit | `set_limit(view,0)` removes view limit; authored inner SQL LIMIT remains SQL |
| Cube composition | `cubeCompose(enabled[,mcpTool,maxCubes,maxLimit,timeoutMs])`; omitted positive budgets normalize to 8 cubes, limit 100, timeout 30000 ms |
| Warmup | `Limit`, `MaxCases`, `FieldNames` and per-case exclusions exist on typed settings, not standalone DQL options; unknown warmup option names become parameter dimensions |
| Cache | positive TTL and agreement between duration/milliseconds checked by configuration; no automatic backend selection or fallback |
| SQL parameters / expressions | database dialect placeholder budgets and reader/report validation apply; no universal DQL parameter-count maximum |
| Resource/folder/file paths | explicit resource authority, traversal and filename checks owned by their loaders/generator; parsing text does not authorize filesystem access |

The dynamic reader builder initially enables cube/report only for a narrow main
view shape: one wrapped SELECT over one source, direct aliased
SUM/COUNT/MIN/MAX/AVG measures, explicit grouped column dimensions, and no
HAVING, joins, CTEs, windows, DISTINCT, set operations, nested aggregates or
aggregate wrappers such as COALESCE/CAST. Outer DQL CAST may still assign the Go
type of the resulting measure. These activation checks do not restrict ordinary
reader DQL compilation.

### Known parser boundaries

The grammar is an authoring contract backed by the current owners, not a promise
that all malformed input is rejected at the first stage. In particular:

- The general component-setting switch has no unknown-name rejection branch.
  Many settings also ignore fluent tails or surplus arguments. The exact counts
  above identify where this occurs; unsupported spellings must not be used as
  working settings just because parsing succeeds.
- Cache modifiers have weaker validation than declaration options. Unknown
  declaration options fail, and duplicate singleton options fail even through
  aliases. A cache modifier's acceptance does not prove it was applied.
- Declaration type brackets consume the first two comma-separated types; extra
  entries are not a documented type facility. Prefer one type, or the meaningful
  input/output pair. Go type syntax is delegated to `viant/x/shape` and Go parsing;
  not every Go type is a suitable request, row or generated contract type.
- Host-configured report/warmup/selector/resource policy is not automatically a
  DQL setting. Provider, codec, predicate and type registration remain separate
  semantic requirements.
- Inner SQL remains subject to the actual parser and dialect. Opaque grammar
  notation does not guarantee every vendor feature compiles. Missing/duplicate
  outputs and unresolved rich types fail; do not repair them by guessing aliases.

## Grammar and implementation inventory

Paths below are repository-relative source evidence. They identify the owner to
consult when syntax and behavior differ; the EBNF does not replace these checks.

| Grammar / feature | Parser/compiler/runtime owner | Evidence tests |
| --- | --- | --- |
| Directive scanning, quoting, balanced arguments | `transcribe/dql/directive_scan.go`, `parse_primitives.go`, `preprocess.go`; SQLparser source scanner | `transcribe/dql/preprocess_test.go`, `parse_test.go` |
| Package/import/type authority | `transcribe/dql/type_context.go`, `transcribe/discovery_imports.go`, `typecatalog/resolver.go`, `viant/x/shape` | `transcribe/discovery_imports_cli_test.go`, `generator_required_package_test.go`, `typecatalog/context_test.go` |
| All component settings / arity | `transcribe/dql/component_settings_parse.go`, `component_settings_directives.go`, `route_directive.go`, `component_settings_cache.go` | `transcribe/dql/output_settings_test.go`, `filename_settings_test.go`, `cube_compose_test.go`, `parse_test.go` |
| Every fluent declaration option | `transcribe/dql/declarations_head.go`, `declarations_options.go`, `declarations_view.go`, `declarations.go` | `transcribe/dql/declarations_test.go`, `declaration_sql_test.go`, `record_count_test.go` |
| Transport/provider bindings | Bindly locator providers, `runtime/handler/provider`, `runtime/handler/engine/provider_composer.go` | `runtime/handler/provider/provider_test.go`, `runtime/handler/engine/provider_composer_test.go` |
| Outer named views and inner SQL | `transcribe/compile/reader.go`, `reader_source.go`, `reader_named_projection.go`, SQLparser | `transcribe/named_view_sqlite_test.go`, `transcribe/compile/outer_projection_independent_probe_test.go` |
| Complete view controls | `transcribe/compile/view_directives.go`, `spec/view_controls.go` | `transcribe/compile/reader_test.go`, `entity_hooks_test.go` |
| CAST, literal/pseudo columns, tags, internal fields | `transcribe/column`, `transcribe/compile/reader_projection.go`, `transcribe/generate/plan_column_tags.go` | `transcribe/cast_override_test.go`, `cast_scalar_sqlite_test.go`, `invariant_pseudo_test.go`, `transcribe/column/literal_authority_test.go` |
| Selector names and view scope | `spec/query_selector.go`, `sql/reader/compiler/selector.go`, `sql/builder/selector_validation.go` | `spec/query_selector_test.go`, `sql/reader/selectors_test.go`, `selector_declared_names_sqlite_test.go` |
| Predicate declaration/groups and catalog | `transcribe/dql/declarations_options.go`, `runtime/predicate/velty/program.go`, `context.go`, `registry.go` | `transcribe/generated_predicate_reload_test.go`, `runtime/predicate/velty/program_test.go` |
| Auxiliary graph, composite links, AND 1=1 | `transcribe/compile/reader_relation_projection.go`, `transcribe/handler/compiler/relations.go`, `generation_auxiliary.go` | `transcribe/auxiliary_runtime_test.go`, `to_one_hint_test.go`, `to_one_hint_runtime_test.go`, `to_one_hint_regenerate_test.go` |
| Lifecycle and invariant metadata | `transcribe/handler/compiler/entity_hooks.go`, `entity_invariants.go`, `transcribe/generate/lifecycle.go` | `transcribe/lifecycle_target_test.go`, `mutation_hook_scaffold_test.go`, `entity_invariant_generation_test.go` |
| File roles, shapes, regeneration ownership | `spec/generation_files.go`, `transcribe/generate/filename_settings.go`, `destination_packages.go`, `persist*.go` | `transcribe/generate/filename_layout_test.go`, `cmd/datly/generate_regeneration_test.go`, `transcribe/authored_hook_destinations_test.go` |
| Global casing and explicit names | `runtime/output`, pinned Structology JSON marshaler | `runtime/output/wire_test.go` |
| Resources / static / MCP folders / YAML | `transcribe/resources.go`, `transcribe/generate/static.go`, `transcribe/description.go`, `spec/resource_folder.go` | `transcribe/package_resources_sqlite_test.go`, `static_test.go`, `mcp_folders_test.go`, `shared_documentation_test.go` |
| Cache and warmup metadata | `transcribe/dql/component_settings_cache.go`, `spec/cache_warmup.go`, bootstrap native SQLX cache wiring | `spec/cache_warmup_test.go`, `transcribe/dql/parse_test.go` |
| DQL + Velty boundary and SQL fragment arguments | `transcribe/dql/template_sql.go`, `sql/template`, `sql/fragment`, `runtime/predicate/velty` | `transcribe/compile/reader_template_test.go`, `sql/template/program_test.go`, `runtime/predicate/velty/builder_test.go` |

## DQL + Velty: separate language layer

This is the final section. Everything above describes high-level DQL metadata,
view graphs and ordinary SQL. Executable template expressions are a separate
language layer, processed by the installed Velty/SQL-template owners. Do not
move their operators into the DQL declaration grammar or treat arbitrary Velty
statements as application Go hook metadata.

### Target and template settings

The CLI accepts `-lang velty` as an explicit target selection. `useTemplate`
accepts one or more arguments and consumes the last nonempty template type;
this setting is generation metadata, not a general expression evaluator.
`template_dest` requires exactly one nonempty quoted relative resource path,
rejects duplicates and fluent tails, and defaults to `handler.velty` when that
artifact is needed. It permits relative subdirectories such as
`templates/main.velty`. A selected Velty factory still uses the handler file role.
Go handler/business lifecycle generation has its own roles and contracts.

### Executable syntax boundary

Declaration-shaped `#set($_ = $Name<Type>(kind/location)...)` is recognized by
the declaration owner; an explicit `#define` replaces it for the same canonical
name/source identity. This does not make a procedural assignment a request
binding. New authoring should use `#define` for binding authority.

SQL-template detection recognizes directive names `if`, `elseif`, `else`, `set`,
`foreach`, `for`, `evaluate`, `end` and `break`. Full statement/expression syntax,
member access, calls, indexing, operators and literal behavior belong to Velty;
recognition alone does not establish that a construct type-checks.

A parser-supported source prefix and balanced suffix may surround reader SQL;
`TemplateFrame` preserves that envelope during query rewrites and decomposition.
The SQLparser error handler also delegates supported template operands and
predicate suffixes to their owners. A leading template assignment is not proof
that the remaining source must match only the preferred named-view skeleton.
See `transcribe/dql/statement/parser.go`, `transcribe/compile/reader_template.go`
and the `TemplateFrame` cases in `transcribe/compile/reader_test.go`.

The transcription statement owner is `transcribe/dql/statement/parser.go`, while
`sql/template/program.go` compiles executable query templates. `#define` declares
binding authority; executable `#set` assigns template state. The metadata cube
exception described above is classified before executable statements.

Use declared identifiers and registered context objects exactly as authored.
Generated tags may preserve an authored SQL/input spelling for an existing Go
field; this is preservation of authority, not automatic alias renaming. Do not
invent alternate names to make a template compile.

The SQL-template scanner protects the context variables/directives it handles
inside SQL literals, quoted identifiers and comments (`sql/template/directive.go`).
This protection does **not** apply to the separate embedded-resource scan below.

### SQL values and query context

- Named values: :ID, :Name with declared bindings.
- Declared input references such as $Name resolve to bound values.
- Input values become bound parameters. Do not quote or concatenate them into SQL.
- Embedded SQL uses `${embed:sql/shared.sql}` or `${embed:namespace:path}`; resources must exist before execution. `EmbeddedSQLRefs` scans raw source for the exact `${embed:` token through the next `}` without checking SQL literals/comments. It trims the path and deduplicates by the full raw token. `sql.ResolveSource` then replaces occurrences of that raw token. A token in a SQL string, quoted identifier or comment can therefore request/expand a resource; do not place one there expecting SQL lexical protection.
- $View.NonWindowSQL preserves the parent query without view pagination, including its arguments.
- $View.Limit, $View.Offset and $View.Page are scoped values.
- Composition uses $CubeSQL1 through $CubeSQLN for requested frames. Validate all references and resource/parameter budgets.

These fixed query-context references preserve bindings and pagination semantics. Declare optional filters through typed predicates and relation membership through JOIN keys; the generator derives parent membership and all bound query fragments.

### Constant identifiers and per-instance expressions

The [constants guide](constants-and-substitutions.md) documents the supported
identifier forms, YAML/JSON instance files, CLI and standalone configuration.
Identifier constants are trusted deployment configuration; ordinary request
values remain SQL bind parameters. Expansion happens only in a private
database-bound or resource-access copy, so discovery and execution use the same
instance value without persisting it into the DQL or generated artifacts.

### Parent-query helpers and bound fragments

These SQL builder helpers belong to query execution, separate from outer
view-control functions:

| Helper | Accepted arguments / behavior |
| --- | --- |
| `$View.ParentJoinOn(column)` | one column; default prefix AND |
| `$View.ParentJoinOn(prefix,column,...)` | explicit prefix followed by parent-key columns |
| `$View.ParentCompositeJoinOn(prefix,column,...)` | prefix plus composite key columns; expander validates the key shape |
| `$View.AndParentJoinOn(column)` | exactly one column, AND prefix |
| `$View.ColIn(prefix,column)` | exactly two arguments |
| `$View.NonWindowSQL` | parent query without its view pagination; appends the parent arguments; fails without a parent query |

Values remain placeholders with ordered arguments. Do not interpolate request
strings as SQL or build a fake WHERE by concatenating untrusted text. Parent
membership is normally derived from declared relation keys; manually authored
helpers still have to match the prepared parent shape and dialect budget.

### Predicate groups: creating WHERE or appending AND

These are reader SQL-source fragments. Declare predicates in DQL first; for
example, put status/category filters in group `0` and name/description search
filters in group `1`. `FilterGroup(0, "AND")` joins the active filters within
group 0 with AND; `FilterGroup(1, "OR")` joins those within group 1 with OR.
`CombineAnd` then joins the two parenthesized groups with AND.

When the query has no WHERE clause, let the builder create it:

```sql
SELECT p.ID, p.NAME FROM PRODUCTS p
${predicate.Builder()
    .CombineAnd($predicate.FilterGroup(0, "AND"), $predicate.FilterGroup(1, "OR"))
    .Build("WHERE")}
```

When the query already has a fixed condition, append the groups with AND:

```sql
SELECT p.ID, p.NAME FROM PRODUCTS p
WHERE p.ACTIVE = 1
${predicate.Builder()
    .CombineAnd($predicate.FilterGroup(0, "AND"), $predicate.FilterGroup(1, "OR"))
    .Build("AND")}
```

The second form means `p.ACTIVE = 1 AND (group 0 AND group 1)`; an OR search
inside group 1 cannot bypass the fixed condition. `Build` supplies the leading
keyword, not the operator inside a group. Empty groups are omitted. If every
group is empty, both forms emit nothing: no dangling WHERE or AND, and the
second query retains its fixed condition. Predicate values remain bound SQL
arguments rather than text interpolated into the query.

### Predicate expression API and evaluation order

`Expand(group)` takes exactly one integer and joins with AND.
`ExpandWith(group,operator)` and `FilterGroup(group,operator)` take exactly two
arguments. `Builder()` takes none. Builder `Combine`, `CombineAnd` and
`CombineOr` are variadic fragment calls; `Combine` means AND. `And()` and `Or()`
take no arguments and choose the operator between subsequent combined blocks.
`Build(keyword)` takes exactly one string. Group/keyword/operator strings are
authored SQL structure; use fixed AND/OR and WHERE/AND, not request values.

Group expansion appends arguments when evaluated. Expand each intended group
where its fragment is emitted and preserve evaluation/output order; do not
compute fragments, discard them or reorder their SQL independently of arguments.
The builder only assembles fragment text and parentheses. It does not parse the
surrounding SQL to decide whether WHERE already exists. Empty groups emit no
fragment or arguments, and an empty builder emits no prefix. Presence handling,
custom predicate registration and catalog signatures are separate from builder
syntax. See [Reader predicates](reader-predicates.md) for the reader catalog.

The EBNF's final extension delegates template bodies to Velty explicitly. It is
not an exhaustive Velty grammar, just as the main grammar is not an exhaustive
database SQL grammar.

## Generated mutation markers

`delete_marker(view.column)` and `concurrency_token(view.column)` are standalone
outer SELECT annotations, with one qualified projected column argument and no
SQL alias. Each mutable view may declare at most one of each. They require the
generated Go PATCH/PUT policy; readers, auxiliary views, and POST reject them.

A delete marker is a logical boolean (for example inner `'' AS should_delete`,
outer `CAST(items.should_delete AS bool), delete_marker(items.should_delete)`).
Only explicitly supplied true flags with complete, authorized, parent-scoped
identities request deletion. Omitted rows and collections never imply deletion.

A concurrency token is numeric or `time.Time`, optionally pointer-valued. Its
validation compares captured expected presence/value with loaded Previous before
other validation. It does not add a SQL predicate, advance tokens, lock rows, or
provide atomic race prevention. Init may explicitly prepare a next working token
without changing the captured expectation. Missing/mismatched update tokens fail
with a typed conflict before mutations proceed.
