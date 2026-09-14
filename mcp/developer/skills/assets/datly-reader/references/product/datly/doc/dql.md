# DQL: syntax, declarations and grammar

[All guides](references/product/datly/doc/README.md) · [Authoring and transcription](references/product/datly/doc/authoring.md) · [Hook flows](references/product/datly/doc/hooks.md) · [Formal EBNF](references/product/datly/doc/dql.ebnf)

## Scope

DQL combines Datly declarations and view controls with SQL, Go type expressions, StructQL declaration queries and Velty templates. This reference and dql.ebnf cover the Datly-specific authoring grammar. SQL expressions/vendor extensions, Go type syntax and Velty expressions are delegated to their actual parsers: this is not a claim that one small EBNF implements every SQL dialect.

Use this reference to author declarations and view controls, then validate and transcribe with the matching Datly build. Syntax, installed capabilities and execution are separate checks; see [release status](references/product/datly/doc/status.md) for current boundaries.

## Lexical conventions

- Parameter names start with an ASCII letter, followed by letters/digits/underscore. Leading underscore is reserved for declaration machinery.
- SQL names and quoting follow the selected dialect. Preserve quoted dots and case-sensitive names.
- DQL string arguments use single or double quotes. Put a valid double-quoted Go tag inside an outer single-quoted DQL argument.
- Balance nested parentheses, brackets, type arguments and comments. Commas inside a type/string/call do not split outer arguments.
- Never interpolate parameter values inside SQL quotes/comments.
- #package and #import are complete line directives. Prefer one directive per line.
- Use canonical #setting declarations; do not invent directive spellings.
- A semicolon separates executable statements; metadata declarations are not DB commands.
- A successfully parsed token is not proof its provider/type/codec is installed.

## Package and import grammar

~~~~sql
#package('example.com/app/records')
#import('model', 'example.com/app/model')
~~~~

Use full module/package identity, not a filesystem path. model.Record, *model.Record, []*model.Record and model.Page[model.Record] resolve through the alias. Do not substitute a same-short-name local type.

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
| useTemplate | named generation template |
| input_type, output_type | Go type expression |
| dest, input_dest, output_dest, router_dest | generated destination |
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

One route can list multiple methods. Conflicting multiple route directives are not a route list. Placeholder names occupy entire segments, for example /records/{id}, not /records/prefix-{id}.

Use authored cube settings; do not confuse metadata declarations with executable assignments.

Cache modifiers: .WithProvider(value), .WithLocation(value), .WithTimeToLiveMs(integer).
Warmup options: connector=..., indexParameter=... (also index_param/indexparam), indexMeta=true|false (also index_meta), and parameter value lists. Discover installed providers.

## Parameter and view declarations

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

#define establishes declared authority. #set can supply compatible declarations/defaults and also belongs to Velty. Do not rely on duplicate conflicting definitions.

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

Controls target SQL aliases, not arbitrary metadata names. They must be standalone SELECT projection items, not WHERE terms or nested function arguments, and cannot be the entire projection.

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
| mutation hooks | entity_hooks(alias,'package.Hooks') |

Numeric control arguments are unquoted, nonnegative integer literals. set_limit(alias,0) removes the view limit; it does not erase an explicitly authored SQL LIMIT. Controls are consumed as metadata rather than sent to the DB. Allowed-order declarations can repeat without ambiguous mappings.

~~~~sql
SELECT r.*, c.*, batch_size(c,100), batch_concurrency(c,2)
FROM records r
JOIN children c ON c.record_id=r.id AND c.tenant_id=r.tenant_id
~~~~

Use explicit equalities for composite links. Parenthesized physical sources in mutation intent, such as JOIN (lookup_table) l ON ..., represent auxiliary/nonmutating data. This is distinct from a writable table and from (SELECT ...).

SELECT r.* EXCEPT INTERNAL_NOTE is Datly visibility syntax. Internal backing data may still be fetched for joins/hooks; do not confuse it with a dialect set-difference operation.

## Rich CAST, pseudo fields and tag customization

Typed projection and tag annotations:

~~~~sql
#import('model', 'example.com/app/model')
-- Projection annotations:
CAST(r.bounds AS model.Bounds)
tag(r.bounds, 'sqlx:"-"')
tag(r.BOUND_UNIT, 'internal:"true"')
tag(r.name, 'validate:"required"')
~~~~

Declare the pseudo projection explicitly through the view/shape contract (for example a logical NULL projection with non-DML mapping). CAST provides rich type authority; it does not make every physical JSON/custom-cast column transient.

The application shorthand tag(r.name,'validate:required') expresses the same validation annotation; generated Go tags must be valid validate:"required". Prefer quoted Go-tag spelling in examples. Explicit tags refine the corresponding metadata without deleting unrelated json/sqlx tags.

A standalone custom Go CAST is a type declaration. Ordinary CAST(expression AS SQLType) AS result_alias remains executable SQL. Preserve physical codecs. Resolve model.Bounds through the import; never generate a duplicate empty Bounds type.

Internal physical columns remain SQL/DML-mapped and hidden from clients. The logical pseudo field is populated by OnFetch, translated by writer Init, and omitted from physical DML. Nested Has flags decide which backing fields change.

## SQL parameters and template fragments

- Named values: :ID, :Name with declared bindings.
- Template selectors: $Input.Rows, $Name and supported member/index paths.
- Emitted values become bound parameters. Do not quote or concatenate them into SQL.
- Embedded SQL uses ${embed:sql/shared.sql} or ${embed:namespace:path}; resources must exist before execution.
- $View.ParentJoinOn('child_key') or $View.ParentJoinOn('AND','child_key').
- $View.ParentCompositeJoinOn('AND','tenant_id','record_id').
- $View.AndParentJoinOn('child_key').
- $View.ColIn('WHERE','child_key').
- $View.NonWindowSQL preserves the parent query without view pagination, including its arguments.
- $View.Limit, $View.Offset and $View.Page are scoped values.
- Composition uses $CubeSQL1 through $CubeSQLN for requested frames. Validate all references and resource/parameter budgets.

These SQL-building helpers are not view-control metadata functions.

## StructQL declaration queries

StructQL derives typed projections/index helpers from an input graph rather than a physical DB source.

~~~~sql
#define($_ = $Keys<?>(param/Keys) /*
 SELECT Id FROM /Events
*/)
~~~~

Use the actual declared graph path and supported StructQL conventions. Nested /Rows/Children paths are not database table names. Preserve explicit projection aliases, all composite-key parts and package/type identity.

Compatibility declaration comments may begin with ?, !, !!/digits and a JSON datatype hint before SELECT. Do not use JSON comments as an alternative binding schema; use fluent Required/Optional/status/type options.

## Velty and executable writers

Velty provides assignments, selectors/calls, if/elseif/else/end, foreach, supported for loops, break and expression evaluation. A SQL template and a writer service program have different capability allowlists.

~~~~text
#if($Has.Name)
  AND name = $Name
#end

#foreach($record in $Input.Records)
  $dml.Update("records", $record);
#end
~~~~

Custom writer programs can use supplied $dml.Insert/Update/Delete/Execute, $sequencer.Allocate, $validator.Check and registered helpers. A variable reference does not install a capability. Use typed StructQL identity helpers, not a raw-map row architecture.

Raw INSERT/UPDATE/DELETE are executable SQL, not reader metadata. Do not mix raw SQL, service calls and reader statements and expect automatic handler selection.

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
