# Named view projection authority

The outer DQL SELECT list defines each named view's output. The read compiler
partitions native sqlparser projection items by canonical namespace and preserves
SQL expressions and output aliases in the independently executable sources. It
never flattens the nested DB SQL or replaces a restricted SQL source with its
Table metadata. Outer root filters, ordering and limits retain their scope.
Unknown projection namespaces fail explicitly. Outer multi-view UNION and
DISTINCT remain unsupported rather than changing their row semantics.

Unselected ordinary fields disappear from generated row and presence shapes
through the existing ownership-controlled persistence path. Relation holders
and roles remain. For an outer projection over a named root source, an unlisted
named SQL child retains only matching and identity backing fields. A table-root
read retains each unlisted child's own SQL projection; explicit child selections
still narrow it. Bare table relations retain their natural table source.
Missing relation columns use the existing internal:"true" contract. Column
refinement retains a primary key only when SQLX table metadata and existing
source lineage prove that the named source already exposes it. Keys absent from
an inner projection, computed keys, ambiguous identities and grouped/set source
semantics do not grant authority to broaden that source.

Names and storage mapping are distinct: SQL output aliases remain canonical
column names, while established direct lineage supplies physical DML columns.
SQLX's existing alternative mappings carry exact aliases that differ from Go
field spelling, keeping the physical column first. No local row mapper is used.
After source resources resolve, the reader compiler maps a relation's SQL side
to its native projection source expression; its typed output field stays intact.
This includes aliases declared inside a named source.

Standalone annotations target projected outputs before backing fields are
inserted. Removed/renamed physical CAST, tag and invariant targets fail. Explicitly typed transient logical fields still require a corresponding
database output.
Wildcard physical CAST and invariant targets are also checked by SQLX discovery.
Quoted StructQL FROM paths use native identifier decoding so generated child key
helpers resolve the actual child rather than a same-named parent field.

An exact generated relation `on` tag may follow canonical output aliases only
when its prior type/tag still match the manifest and its child type is unchanged.
Other relation tags, source/destination authority, cardinality migration checks,
authored edits and staged publication retain their existing protections.

The original Datly shape compiler's canonical view declarations and internal
projection-column tags are the reference for ownership. This implementation
uses the current spec/read compiler, discovery and generation owners; it does
not restore the old translator or introduce a runtime/binding path.

## Independent-review correction

A required relation key must be available in its FROM/JOIN source. A closed
inner projection that omits the key is rejected, including an outer selector
that tries to name that missing inner output. A physical table source remains
open to database schema discovery even when its outer SELECT is narrowed.
An optional typed relation must not invalidate an invocation that selects only
unrelated scalar fields. Selected matching outputs remain subject to the normal
runtime projection checks after selector pruning.

Static inspection uses the builder's existing relation-marker preparation;
`$WHERE_CRITERIA` and `$View.ParentJoinOn(...)` remain in the authored SQL and
are expanded through the same canonical builder/template owners at runtime.
An opaque inner AST is not proof of a missing key. Known duplicate inner output
names are ambiguous and still fail.

Bootstrap relation inspection defers only the SQL projection owner's typed
unresolved-inspection error. This includes SQL awaiting template evaluation and
malformed SQL in a child that may be pruned. It does not establish output
existence or executable validity: SQLX metadata and the selected reader build
retain those checks. Namespace conflicts, known duplicates, computed child
predicate associations and proven closed-source missing keys still fail at
registration. Resources and templates retain their existing compilation owners.
The reader service prunes relations before constructing their selected queries;
selected malformed children still error, including after reuse of a plan.

Runtime relation links distinguish the source column used by a SQL predicate,
the authored result label requested for matching, and the typed Go field used
by the collector. Selecting a relation adds its actual output label rather
than accidentally requesting its predicate source name as a selector alias.
No prepared Go name is promoted to SQL alias authority.

The accompanying native sqlparser correction captures SELECT modifiers using
selectionKindCode and renders the AST Kind on round trips. Its modifier matcher
uses the native selector boundary so ordinary names such as all_records remain
identifiers. Datly's existing AST guard rejects only outer multi-view DISTINCT;
ALL and ordinary SELECT remain accepted and inner DISTINCT remains intact. No
textual DISTINCT workaround, fabricated AST or Datly parser replacement is used.

## Go type authority at discovery

Prefer outer `CAST(view.column AS GoType)` for pseudo and computed outputs. It
owns the Go type independently of an opaque inner CTE or database expression.
SQLX result metadata supplies output names and validates duplicates; CAST does
not supply missing outputs. A transient `sqlx:"-"` tag remains a separate,
explicit DML decision. Simple `'' AS name` and `0 AS name` projections may default
to `string` and `int`; literal inference is optional and never gates a CAST.
Unknown undeclared outputs receive no generic type fallback.

An incomplete inner dialect AST is not proof of a missing output. After SQLX
has returned and validated the result labels, an otherwise opaque named
wildcard can be materialized as an explicit outer list. Its inner query and
binding text remain unchanged. Discovery falsifies the outer query only; it
does not inject predicates into authored CTEs or nested sources.
