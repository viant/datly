# Datly application concepts

## Philosophy

Datly organizes a backend around **data-driven components**: a named, typed input/output contract, its data dependencies, and its behavior. A component can be a reader, generated writer, or custom handler. It can be exposed through HTTP and business MCP tools, or remain an internal dependency.

Do not recreate a DAO/service/controller stack just to forward a query. Declare data shape, binding, relations, validation and behavior; use Go hooks or a custom handler where business logic belongs. This does not forbid ordinary application services or custom orchestration.

DQL and Go shapes are complementary. DQL expresses data intent and metadata. Go shapes express typed application contracts and reusable behavior. Neither requires the other for every component.

## Choose an authoring mode

| Mode | Use when | What the developer owns |
| --- | --- | --- |
| Go shapes only | Existing Go application contracts already describe the component | Components declaration, tags, SQL/resources where needed, hook/handler methods |
| DQL + linked Go shapes | Queries evolve but application types are established | DQL, imports, selected existing types, application hooks |
| Dynamic DQL + persisted shapes | Generate a package from DQL and keep Go source | DQL and authored hook files; review owned projection additions, CAST type changes and removals |
| Dynamic DQL + runtime shapes | DQL is loaded dynamically, possibly from a DB | DQL version, resource/type bindings, validation and atomic reload workflow |
| Custom Go or Velty component | Application orchestration is not a standard reader/writer | Handler, typed contract, scoped capabilities and explicit side-effect policy |

Runtime-only generation is a required authoring mode; ask the developer MCP server to confirm its support. Persisted Go generation is not a silent substitute if runtime-only behavior was requested.

## Terms

| Term | Application meaning |
| --- | --- |
| Component | A named backend capability with input and output types |
| Go shape | A Go struct/type defining the contract, including tags and methods |
| Input | Values bound from request, constants, resources, other components or views |
| Output | The public result; writer output is the transformed request body with final IDs/links |
| View | A typed query result, optionally related to other views |
| SQL namespace | The SQL alias, such as `r` in `FROM records r`; used by DQL view controls |
| Relation | A parent/child association, normally with explicit join-key equalities |
| DerivedView | A query-derived result such as count, pagination data, totals or aggregation; not a single special summary slot |
| Self reference | A relation between rows of the same entity type, identified by child/parent keys |
| Connector | A configured database connection name; not credentials supplied by a client |
| Predicate | A parameterized, reusable filter expression or custom filter handler |
| Selector | A controlled client choice of fields, filters, order, limit, offset or page |
| Codec | A typed conversion between representation and application value |
| Presence / Has | Internal suppliedness flags; omission differs from a supplied zero, false, empty string or null |
| Original identity | The tuple and presence captured before initialization; it decides matching, not a later generated ID |
| Current / Previous | A DB read used for comparison; `Previous` is the detached matching row available to writer hooks |
| PreviousFields | Which previous fields were actually loaded; SQL NULL is loaded, omission is not |
| Invariant group | Fields that must be considered together, such as Start/End or Unit/Cap |
| Sequencer | Allocates IDs before queued writes |
| Queue | Buffer a mutation; it is not a commit |
| Outcome | Actual completion state: committed, rolled back, caller-pending, unknown or mixed |
| Exposure | Which components/packages become public routes/tools; dependencies need not be exposed |
| Developer MCP server | Authoring/inspection/validation tools used to create components |
| Business MCP server | Runtime tools/resources clients call after a component is deployed |

## Base types and shape semantics

Use normal Go types: `string`, `bool`, integer/unsigned types, floating-point types, named scalar types, `time.Time`, pointers, slices, maps where genuinely appropriate, nested structs and imported named/generic types. Use a project-approved decimal type for precise decimal quantities; do not invent a precision-safe float convention.

- `T` is a value; `*T` can also express null/absence at the Go value level.
- `[]T` and `[]*T` express collections. Preserve nil versus empty if the public contract distinguishes them.
- A pointer alone is not a complete PATCH presence contract. A supplied null and an omitted nullable field can both yield nil; Has differentiates them.
- `any` is for genuinely dynamic values, not a replacement for known row shapes.
- A Go import alias is shorthand, not identity. `model.Record` resolves through the declared full package path.
- Maps may be useful payload fields or deliberate error objects; normal SQL results should remain typed rows.
- JSON names, Go names and SQL column names are independent. Make mappings explicit when they differ.
- A scalar's zero value is not proof that a field was omitted.

## Public versus physical shape

A public logical field can have a rich Go shape while its storage is spread across columns. For example, `Bounds{Unit, Cap}` may represent internal `BOUND_UNIT` and `BOUND_CAP`.

The required DQL pattern is a pseudo projection plus `CAST(r.bounds AS model.Bounds)`, with a declared import. Use tags to make the public logical field non-DML and the backing physical fields internal. **Internal is not transient**: backing fields are hidden from public contracts but still read and written.

OnFetch assembles the logical value. Writer Init maps supplied logical/nested members back into the physical values and marks only corresponding fields. Sparse nested omission must not overwrite an existing backing value. The developer server must verify the resolved shape and mapping; rich CAST does not automatically mean every physical JSON/custom-cast column is non-DML.

## Error and validation philosophy

Framework checks precede custom business validation. They include schema/Go rules and DB-backed required/nullability, uniqueness and reference checks where authoritative metadata supports them.

For a new row, run complete checks. For a sparse existing row, skip field checks when its Has flag is false; validate supplied values when true. Backfill supplies values for coherent validation but does not mark them as client changes.

Keep SQL NOT NULL distinct from “nonzero required”: 0, false and empty text can be valid DB values. Explicit business rules may impose stronger requirements.

An application can choose status, message, error object and violations. Preserve explicit empty strings, false and null. Keep private causes out of the public payload. HTTP carries the selected status; MCP reports a tool error with the structured application body rather than pretending an application validation failure is transport authentication failure.

## Transactions and lifecycle

Readers do not become writers merely because their route uses a particular method. Generated mutation behavior is explicitly selected. Custom handlers choose their orchestration.

The first managed transaction owner controls completion; nested components share it. Flushing supplied work does not make a child the commit owner. Publish commit-dependent messages only after a confirmed successful outcome, not after Queue or a caller-pending result.

## Authoring without internals

An application developer needs connector names, schema/constraints, public types, DQL, tags, hooks and expected behavior. Use developer tooling to inspect and compile those. You do not need to edit Datly's parser, runtime, row collector or dependencies to author a component. A missing required capability should be reported as such, not worked around by silently changing the component's semantics.
