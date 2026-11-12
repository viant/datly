# Shape Improvement Proposal

This note captures the main internal improvements suggested by the translator-to-shape migration work.

Scope:

- Applies to `repository/shape`
- Focuses on `DQL -> shape -> IR`
- Uses migration findings from grouping, summary, selector, and generated patch routes

## Goals

- Make shape the authoritative semantic model for DQL and Go-derived routes.
- Reduce runtime/bootstrap recovery logic.
- Replace translator-era implicit behavior with explicit shape metadata.
- Keep load/materialization modular so views, components, and resources can be built independently.

## 1. Promote `ComponentRoute` As A First-Class Shape Primitive

Observed gap:

- Route path, method, template strategy, and related component-level metadata were historically reconstructed outside shape.
- That encouraged direct `DQL -> IR` workarounds.

Proposal:

- Treat `ComponentRoute` as a first-class primitive produced by DQL compile.
- Carry at minimum:
  - `Method`
  - `RoutePath`
  - `TemplateType`
  - route-level connector/defaults
  - route-level metadata/docs/cache/auth flags

Target:

- `DQL -> ComponentRoute`
- `DQL -> View`
- `shape/load -> component/resource IR`

Benefit:

- Transcribe, bootstrap, and future `AddRoute` APIs can consume route metadata from shape only.

## 2. Make Template Strategy Explicit

Observed gap:

- Generated patch routes and translated exec routes were previously distinguished by inference.
- That was fragile and led to runtime fallbacks.

Proposal:

- Keep explicit DQL settings such as:
  - `#setting($_ = $useTemplate('patch'))`
- Store the resolved value on shape route metadata as `TemplateType`.

Recommended semantics:

- `translate` or empty: preserve authored DQL/Velty behavior
- `patch`: synthesize mutable Velty from shape AST/metadata
- future values may include `post`, `put`, `upsert`

Benefit:

- Removes heuristic detection of generated mutable routes.

## 3. Eliminate Runtime Type Recovery For Helper Parameters

Observed gap:

- Generated patch helpers such as `CurFoosId` and `CurFoos` needed runtime recovery of source type information.
- Local generator paths were more explicit than the early `v1` shape/transcribe path.

Proposal:

- Shape/transcribe should emit enough source/output type metadata so runtime codec initialization does not need to infer types from referenced params.
- Helper params should carry explicit source owner type and output type in shape/IR.

Benefit:

- Moves correctness back into shape.
- Reduces special handling in `view/state/parameter.go` and codec initialization.

## 4. Keep View-Level And Column-Level Semantics Separate

Observed gap:

- Grouping and selector metadata were easy to blur across view and column layers.

Proposal:

- View-level metadata stays explicit:
  - `Groupable`
  - selector namespace
  - selector constraints
  - summary URI / summary behavior
- Column-level metadata is explicit or inferred independently:
  - `ColumnConfig.Groupable`
  - inferred grouped projections from `GROUP BY`

Benefit:

- Avoids deriving view semantics from column accidents.
- Keeps Go tags and DQL hints aligned.

## 5. Add Dedicated Shape Primitives For Selector Holders

Observed gap:

- Flattening query selector fields into business input types makes Go shape contracts noisy and semantically wrong.

Proposal:

- Keep query-selector state as a separate shape concept.
- Support Go-derived contracts like:
  - business input holder
  - selector holder tagged with `querySelector:"viewAlias"`

Target Go model:

- `VendorInput` remains business input
- `ViewSelect` remains selector state
- shape merges both into component contract IR

Benefit:

- Aligns Go-derived shape with the DQL selector model.

## 6. Make Summary A Real Shape Concept, Not A Side Effect

Observed gap:

- Summary handling drifted between tags, parent view attachment, and runtime conventions.
- Multi-level summaries exposed gaps in child summary attachment and typing.

Proposal:

- Represent summary explicitly in shape at any view level.
- Include:
  - summary target view/ref
  - summary URI/source
  - parent attachment semantics
  - summary output schema/type

Benefit:

- Root summaries and child summaries can be materialized consistently from shape.

## 7. Add Recursive Mutable Generation For Nested Graphs

Observed gap:

- `patch_basic_one` and `patch_basic_many` became stable, but nested mutable graphs such as many-many flows need more general helper synthesis.

Proposal:

- Generalize mutable generation to recurse across relation graphs.
- Generate helper views, `IndexBy` maps, key propagation, and DML blocks per mutable node.

Examples:

- root collection patch
- nested child collection patch
- nested key propagation such as `FooId = parent.Id`

Benefit:

- Closes the remaining gap between local generate flows and shape-generated mutable routes.

## 8. Introduce A Strong Shape Validation Stage

Observed gap:

- Some failures were discovered too late at bootstrap/runtime.

Proposal:

- Expand `datly validate` as the primary shape-only validation gate.
- Validate:
  - DQL syntax and directives
  - SQL asset existence
  - route metadata completeness
  - helper type completeness
  - selector/summary/grouping consistency
  - generated mutable prerequisites

Benefit:

- Detects incomplete shape before runtime.

## 9. Add Deterministic Diagnostics Codes

Observed gap:

- Migration debugging spent too much time on ad hoc runtime errors.

Proposal:

- Extend shape diagnostics with stable codes across:
  - route metadata
  - selector metadata
  - summary attachment
  - mutable helper generation
  - groupable inference
  - type collisions

Benefit:

- Better tooling, tests, and compile-time failure handling.

## 10. Keep Load Modular By Primitive

Observed gap:

- Some behavior was easier to validate once view/component/resource loading was separated.

Proposal:

- Continue building around primitive loaders:
  - `LoadView`
  - `LoadComponentRoute`
  - `LoadComponent`
  - `LoadResource`
- Keep both inputs supported:
  - Go types
  - DQL

Benefit:

- Allows future APIs such as `AddRoute` to stay thin.
- Makes unit coverage sharper and reduces cross-coupled runtime fixes.

## 11. Reduce Direct Translator Dependence To Parity Specs And Fixtures

Observed gap:

- Migration often required checking local regression translator output to understand target semantics.

Proposal:

- Treat translator/local regression outputs as parity fixtures, not active implementation dependencies.
- Keep explicit parity docs and focused regression fixtures in shape tests.

Benefit:

- Shape remains independent while still preserving observable legacy behavior.

## Suggested Priority

1. `ComponentRoute` ownership in shape
2. Explicit `TemplateType`
3. Remove runtime helper type recovery by emitting complete helper metadata
4. Summary as explicit shape metadata
5. Recursive mutable generation
6. Broader `datly validate` coverage
7. Diagnostics standardization

## Success Criteria

The migration is structurally complete when:

- Bootstrap and transcribe no longer need to reconstruct missing semantics from raw DQL.
- Generated patch routes are selected explicitly, not inferred heuristically.
- Summary, selector, and grouping behavior are fully representable in shape.
- Runtime does not need shape-recovery logic for helper/source types.
- Local translator outputs are matched by shape through tests, not through runtime workarounds.
