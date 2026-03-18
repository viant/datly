# Component Contract Parity Target (Shape)

This document defines the target behavior for cross-component contract discovery in `repository/shape`.

Scope:

- Applies to DQL compile flow (`compile -> plan -> load`)
- Does not depend on `internal/*` packages
- Defines observable behavior and acceptance criteria

## Problem Statement

When DQL declares component dependencies (for example via component-typed state declarations), shape compile should produce component-facing IR that is functionally equivalent to translator contract/signature resolution for:

- route reference normalization
- output schema/type resolution for component states
- dependent type propagation
- deterministic diagnostics

Today, shape produces useful plan artifacts for views/states but component contract resolution parity is incomplete.

## Target Semantics

### 1) Reference Forms

A component reference MUST support these forms:

- Relative: `../acl/auth`
- Method-qualified absolute route: `GET:/v1/api/platform/acl/auth`
- Absolute route without method: `/v1/api/platform/acl/auth` (defaults to `GET`)

Normalization target:

- Stable route identity represented as `method + uri` (default method `GET`)
- Namespace/path derivation remains deterministic for file-layout lookups

### 2) State Enrichment

For each `plan.State` with `Kind == "component"`:

- `In` retains user-declared logical reference (for traceability)
- `DataType` is inferred from referenced component output when not explicitly declared
- `OutputDataType` is preserved if user declares explicit output type

If inferred type is unavailable, a diagnostic is emitted (see diagnostics section).

### 3) Type Propagation

Referenced component route/resource types required for consuming component states SHOULD be appended to `plan.Result.Types` unless a collision exists.

Collision policy:

- Existing local type names win
- Emit collision diagnostic for skipped imported type

### 4) Nested Component Dependencies

If referenced route/resource parameters include additional component references:

- Resolver walks nested dependencies transitively
- Cycle detection MUST prevent infinite recursion
- Cycle reports a deterministic warning diagnostic

### 5) Loader Classification

Shape-loaded component artifact SHOULD classify component dependencies as input-like contract dependencies (not miscellaneous "other").

## Diagnostics Contract

Component-related diagnostics use `DQL-COMP-*` codes:

- `DQL-COMP-REF-INVALID`
- `DQL-COMP-ROUTE-MISSING`
- `DQL-COMP-ROUTE-INVALID`
- `DQL-COMP-CYCLE`
- `DQL-COMP-TYPE-COLLISION`

Requirements:

- Deterministic code/message per failure class
- Span points at the referenced component token when available
- Warnings by default unless compile strict mode escalates

## Non-Goals (Step 1)

- No resolver implementation changes
- No signature engine wiring changes
- No compile pipeline behavior changes

This step defines the contract only; implementation phases follow separately.

## Acceptance Criteria

The parity contract is considered defined when:

1. Reference normalization rules are explicit and unambiguous.
2. Required state/type/diagnostic behavior is documented for success and failure paths.
3. Nested dependency and collision behavior is documented.
4. Constraints are independent of `internal/*` packages.
