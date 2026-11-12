---
name: datly-native-first
description: Audit Datly 1.0 capabilities before adding handlers, query workarounds, or framework extensions; reuse native behavior and remove demonstrated duplication without weakening contracts.
---

# Datly v1: reuse before extending

Use this workflow before introducing application-side query logic, a custom handler,
new query-control metadata, or a Datly extension. Also use it to remove an existing
workaround that may duplicate v1. Passing tests do not justify redundant machinery.

## Establish the actual contract

- Identify the behavior the user needs, including input presence/defaults, output
  types, query grain, permissions, sorting, paging, and existing callers.
- Select the real v1 checkout/ref and the consumer's pinned version. Use the
  adjacent `datly-authoring/scripts/discover.sh` discovery workflow; read the
  selected canonical reader, writer, or custom-component skill and relevant grammar.
- Search that version's implementation, examples and tests. Documentation may
  describe a newer feature; source presence alone does not prove the running binary
  supports it. Do not use legacy Datly packages as a substitute for v1.

## Establish the cause before proposing a fix

Outer DQL joins describe graph nodes, key links and cardinality; join modifiers do
not confer database join semantics across views. Inspect executable SQL inside each
view separately. Read the compiled relation and its consumers before describing
how it executes or changing the graph.

For a generation failure, compare unchanged authored DQL with the edited source
using the same binary and metadata supplier. Trace the failed value from its
producer to the exact consumer. Check temporary tooling and supplied metadata
before attributing the error to Datly. In particular, column Name alone may not
satisfy writer relation planning: compare Source, types and constraints with the
native refiner's output. Repair incomplete supplied facts at their owner.

State which evidence exists: a reproducible baseline limitation, a regression
against a previously passing revision, or a caller/tooling defect. Do not describe
all three as “a framework gap.” Before a framework proposal, show the existing
mechanism, concrete failing path, smallest correction and compatibility impact.
Reuse parser-owned mechanisms; no SQL heuristics or name-based guesses.

## Prove reuse before writing an alternative

For each proposed mechanism, record the native candidate and a concrete execution
test. Use invented fixtures and the actual typed binding/compiler/reader path.
Tests should distinguish the intended behavior from the workaround, not merely
assert that both return some rows.

| Requirement | Native capability to check first |
| --- | --- |
| Database text/value filtering | Registered predicates, including `contains` where supported; verify exact signature, bindings and source semantics. Do not duplicate filtering with Go string scans or ad-hoc wildcard SQL. |
| Omitting one non-security filter for choice discovery | Optional input presence and native predicate activation. Omit the mapped input; do not substitute nil, zero or an invented sentinel without proving its semantics. |
| Choice/member queries | Native cube field projection/grouping with remaining predicates and bounded results. Reuse the existing authenticated producer boundary. |
| Sorting and paging | Reader selectors and admitted datasource `orderBy`, limit and offset/page contracts. No local sorting of a partial result as a substitute for global producer sorting. |
| Relations and metadata names | Native view relationships, complete join keys, cross-connector relations and cardinality metadata. Preserve outer `AND 1=1` to-one markers where authored; do not flatten the graph to evade binding errors. |
| Enumeration, typing and nullability | Existing Go shapes/tags, `choice`, validation opt-in, codecs and presence rules. No parallel enum parser or string `NULL` workaround. |

For filter omission, test normal execution as well as choice discovery: only the
edited non-security filter may disappear, other slices remain active, required
authorization/entity inputs still fail closed, and ordinary runs retain their
existing validated defaults. Explicit null/blank may fall back to a default in a
consumer; inspect and preserve that contract rather than claiming rejection.
Omission does not override source requiredness or defaults. Establish an authorized
discovery binding first; changing internal source suppliedness requires a caller
audit and proof that normal public execution is not weakened.

Verify predicate semantics, not just its name: native `contains` may retain SQL
LIKE wildcard behavior for `%` and `_`. Test wildcard, quoting and collation cases
before claiming literal substring equivalence.

Classify a failure before changing the framework: missing datasource declaration,
wrong input path, stale published artifact, unsupported native feature, or browser/
test-tool failure. Fix the layer supported by evidence.

## When a native capability exists

Use it. Remove only the demonstrated duplication: extra handler branches, bypass
inputs, SQL guards, adapters or metadata that the native path replaces. Keep the
minimal authenticated publication/access adapter and legitimate public-to-native
field mappings. Do not delete unrelated work merely because it is custom code.

Inspect callers before removal. Preserve public compatibility or explicitly migrate
owned consumers together. Replace workaround tests with native execution coverage;
retain independently authored values and negative authorization cases. Rebuild and
verify the consuming application, browser interactions and affected exports.

## When a real gap remains

Document the exact version, required behavior, searched native candidates, and the
smallest failing executable reproduction. Propose the smallest backward-compatible
Datly v1 extension at the existing capability boundary. Keep public libraries
business-neutral; no report-ID switches, parallel parser/query engine or extra DAO.
Add native regression and affected-consumer tests. Do not conceal a gap behind an
application workaround without an explicit user decision.

## Evidence to leave behind

Record: requirement → existing native API/grammar → pinned revision → execution
proof → duplication removed or precise remaining gap → consumer verification.
Separate authored, tested, committed, published and deployed states. Never describe
a local overlay or a mock as completed runtime integration.
