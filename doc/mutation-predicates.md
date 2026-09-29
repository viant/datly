# Atomic predicates for generated mutations

Generated PATCH/PUT views may select an existing input predicate group for the
UPDATE/DELETE condition. This uses the reader's predicate compiler, built-ins,
custom `predicate.Handler`, Has activation and ordered bind arguments.

```sql
#define($_ = $ExpectedOwner<string>(query/expectedOwner).Required().WithPredicate(7,'equal','records','owner'))
#define($_ = $MaxAttempt<int>(query/maxAttempt).Optional().WithPredicate(7,'less_or_equal','records','attempt'))
SELECT r.*, type(r,'Record'), mutation_predicate(r,7)
FROM (SELECT id,title,owner,attempt FROM records) r
```

This is an annotation fragment for an ordinary writer DQL with explicit package,
route, connector, input/output and body holder. Transcribe with `patch` or `put`.
The generated body-view tag carries `mutationPredicate=7`; application Go does
not construct mutation plumbing. An absent group, an auxiliary view or a
GET/POST component is rejected before emitting artifacts.

Use the physical table as the predicate qualifier (`records` above); a local
read alias (`r`) is not an UPDATE/DELETE alias. Operators remain existing named
predicates such as `equal`, `less_or_equal`, `greater_or_equal` and `is_null`.
Custom business predicates still return trusted expression text and bound data.
Never accept arbitrary client SQL as a mutation condition.

`.Required()` retains normal input binding validation. `.Optional()` skips a
predicate when its value is absent; explicit zero and empty string remain
supplied. An empty group adds no mutation condition. Security conditions must
use required input or a custom predicate that fails when authorization is denied.
Generated Previous reads retain their own authored authorization scope; mutation
predicates add an execution guard and do not replace those read filters.

The writer evaluates the selected group against its canonical invocation input
at Queue and copies the result into the managed operation. SQLX owns
`Criteria{Expression, Placeholders}` and accepts it as an update/delete service
option. Datly bridges the existing SDK predicate result; xdatly gains no SQLX
dependency. Criteria compose with full identity and any existing IfMatch token.
INSERT is unaffected. Sparse SET fields and caller-owned transactions retain
native behavior. A guarded update/delete that affects no matching row returns
`*handler.Conflict` (409), and managed completion rolls back earlier actions.

Existing `concurrency_token` also supplies an atomic IfMatch guard in the
current v1 runtime. Older writer documentation describing it as validation-only
is stale. General mutation predicates extend that existing capability to
compound/range/custom conditions without a second predicate language.

SQLite acceptance covers generated optional/required inputs, equality and range,
explicit empty/zero, matching/stale deletion, metadata regeneration, queue-to-
execution races, sparse updates, criteria snapshots, IfMatch composition,
rollback of earlier writes and caller-owned transaction rollback.
