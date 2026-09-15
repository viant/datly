# Reader predicates: built-in and custom groups

Use the [complete predicate guide](../../../datly/doc/reader-predicates.md) for scoped AND/OR readers, optional bounds, present-zero markers, repeated parameters and the custom-handler/JWT example.

- `FilterGroup(group, "AND"|"OR")` joins predicates within that integer group. `CombineAnd`/`CombineOr` join fragments within one call; `And()`/`Or()` control the persistent connector between calls. Preserve parentheses and AND-connect required authorization to the entire optional expression.
- Empty groups contribute neither SQL nor arguments. `Build("WHERE")` and `Build("AND")` emit nothing when the complete Builder is empty. Match the keyword to the surrounding source.
- Custom predicates implement SDK `predicate.Handler.Compute(context.Context, any) (*predicate.Criteria, error)`. Return trusted SQL in `Expression` and ordered data in `Placeholders`. Select the built-in `handler` adapter with a declared import alias or the actual linked full Go type identity in `WithPredicate`; do not invent a predicate-name registration step or extra configuration arguments.
- DQL imports and explicit package/input/output/root types remain part of the authoring contract. Custom handler aliases resolve through declared imports, and generated metadata retains the canonical type identity.
- Scoped `bind:"kind=input,required"` exposes the declared canonical input. JWT requires a declared credential source, `JwtClaim` codec and configured native verifier; no undeclared claims are injected. A `kind=component` dependency requires an explicitly registered typed component and real authorization policy.
- Ordinary absent optional triggers skip custom dependency binding and Compute. Nil/empty criteria omit a condition, so required authorization must fail with an error when denied. Binding/Compute errors abort the protected read.
- Every expansion appends arguments. Repeated parameter declarations across groups each bind their emitted values; do not cache a SQL-only group string for multiple uses or evaluate a group only to discard it.

The guide includes complete DQL and Go contracts plus realistic SQLite evidence for both built-in grouping and custom/built-in grouping under verified identity.
