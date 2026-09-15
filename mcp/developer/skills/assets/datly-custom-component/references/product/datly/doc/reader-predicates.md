# Reader predicates and groups

Optional predicates let one reader handle several combinations of filters without
interpolating request values into SQL. A predicate declaration describes one
condition; a group selects conditions; the Builder puts groups together with
explicit parentheses.

## Complete reader: scoped order search

This reader returns orders in the caller's trusted tenant **and** workspace,
optionally matching the name **or** reference, with optional inclusive total
bounds. Its Boolean shape is:

```text
(tenant_id = ? AND workspace_id = ?)
AND (name LIKE ? OR reference LIKE ?)
AND (total >= ? AND total <= ?)
```

The search and bounds groups disappear when their inputs are absent. The scope
inputs are required and fail binding when unavailable; they must never become
optional to make a request succeed.

Use `orders` as the component/source name. Configure the `main` SQLite connector,
link the Go shapes below under `example.com/app/orders/read`, and supply trusted
named providers for `tenant/tenant` and `workspace/workspace`. These are
application-defined binding kinds, not built-in authentication. The host must
verify the caller's identity and allowed workspace before providing those values.
Request query parameters cannot select them.

### orders.sql

```sql
#package('example.com/app/orders/read')
#setting($_ = $input_type('OrderSearchInput'))
#setting($_ = $output_type('OrderSearchOutput'))
#setting($_ = $case_format('lc'))
#setting($_ = $route('/orders/search', 'GET'))
#setting($_ = $connector('main'))
#define($_ = $TenantID<int>(tenant/tenant).Required().WithPredicate(0, 'equal', 'r', 'tenant_id'))
#define($_ = $WorkspaceID<int>(workspace/workspace).Required().WithPredicate(0, 'equal', 'r', 'workspace_id'))
#define($_ = $Search<string>(query/q).Optional().WithPredicate(1, 'contains', 'r', 'name').WithPredicate(1, 'contains', 'r', 'reference'))
#define($_ = $Minimum<int>(query/min).Optional().WithPredicate(2, 'greater_or_equal', 'r', 'total'))
#define($_ = $Maximum<int>(query/max).Optional().WithPredicate(2, 'less_or_equal', 'r', 'total'))
#define($_ = $Data<[]*Order>(output/view))
SELECT orders.*, type(orders, 'Order')
FROM (
    SELECT r.id, r.tenant_id, r.workspace_id, r.name, r.reference, r.total
    FROM purchase_orders r
    ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND"), $predicate.FilterGroup(1, "OR"), $predicate.FilterGroup(2, "AND")).Build("WHERE")}
    ORDER BY r.id
) orders
```

Names have separate jobs:

| Name | Meaning |
| --- | --- |
| `example.com/app/orders/read` | Explicit Go package identity from `#package` |
| `OrderSearchInput`, `OrderSearchOutput` | Component input and output types |
| `Order` | Root row type selected by `type(orders, 'Order')` |
| `Data` | Typed output holder, bound through `output/view` |
| `orders` | Named outer Datly view / SQL namespace; also the component/source name used here |
| `r` | Inner SQL table alias, used by predicate column arguments |
| `lc` | Lower-camel output case format; explicit JSON tags below fix the public field names |

The outer SELECT describes the Datly view and its row type. The inner SELECT is
the SQL source executed against `purchase_orders`. Predicate aliases refer to
that inner source: use `'r', 'total'`, not `'orders', 'total'` there. Do not assume
an outer alias alone overrides the component/source's root name; this example
uses `orders` consistently for both.

### Linked Go shapes

These types supply the typed row holder and internal presence markers used by the
runnable regression. Binding and predicate declarations remain in DQL.

```go
package read

type OrderSearchInput struct {
    TenantID    int
    WorkspaceID int
    Search      string
    Minimum     int
    Maximum     int
    Has         *OrderSearchHas `setMarker:"true" json:"-"`
}

type OrderSearchHas struct {
    TenantID, WorkspaceID, Search, Minimum, Maximum bool
}

type Order struct {
    ID          int    `sqlx:"id" json:"id"`
    TenantID    int    `sqlx:"tenant_id" json:"tenantId"`
    WorkspaceID int    `sqlx:"workspace_id" json:"workspaceId"`
    Name        string `sqlx:"name" json:"name"`
    Reference   string `sqlx:"reference" json:"reference"`
    Total       int    `sqlx:"total" json:"total"`
}

type OrderSearchOutput struct {
    Data []*Order `parameter:"Data,kind=output,in=view" view:"orders" json:"data"`
}
```

For an application-owned generated reader, author these same explicit type names
and use `datly transcribe get`; inspect the generated contracts and preserve their
presence bookkeeping. The regression here executes the DQL with linked Go shapes;
it does not establish every generation or deployment configuration.

## Declarations, optional inputs and group IDs

```sql
#define($_ = $Minimum<int>(query/min).Optional().WithPredicate(2, 'greater_or_equal', 'r', 'total'))
```

`Minimum` is the Go/DQL parameter, `min` is its query-string location, `2` is the
predicate group ID, `greater_or_equal` is the registered predicate, and `'r'`,
`'total'` are its authored alias and column arguments. The value comes from the
bound parameter. For these built-ins, alias and column are separate arguments;
`'r.total'` is not a replacement for the two-argument contract.

Group IDs are integer labels, not priorities, nesting levels or automatic
operators. This example uses 0 for scope, 1 for search, 2 for bounds. Group 0 is
the default when omitted from `WithPredicate`; using explicit IDs makes the
composition easier to audit. Only the groups you expand participate in SQL.
An undeclared group, or one with no active predicates, expands to an empty string.

`.Optional()` permits an absent input. Ordinary predicates skip absent values.
A supplied value enables every predicate attached to that parameter, so `Search`
enables both search conditions. Requiredness is enforced by binding, independently
of the group's Boolean operator. `ApplyWhenAbsentPredicate(...)` is a separate
opt-in for predicates whose contract explicitly handles absence; it is not a
substitute for required authorization inputs.

## Three levels of Boolean composition

| API | What it joins |
| --- | --- |
| `FilterGroup(1, "OR")` | Active predicates **inside group 1** |
| `CombineAnd(fragmentA, fragmentB)` | Nonempty fragments passed to **this call**, with AND |
| `CombineOr(fragmentA, fragmentB)` | Nonempty fragments passed to **this call**, with OR |
| `Combine(...)` | Same within-call behavior as `CombineAnd(...)` |
| `And()` / `Or()` | Set the connector between the accumulated expression and subsequent nonempty Combine calls |
| `Build("WHERE")` / `Build("AND")` / `Build("")` | Render a keyword-prefixed clause or a bare expression; empty Builders render nothing |

### AND between groups, OR inside search

The complete example uses this supported template expression:

```sql
${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND"), $predicate.FilterGroup(1, "OR"), $predicate.FilterGroup(2, "AND")).Build("WHERE")}
```

Changing the middle `FilterGroup` to `"AND"` requires **both** the name and
reference to match. Changing only `CombineAnd` to `CombineOr` allows any of the
scope, search or bounds groups to match, which is unsuitable for tenant scoping.
Keep the security group AND-connected to the complete optional expression.

### Successive calls with And and Or

This is equivalent for this reader and is exercised by the SQLite regression:

```sql
${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).And().CombineOr($predicate.FilterGroup(1, "OR")).And().CombineAnd($predicate.FilterGroup(2, "AND")).Build("WHERE")}
```

The one-argument `CombineOr` above receives an already-formed search group; its
internal OR comes from `FilterGroup(1, "OR")`. `CombineOr` does **not** change how
the call connects to previous content. For illustration, using authored fragments
`A`, `B`, `C` (schematic expressions, not request values):

```text
Builder().CombineAnd(A).CombineOr(B, C)       => A AND (B OR C)
Builder().CombineAnd(A).Or().CombineAnd(B,C)  => A OR (B AND C)
Builder().CombineAnd(A).Or().Combine(B).Combine(C)
                                             => A OR B OR C
```

The default between-call connector is AND. `Or()` persists until `And()` resets
it, including across empty Combine calls. It does not wrap all previous content
in a new group. Thus `Combine(A).Or().Combine(B).And().Combine(C)` means
`A OR (B AND C)` under SQL precedence, not `(A OR B) AND C`.

For `(A OR B) AND C`, put A and B into a **single** `CombineOr(A, B)` call,
then `.And().CombineAnd(C)`. For `scope AND (group1 OR group2)`, one useful
nested form is:

```sql
${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND"), $predicate.Builder().CombineOr($predicate.FilterGroup(1, "OR"), $predicate.FilterGroup(2, "AND")).Build("")).Build("WHERE")}
```

This last fragment changes the business policy: either search or bounds is enough,
but scope is still required. Use it only when that is the intended filter policy.

## Parentheses, empty groups and WHERE versus AND

`FilterGroup` wraps each emitted condition; multiple conditions get another group
wrapper. Each Combine call also wraps its nonempty fragments and its complete
within-call expression. Extra parentheses are intentional; do not strip them
while assembling a mixed AND/OR expression.

Empty fragments are skipped. A Combine call with only empty fragments contributes
neither parentheses nor a connector. If everything is empty:

```text
Builder().CombineAnd("", " ").Build("WHERE") => ""
Builder().CombineOr("").Build("AND")          => ""
Builder().Combine("").Build("")              => ""
```

An empty group is omitted, not treated as SQL TRUE or FALSE. For example,
`CombineOr(nonempty, empty)` produces only the nonempty condition. A completely
empty WHERE Builder imposes no restriction: the required scope in the main
example prevents that case from becoming an unrestricted order query.

`Build` does not inspect surrounding SQL. Use `Build("WHERE")` when it owns the
first WHERE; use `Build("AND")` after an existing condition. Do not leave a bare
`WHERE` or `AND` outside an optional Builder expression:

```sql
-- Fragment inside the same inner source. Fixed scope uses native named binds.
WHERE r.tenant_id = :TenantID AND r.workspace_id = :WorkspaceID
${predicate.Builder().CombineAnd($predicate.FilterGroup(1, "OR"), $predicate.FilterGroup(2, "AND")).Build("AND")}
```

This variant is tested with both optional groups empty: the fixed scope remains
valid and the Builder emits no dangling AND. `Build("")` is useful for a nested
expression; do not surround an optionally empty expression with unconditional `()`.

## Bind arguments and repeated parameters

For tenant 7, workspace 70 and `q=Alpha&min=0&max=20`, the logical SQL and ordered
arguments are:

```sql
WHERE (r.tenant_id = ? AND r.workspace_id = ?)
  AND (r.name LIKE ? OR r.reference LIKE ?)
  AND (r.total >= ? AND r.total <= ?)
```

```text
[7, 70, "%Alpha%", "%Alpha%", 0, 20]
```

The actual SQL has additional harmless parentheses. `contains` constructs the
LIKE pattern as a **bound value**. Quotes and SQL-looking text remain data.
LIKE wildcard semantics still apply; binding is not wildcard escaping.

Each emitted predicate appends its arguments in expansion order. Within a group,
that is parameter declaration order, then predicate declaration order for each
parameter. Between groups, it follows the expression's group evaluation order.
An absent group adds no arguments. Column names, operators and predicate names
are trusted authored metadata, never client-provided SQL fragments.

One parameter can have repeated `WithPredicate` options in the same or different
groups. The main example uses `Search` twice in group 1. This refinement adds a
third use of the same value in group 3:

```sql
#define($_ = $Search<string>(query/q).Optional().WithPredicate(1, 'contains', 'r', 'name').WithPredicate(1, 'contains', 'r', 'reference').WithPredicate(3, 'contains', 'r', 'name'))
```

Replace the original Search declaration with this declaration and append
`$predicate.FilterGroup(3, "AND")` as the fourth argument of the main
`CombineAnd`. That means `(name matches OR reference matches) AND name matches`;
a reference-only match is excluded. With all filters present the arguments become
`[7, 70, "%Alpha%", "%Alpha%", 0, 20, "%Alpha%"]`. Omitting `q` removes both
groups 1 and 3. The regression verifies both cases.

`FilterGroup` has a binding side effect: every evaluation appends the arguments
for its emitted predicates. Expand inline in the same order that fragments appear
in SQL. Do not evaluate a group just to check whether it is empty and then evaluate
it again for output. Do not cache one rendered group string and emit it twice:
two copies of its placeholders need two copies of its arguments. A Builder stores
SQL text, not an independently reusable SQL-plus-arguments object.

## Present zero versus absent

With a `Has` field tagged `setMarker:"true"`, the matching Boolean marker is
authoritative. The native binder sets it from suppliedness, not from whether the
bound integer is zero:

| Request | Bound Maximum | Has.Maximum | Resulting upper bound |
| --- | --- | --- | --- |
| no `max` | 0 | false | none |
| `max=0` | 0 | true | `r.total <= ?`, argument 0 |
| `max=20` | 20 | true | `r.total <= ?`, argument 20 |

The same distinction applies to false and empty strings. `q=` is present; the
contains predicate binds `%%`, whereas an omitted `q` contributes no search
predicate. This fixture's text columns are non-NULL; LIKE against NULL follows
normal SQL NULL semantics.

Without an authoritative marker, the predicate runtime falls back to value-based
presence: zero numbers, false, empty strings/collections and nil pointers are
unset. A nonnil pointer can represent a present zero. Keep generated markers or
use an appropriate pointer contract; do not use scalar zero as an absence sentinel
for optional numeric bounds. `Has` is internal bookkeeping, not part of request
JSON or the public response.

## Worked requests

For trusted scope tenant 7 / workspace 70, the fixture has totals -5, 0, 20, 50
and 30 at IDs 1, 2, 3, 4 and 8. `Alpha` matches name at IDs 1 and 3 and reference
at ID 2. Other rows deliberately match the search in another tenant or workspace.

| Query | Active optional groups | IDs returned |
| --- | --- | --- |
| none | none | 1, 2, 3, 4, 8 |
| `q=Alpha` | search | 1, 2, 3 |
| `q=Alpha&min=0&max=20` | search and bounds | 2, 3 |
| `max=0` | upper bound | 1, 2 |
| `min=0` | lower bound | 2, 3, 4, 8 |
| `min=1&max=25` | both bounds | 3 |
| `min=30&max=0` | both bounds | none |

Matching other-tenant and other-workspace rows never appear. A missing trusted
tenant fails before SQL evaluation. A second invocation with absent optional
filters does not inherit the previous invocation's groups or arguments.

## Custom predicates with the SDK handler

Use a custom predicate when its SQL depends on a cohesive application policy or
scoped dependency that the built-in predicates do not express. Implement the
actual SDK contract from `github.com/viant/xdatly/predicate`:

```go
type Handler interface {
    Compute(context.Context, any) (*Criteria, error)
}

type Criteria struct {
    Expression   string
    Placeholders []any
}
```

`value` is the bound value of the parameter carrying `WithPredicate`, with its
actual Go type after any codec conversion. It is not the whole request. For the
JWT declaration below it is `*jwt.Claims`; for `Reference` it is `string`. A
`bind:"kind=input,required"` field supplies the canonical component input when
policy needs other declared parameters.

### Complete DQL: custom authorization plus built-in search and bounds

Keep the linked `Order` and `OrderSearchOutput` shapes from the first reader.
Replace its input with the following expanded shape (do not declare both versions):

```go
package read

import "github.com/viant/scy/auth/jwt"

type OrderSearchInput struct {
    JWT         *jwt.Claims `json:"-"`
    TenantID    int
    WorkspaceID int
    Search      string
    Minimum     int
    Maximum     int
    Reference   string
    Has         *OrderSearchHas `setMarker:"true" json:"-"`
}

type OrderSearchHas struct {
    JWT, TenantID, WorkspaceID, Search, Minimum, Maximum, Reference bool
}
```

This variant adds `owner_subject` to the database table. The query does not expose
that column in its output. `JWT` comes from the declared Authorization header and
is verified by `JwtClaim`; tenant/workspace remain explicit trusted providers.
A separate invocation-scoped `ceiling` provider supplies the maximum authorized
total. The host must ensure that this ceiling and scope are authorized for the
verified principal; the fixture supplies a ceiling of 30.

```sql
#package('example.com/app/orders/read')
#import('jwt', 'github.com/viant/scy/auth/jwt')
#import('security', 'example.com/app/orders/security')
#setting($_ = $input_type('OrderSearchInput'))
#setting($_ = $output_type('OrderSearchOutput'))
#setting($_ = $case_format('lc'))
#setting($_ = $route('/orders/search', 'GET'))
#setting($_ = $connector('main'))
#define($_ = $JWT<string,*jwt.Claims>(header/Authorization).Required().WithCodec('JwtClaim').WithStatusCode(401).WithErrorMessage('authentication required').WithPredicate(0, 'handler', 'security.OrderScope'))
#define($_ = $TenantID<int>(tenant/tenant).Required().WithPredicate(0, 'equal', 'r', 'tenant_id'))
#define($_ = $WorkspaceID<int>(workspace/workspace).Required().WithPredicate(0, 'equal', 'r', 'workspace_id'))
#define($_ = $Search<string>(query/q).Optional().WithPredicate(1, 'contains', 'r', 'name').WithPredicate(1, 'contains', 'r', 'reference'))
#define($_ = $Minimum<int>(query/min).Optional().WithPredicate(2, 'greater_or_equal', 'r', 'total'))
#define($_ = $Maximum<int>(query/max).Optional().WithPredicate(2, 'less_or_equal', 'r', 'total'))
#define($_ = $Reference<string>(query/ref).Optional().WithPredicate(2, 'handler', 'security.ExactReference'))
#define($_ = $Data<[]*Order>(output/view))
SELECT orders.*, type(orders, 'Order')
FROM (
    SELECT r.id, r.tenant_id, r.workspace_id, r.name, r.reference, r.total
    FROM purchase_orders r
    ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND"), $predicate.FilterGroup(1, "OR"), $predicate.FilterGroup(2, "AND")).Build("WHERE")}
    ORDER BY r.id
) orders
```

`handler` is the registered built-in adapter; the following argument is the
**linked Go type identity**, not a new predicate name. Link the application package
and SDK types using the host's normal package/type configuration. The imports
declare package authority; `security.OrderScope` resolves to
`example.com/app/orders/security.OrderScope`. Explicit fully qualified identities
are also supported, and generated metadata retains the canonical identity. There is no
`RegisterPredicate("OrderScope", ...)` step. Only one handler type argument is
accepted; configure dependencies through bound fields instead of extra positional
DQL arguments. A missing linked type or a type that does not implement the SDK
interface fails artifact preparation.

### Application handlers

The following is one complete `security` package file. The read package above
contains the input type only; it does not import this Go package. DQL imports are
metadata, so they need not create a Go import cycle.

```go
package security

import (
    "context"
    "fmt"

    read "example.com/app/orders/read"
    "github.com/viant/scy/auth/jwt"
    "github.com/viant/xdatly/predicate"
)

type OrderScope struct {
    Input   *read.OrderSearchInput `bind:"kind=input,required"`
    Ceiling int                   `bind:"kind=ceiling,required"`
}

var _ predicate.Handler = (*OrderScope)(nil)

func (p *OrderScope) Compute(ctx context.Context, value any) (*predicate.Criteria, error) {
    if err := ctx.Err(); err != nil {
        return nil, err
    }
    claims, ok := value.(*jwt.Claims)
    if !ok || claims == nil || p.Input == nil || p.Input.JWT != claims || claims.Subject == "" {
        return nil, fmt.Errorf("verified authorization input required")
    }
    // Example business denial; replace with the application's access policy.
    if claims.Subject == "denied" {
        return nil, fmt.Errorf("order access denied")
    }
    return &predicate.Criteria{
        Expression:   "r.owner_subject = ? AND r.total <= ?",
        Placeholders: []any{claims.Subject, p.Ceiling},
    }, nil
}

type ExactReference struct {
    Enabled bool `bind:"kind=referencePolicy,required"`
}

var _ predicate.Handler = (*ExactReference)(nil)

func (p *ExactReference) Compute(ctx context.Context, value any) (*predicate.Criteria, error) {
    if err := ctx.Err(); err != nil {
        return nil, err
    }
    reference, ok := value.(string)
    if !ok {
        return nil, fmt.Errorf("reference must be a string")
    }
    if !p.Enabled {
        return nil, fmt.Errorf("reference filtering disabled")
    }
    // Explicit API policy: "all" and present-empty mean no extra refinement.
    if reference == "all" {
        return nil, nil
    }
    if reference == "" {
        return &predicate.Criteria{}, nil
    }
    return &predicate.Criteria{
        Expression:   "r.reference = ?",
        Placeholders: []any{reference},
    }, nil
}
```

Every placeholder in `Expression` has a corresponding value in `Placeholders`,
in SQL order. For claims subject `alice`, ceiling 30, tenant 7, workspace 70,
and `q=Alpha&min=0&max=20&ref=Alpha-2`, the logical WHERE is:

```sql
WHERE ((r.owner_subject = ? AND r.total <= ?)
       AND r.tenant_id = ? AND r.workspace_id = ?)
  AND (r.name LIKE ? OR r.reference LIKE ?)
  AND (r.total >= ? AND r.total <= ? AND r.reference = ?)
```

```text
["alice", 30, 7, 70, "%Alpha%", "%Alpha%", 0, 20, "Alpha-2"]
```

The custom fragment is parenthesized as one condition by `FilterGroup`. It can
contain internal AND/OR logic, but the handler must make that logic correct before
returning it. The scope group mixes one custom predicate and two built-ins; the
bounds group mixes two built-ins and an optional custom predicate. Builder rules
and argument ordering are identical for both kinds.

For an existing WHERE, retain all three groups and change only the prefix:

```sql
-- Fragment in the same inner SQL source; also covered by SQLite.
WHERE r.id > 0
${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND"), $predicate.FilterGroup(1, "OR"), $predicate.FilterGroup(2, "AND")).Build("AND")}
```

Never construct `Expression` with a raw token, subject, reference or query value.
`Expression` contains trusted SQL and placeholders; `Placeholders` carries data.
The custom reference test includes `x' OR 1=1 --` and matches only the row whose
reference contains those exact bytes.

### Verified JWT setup is explicit

Declaring an input named JWT does not install verification. Configure the native
verifier through `runtime/auth.New(ctx, &auth.Config{JWTValidator: config})` and
supply the returned codec factory to the host's existing component configuration
(`bootstrap.ArtifactInput.CodecFactory` in Go bootstrap). The validator config is
`github.com/viant/scy/auth/jwt/verifier.Config`; use the deployment's trusted
`CertURL` or RSA public-key resources. `JwtClaim` converts the declared header
string into verified native claims. Parsing a token without verification does not
satisfy this contract.

The required header and codec run before protected reader SQL. Missing credentials,
wrong signatures and expired tokens fail binding; the custom predicate does not
receive unverified claims. Verification alone does not grant access to a particular
row: `OrderScope` checks business identity and emits the owner condition, while
the trusted scope providers enforce tenant/workspace policy. The `Subject` meaning
and permitted issuer/audience follow the application's verifier configuration.
No undeclared `$JWT`, ambient principal or automatic tenant mapping is used.

The fixture verifies local RSA signatures and expiry with the shared JWT harness.
It does not establish remote certificate-cache refresh, production issuer/audience
configuration, or HTTP error serialization. A `WithStatusCode(401)` declaration
configures input-binding error metadata; it does not turn every custom business
error into HTTP 401. Use the application's typed error contract for denial status.

### Scoped injection and component results

The runtime creates a handler instance for each compiled predicate declaration in
the current predicate evaluation context and binds its fields through the invocation
binder. It may reuse that instance if the same group is expanded again in that
context; `Compute` is called again. Separate declarations of the same type have
separate instances. Do not use mutable package globals or assume one process-wide
singleton. A new query/template evaluation gets a fresh context.

Besides canonical `kind=input` and application-provided scalar policy values, a
handler can declare a component dependency. This is an alternative authorization
source, not an implicit lookup:

```go
// Fragment: AccessGrant is the application's linked component output type.
type AccessGrant struct {
    TenantID int
    Allowed  bool
}

type TenantAccess struct {
    Access *AccessGrant `bind:"kind=component,in=GET:/order-access,required"`
}

func (p *TenantAccess) Compute(ctx context.Context, _ any) (*predicate.Criteria, error) {
    if err := ctx.Err(); err != nil {
        return nil, err
    }
    if p.Access == nil || !p.Access.Allowed {
        return nil, fmt.Errorf("order access denied")
    }
    return &predicate.Criteria{
        Expression:   "r.tenant_id = ?",
        Placeholders: []any{p.Access.TenantID},
    }, nil
}
```

The host must register `GET:/order-access`, its typed input/output, and its
verification/authorization policy. That component must derive the grant from a
trusted declared identity, not merely echo a client tenant parameter. Attach
`TenantAccess` with `WithPredicate(0, 'handler', 'actual/package.TenantAccess')`
to a required input, and AND-connect its group. Component binding/execution errors
abort the protected read. The existing component-result SQLite regression proves
the injection/error-propagation mechanism; its tenant-echo fixture is not a
production authorization policy.

### Custom absence, empty results and errors

| Situation | Behavior |
| --- | --- |
| Optional triggering parameter absent | Normal custom predicate is skipped, including its dependency binding |
| Present zero/false/empty with an authoritative Has marker | Compute runs with that actual value |
| `ApplyWhenAbsentPredicate(group, 'handler', 'full/type.Name')` | Opts into invocation even when the trigger is absent; inspect canonical Has flags if policy needs to distinguish absence from zero |
| `Compute` returns `nil, nil` | No SQL condition and no arguments |
| Criteria has an empty/whitespace Expression | No condition; its Placeholders are not appended |
| Criteria has a nonempty Expression | Append that condition and its ordered Placeholders |
| Dependency binding or Compute returns an error | Fail the query; do not drop the condition and continue |
| Context cancelled | Honor `ctx.Err()` and stop; the example handlers check it explicitly |

Optional dependency skipping is useful for the reference refinement; it is unsafe
for required authorization. Never return nil/empty criteria to express access
denial: that **removes** the restriction. Return an error, or a deliberate false
condition if the API defines denial as an empty result. Do not hand back mismatched
placeholder/value counts and expect the Builder to repair them.

When a custom parameter is repeated across groups, each declaration receives its
own handler and the same bound parameter value; each emitted fragment contributes
its own placeholders. The same expansion-order rule and warning against caching
or discarding rendered groups apply to custom predicates.


> Packaging boundary: Maintained reader authoring examples and predicate composition semantics; excludes repository test navigation.
