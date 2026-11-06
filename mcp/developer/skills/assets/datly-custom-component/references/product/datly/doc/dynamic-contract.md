# Caller-dependent contracts

Use a `Contract` method on a linked input, output, domain type or nested composite
to select the fields and annotations exposed to the current caller:

```go
Contract(context.Context, bindly.Binder) (*contract.Policy, error)
```

Import `github.com/viant/bindly` and `github.com/viant/xdatly/contract`. The host
supplies the native binder; the application chooses the state to bind. The same
method source works in both supported Datly versions.

Start with the role-based example below, or use the [direct operation example](contract-operation.md)
when you only need POST/PATCH requiredness rules. [Field/schema examples](contract-field-examples.md)
show the published OpenAPI result of each kind of adjustment.

## Application example

```go
package catalog

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/bindly"
	"github.com/viant/xdatly/contract"
)

type CatalogItem struct {
	ID           string                                              `json:"id" sqlx:"ID,primaryKey" required:"true"`
	Title        string                                              `json:"title" validate:"required"`
	Commission   float64                                             `json:"commission"`
	PricingModel int                                                 `json:"pricingModel" choice:"0:fixed" choice:"10:usage" choice:"20:subscription"`
	Has          *struct{ ID, Title, PricingModel, Commission bool } `json:"-" setMarker:"true"`
}

type Authorization struct {
	CanViewCommission bool
	Role              string
}

func (*CatalogItem) Contract(ctx context.Context, binder bindly.Binder) (*contract.Policy, error) {
	// The application chooses the state type and fields. Auth can instead be
	// verified JWT claims through the configured JwtClaim codec, or other
	// native locator/component data. Binding does not call this state's Init.
	var state struct {
		Auth   *Authorization `parameter:",kind=component,in=GET:/auth/context,required=true"`
		Method string         `parameter:",kind=contract,in=method"`
		Debug  bool           `parameter:",kind=header,in=X-Debug"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	if state.Auth == nil {
		return nil, fmt.Errorf("authorization context required")
	}
	// Compute permissions once. The name describes equivalent metadata edits,
	// not a caller identity. Include every decision that affects the result.
	showCommission := state.Auth.CanViewCommission
	creating := state.Method == "POST"

	// Select the enum domain and wire representation from verified role data.
	// Explicit ordinals keep storage stable when a role sees only a subset.
	var choices []string
	var outputStyle, pricingDescription string
	switch state.Auth.Role {
	case "editor":
		choices = []string{"0:fixed", "20:subscription"}
		outputStyle = "literal"
		pricingDescription = "An editor can select fixed or subscription."
	case "finance":
		choices = []string{"0:fixed", "10:usage", "20:subscription"}
		outputStyle = "ordinal"
		pricingDescription = "Finance users can also select usage."
	default:
		return nil, fmt.Errorf("unsupported role %q", state.Auth.Role)
	}

	// Version this name if the role-to-schema rules change without a reload.
	// All choices above are determined by role; no permission is recomputed
	// inside the field callback.
	name := fmt.Sprintf("catalog-item:v1:%s:commission=%t:create=%t", state.Auth.Role, showCommission, creating)
	return &contract.Policy{Name: name, Field: func(parentPath string, parentType reflect.Type, field *contract.Field) (*contract.Field, error) {
		if parentType != reflect.TypeFor[CatalogItem]() {
			return nil, nil
		}
		switch field.Name {
		case "Commission":
			if !showCommission {
				return field.Clone().Hide(), nil
			}
		case "Title":
			if creating {
				return field.Clone().Required(), nil
			}
			return field.Clone().Optional(), nil
		case "PricingModel":
			return field.Clone().SetEnum(choices...).
				SetTag("enumAccept", "literal").
				SetTag("enumStyle", outputStyle).
				SetDescription(pricingDescription), nil
		}
		return nil, nil
	}}, nil
}

type Input struct {
	Body []*CatalogItem `parameter:",kind=body,required=true"`
}

type Output struct {
	Data []*CatalogItem `json:"data"`
}
```

For the `PricingModel` field, the same linked `int` now has these caller-specific contracts:

| Verified role | Effective choices | Input schema / JSON | Go storage | Output schema / JSON |
|---|---|---|---|---|
| `editor` | `0:fixed`, `20:subscription` | string enum `["fixed", "subscription"]`; `{"pricingModel":"subscription"}` | `PricingModel == 20` | string enum `["fixed", "subscription"]`; `{"pricingModel":"subscription"}` |
| `finance` | `0:fixed`, `10:usage`, `20:subscription` | string enum `["fixed", "usage", "subscription"]`; `{"pricingModel":"usage"}` | `PricingModel == 10` | integer enum `[0, 10, 20]`; `{"pricingModel":10}` |

These are individual catalog items; `Input.Body` and `Output.Data` carry the
same policy through their slices automatically. `SetEnum` replaces the original
choice group, so the editor schema does not retain `usage`. Using explicit codes
means `subscription` remains `20`, rather than becoming ordinal `1` when the domain narrows.
MCP and OpenAPI publish the selected domain and wire type for each direction.

An enum domain is not a substitute for permission/transition validation. Keep that
validation in the existing business/validator path. Cross-form conversion requires
a matching choice: with the editor's literal output, returning stored value `10`
would fail because that role's mapping omits `usage`. Return records within that
domain, or keep a complete domain for output when all stored pricing models must be readable.

The native dispatcher owns the `/auth/context` component's resolution and execution.
The target business handler does not run during schema discovery. JWT codecs use
the session's configured verifier; raw JWT decoding is not authorization.
`kind=contract` method/path values come from the registered target route, not the
outer OpenAPI GET or MCP POST request. Metadata-state bindings cannot replace them.
The host owns provider precedence, verifier selection, component resolution and
codec execution. Bind only the state needed to select the policy.

## Related examples

- [Direct POST/PATCH example](contract-operation.md): `IsPK()` and `Optional()`,
  preserving primary keys while clearing other requirements on PATCH.
- [Field methods and schema output](contract-field-examples.md): OpenAPI before/after
  fragments, descriptions, examples, renaming, and enum wire forms.

## DQL and named operations

Expose separate tools for declared operations:

```sql
#setting($_ = $route('/catalog/items', 'POST', 'PATCH'))
#setting($_ = $mcp_route('POST:/catalog/items', 'CreateCatalogItem'))
#setting($_ = $mcp_route('PATCH:/catalog/items', 'PatchCatalogItem'))
```

Keep the component's existing input/output/handler declarations and executable DQL.
These settings name existing method/path targets; they do not create a writer or
change its insert/update policy. Tool names must be distinct.

For PATCH, `Optional()` changes exposed requiredness. Entity `validate` tags and
native presence-aware insert/update validation retain their existing behavior.
Fields are optional unless explicitly required. Optional does not mean nullable;
a non-pointer scalar does not become nullable just because it is optional.
OpenAPI path parameters retain the specification's required-presence rule.

## Target operation versus HTTP request

Use the normal request selector when you want the HTTP request owned by the provider:

```go
Method string `parameter:",kind=http_request,in=method"`
```

It still works. It is not always the operation whose contract is being described.
An OpenAPI request can be GET while describing a PATCH component; MCP discovery
can run without a target HTTP request. A protocol adapter may also supply a synthetic
request. Do not infer a write operation solely from the outer transport request.

The binder passed to a Contract hook additionally supplies trusted target metadata:

```go
Method string `parameter:",kind=contract,in=method,required=true"`
Path   string `parameter:",kind=contract,in=path,required=true"`
```

| Situation | Target `contract.method` | Existing `http_request.method` |
|---|---|---|
| Direct PATCH of the component | PATCH | PATCH for that HTTP request |
| GET of OpenAPI describing that component | PATCH | GET for the documentation request |
| MCP discovery/call for its PATCH tool | PATCH | Provider/transport dependent; it may be absent |

`contract.path` is the declared target path, such as `/catalog/items/{id}`, rather
than the outer documentation URL. These providers exist in the Contract binding
scope, not as a general replacement for ordinary handler binding. Protocol and
component providers cannot override their values.

You only need the values your policy uses. The [direct PATCH example](contract-operation.md)
binds Method and preserves `field.IsPK()` fields; it needs no Path value or operation
utility. If your hook already has the correct business method, reuse it. An already-bound
HTTP method is suitable when you know that request represents the target operation;
it is not a portable discovery rule.

## Representation and composition

Only field names and tags can change. The linked storage layout, field types,
positions, offsets, embedding and methods remain intact. Hidden fields still exist
in application state: input decoding ignores them and output encoding omits them.
`Hide()` sets `json:"-"`, `internal:"true"` and `mcp:"-"`.

Traversal uses `viant/x/shape` and the shared Xdatly contract. Nested hooks are
found automatically through pointers, slices, arrays, map values, and embeddings.
Nested policies run first; parent policies have final authority. Hooks use fresh
metadata receivers, not populated business instances; bind state explicitly.
Presence-holder fields are infrastructure and are not passed to business field policies.

A nil policy or no effective field edits keeps the original linked path. Nil from
`Field` retains the field and continues traversal. `field.SkipChildren()` skips
descendants only for that policy; nested owners and other parent policies remain
independent. Tag helpers replace/add values; `RemoveTag` explicitly removes them.

## Every `Field` method

The callback receives `*contract.Field`, which exposes two editable members:
`Name string` and `Tag reflect.StructTag`. It does not expose a writable field type,
offset, index or embedding flag. Read a tag with `field.Tag.Get("json")` or
`field.Tag.Lookup("required")`; prefer the helpers below for edits.

Mutating helpers update their receiver and return `*Field` for chaining. Call them
on `field.Clone()` to preserve the callback input.
They preserve unrelated tags. Helper errors are recorded on the field; Datly checks
them before publishing a variant. Returning `nil, nil` discards that callback's edit
and continues traversal. Returning a non-nil error aborts contract resolution.

| Method | Behavior |
|---|---|
| `Clone() *Field` | Copies name, tags, recorded error and traversal flag; useful when keeping the supplied field unchanged locally. |
| `Hide() *Field` | Sets `json:"-"`, `internal:"true"`, and `mcp:"-"`. Storage remains present; effective JSON input ignores it and output omits it. |
| `IsPK() bool` | Recognizes explicit SQLX `primaryKey` / `autoincrement` options without inferring from a field name. It does not modify metadata. |
| `Required() *Field` | Sets `required:"true"`, removes `optional`, and updates the `required` option in existing `parameter` / `bind` tags. |
| `Optional() *Field` | Removes `required` and `optional` tags and the `required` option in existing `parameter` / `bind` tags. It does not add `required:"false"`, change nullability, or remove entity `validate` rules. |
| `SetEnum(values ...string) *Field` | Replaces every `choice` occurrence with the supplied ordered choices. Supports literals and explicit `ordinal:literal` entries. An empty argument list records an error; use `RemoveTag("choice")` to remove the domain. |
| `SetDescription(value string) *Field` | Replaces/adds `description`. |
| `SetExample(value string) *Field` | Replaces/adds `example`. Choose an example compatible with the exposed wire form; different input/output forms may need separate owner policies or no shared example. |
| `SetTag(key, value string) *Field` | Replaces all occurrences of that key with one value, or adds it if absent. For repeated choices, use `SetEnum`. |
| `RemoveTag(key string) *Field` | Removes all occurrences of one key. Removing a missing key is a no-op. |
| `SkipChildren() *Field` | Skips descendant callbacks for this policy only. It does not itself hide/change the field or suppress nested owners' policies. Can be chained after an edit. |
| `ChildrenSkipped() bool` | Reports this field's traversal flag. |
| `Err() error` | Returns a recorded helper error without checking the original field type. |
| `Validate(originalType reflect.Type) error` | Checks recorded errors, a valid Go field identifier, tag syntax and choices against the original type. Datly calls this with the canonical field type; normal hooks need not call it themselves. |

For example, inside a callback that has matched the business type and field name:

```go
return field.Clone().
    SetDescription("Caller-visible pricing model").
    SetExample("subscription").
    RemoveTag("deprecated"), nil
```

There are no dedicated `SetEnumAccept`, `SetEnumStyle`, or `Rename` methods.
Use `SetTag("enumAccept", "literal")`, `SetTag("enumStyle", "ordinal")`,
and assign `Name` directly. `SetTag` only changes metadata: it does not move a
parameter to a different HTTP binding or replace the configured codec.

## Even the exposed field name can change

`Field.Name` is the effective field name seen by metadata consumers. The original
linked struct still has its original Go field name and storage. Changing `Name`
alone does not override an explicit `json` tag: `PricingModel` has `json:"pricingModel"` above,
so changing only `Name` would leave its JSON key as `pricingModel`.

To expose that field as `billingModel` in JSON/OpenAPI and `BillingModel` in MCP, a matching
callback can return:

```go
case "PricingModel":
    rewritten := field.Clone()
    rewritten.Name = "BillingModel" // effective metadata name; must be a Go identifier
    return rewritten.
        SetTag("json", "billingModel,omitempty").
        SetTag("mcp", "name=BillingModel,aliases=billingModel").
        SetEnum(choices...).
        SetTag("enumAccept", "literal").
        SetTag("enumStyle", outputStyle), nil
```

For a finance user, JSON input is now `{"billingModel":"usage"}` and JSON output is
`{"billingModel":10}`. MCP advertises `BillingModel` and normalizes its authored alias `billingModel`
to the JSON key. The handler still reads/writes **`CatalogItem.PricingModel`**, with type
**`int`**; it does not gain a `BillingModel` Go member. Presence flags likewise
continue to use the canonical field identity. Avoid exposed-name/alias collisions.
If the rename is conditional on role, include that decision in `Policy.Name` too.

The callback's `parentPath` is the canonical parent path, excluding the current
field; `parentType` is the original linked parent type. For example, a catalog item
member inside `Input.Body` has parent path `Body[]` and parent type `CatalogItem`.
The root parent path is empty, and map values use `{}` in traversal paths. Parent
policies run after nested policies and inspect accumulated name/tag edits, so a
nested policy may already have changed `field.Name`. Match the original parent type
as well as the expected current field name to avoid touching an unrelated field.

This renames a field's exposed representation. It does not rename the Go type,
the `Contract` method, a business event value, or the MCP tool itself; tool names
are configured separately through `MCPRoutes`.

## Consumers and codecs

HTTP, MCP invocation and MCP/OpenAPI schema discovery consume the selected contract.
Discovery does not execute the target business handler. It does bind the policy's
state, including any explicitly requested auth component. Shared catalogs and linked
application types are not rewritten in place.

Use the component's configured encoder and decoder. Native Structology JSON and its
standard-compatible JSON wrappers support effective metadata. Custom codecs can
implement the shared `contract.JSONEncoder` / `contract.JSONDecoder` capabilities.
Where supported, existing codecs can use a reflected projection; method-bearing
embeddings require metadata-aware participation. Presence holders remain internal
and retain their original field identities.

Go's direct `encoding/json` functions do not understand enum or Contract tags.
Use the configured compatible codec path for those features. See
[enum JSON representations](enum-json.md) for the input/output annotations.

Complete custom JSON/Text methods own their representation. Internal edits of those
owners are unsupported; hide the enclosing field or adjust an ordinary wrapper.
Unchanged recursive types work; recursive edits are unsupported by the shared walker.
Dynamic output is JSON-only. See [feature boundaries](dynamic-contract-boundaries.md).

## Enum wire forms

Repeated `choice:"fixed" choice:"usage"` declarations use zero-based ordinals.
`choice:"0:fixed" choice:"10:usage"` supplies explicit ordinals. Both formats work.
`enumAccept:"literal"` controls input JSON/schema and `enumStyle:"ordinal"` controls
output JSON/schema independently. Each defaults from Go storage: integer means
ordinal, string means literal. Neither annotation accepts both wire forms. MCP checks the declared JSON form
before URL encoding; explicit empty literals and bare scalar JSON bodies are preserved.
Storage can differ from either wire type. Unknown cross-form conversions fail;
ordinary choices do not install implicit membership validation.

In the application example, the finance sends `{"pricingModel":"usage"}`, the handler
receives integer `10`, and output is `{"pricingModel":10}`. The editor sends and receives
`"subscription"` while the handler still uses integer `20`. Role selection changes both
choices and output style without changing `CatalogItem.PricingModel`'s Go type. Large explicit
integer ordinals retain their exact values through the shared Tagly enum plan.

## Cache and validation

Each component generation owns bounded caches, partitioned by registered method,
path, direction and original type. Policy names include nested owner paths. Policy
selection still runs for each request; only immutable metadata is cached. Empty
names disable variant retention. `NoCache: true` explicitly bypasses reuse even for a
named policy; it does not invalidate older entries or disable unrelated codec caches. Discovery and execution use their native invocation scopes. Metadata and codec
plans can be reused; authorization state and caller data are not retained in them.

Named variants let codecs reuse separate plans for each effective schema. A variant
must never share another variant's hidden fields, names or enum conversions.
Reusing a named policy reuses its immutable representation; current authorization
still runs for each invocation. Rebuilding equivalent unnamed representations does
not guarantee the same codec-cache identity.

## Policy cache key examples

A name identifies equivalent field edits, rather than a caller ID or token:

```go
policy := &contract.Policy{
    Name: fmt.Sprintf("catalog:v2:%s:commission=%t", role, canViewCommission),
    Field: rewrite,
}
```

The role and permission decisions are computed once before constructing the callback.
For an uncached selection, leave `Name` empty or use the explicit opt-in bypass:

```go
policy := &contract.Policy{Name: "catalog-live", NoCache: true, Field: rewrite}
```

Here `rewrite` is your field callback. Empty names and `NoCache: true` both prevent
Contract variant reuse. A bypass on any parent/nested policy bypasses the composed
variant. Named policies remain cached by default, preserving existing behavior.
Authorization is still resolved on each invocation. See the [operation example](contract-operation.md)
for the full cache behavior table and sparse-PATCH example.
