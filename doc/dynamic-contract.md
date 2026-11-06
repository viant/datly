# Dynamic contracts

**Expose the right fields and values to each caller—from one Go model.**

Dynamic contracts let an API adapt to a caller's permissions and the operation
being performed. You write a field policy once. Datly uses it for both the
MCP/OpenAPI schema and the configured JSON encoder/decoder.

A field hidden by the policy is absent from that caller's schema and response.
A pricing value described as a string is handled as a string on the wire—even
when your Go model stores its numeric code.

## What you can do

- **Keep commission private.** Finance users see `"commission": 0.15`. Other
  callers do not see that field in the response or in their API schema.
- **Accept names instead of codes.** A client sends `"pricingModel": "usage"`.
  Your handler receives integer `10`.
- **Choose response formatting separately.** The handler returns integer `10`.
  You can return `"pricingModel": "usage"` or `"pricingModel": 10` to the client.
- **Offer choices by role.** Editors see `fixed` and `subscription`; finance
  users also see `usage`. Stored codes stay the same for every role.
- **Make PATCH sparse.** Keep `id` required, but allow a request such as
  `{"id":"r-1","commission":0.12}` without resending the title or pricing model.

## Why it matters

Keep your existing Go models and business handlers. Avoid creating another DTO,
schema builder and JSON adapter for every role or operation. The API that clients
discover and the JSON that the configured codecs read/write follow the same rules.

This controls field exposure and representation. Your existing authorization and
validation still decide which records a caller may access and which changes are
allowed. The PATCH policy changes the schema; the writer still owns update behavior.

## How it works

A model supplies a `Contract` hook:

```go
Contract(context.Context, bindly.Binder) (*contract.Policy, error)
```

1. **Read the context.** Bind verified claims, your auth-context component, the
   target operation, or other state needed to make the decision.
2. **Return field rules.** Hide a field, change its JSON name, select enum choices,
   or adjust requiredness. Return `nil, nil` when nothing needs to change.
3. **Use one selected contract.** Datly applies those rules to schema generation
   and JSON handling. Nested types can supply their own rules; parents have final
   authority.

Datly changes field metadata, not your model's storage layout. Handlers keep using
the same Go fields and types. Unchanged parts retain their original linked types.
Named policies can reuse the selected metadata and codec plans, while authorization
is still resolved for each invocation.

The hook uses `github.com/viant/bindly` and `github.com/viant/xdatly/contract`.
Schema discovery runs the hook and any explicitly bound dependencies, such as an
auth component; it does not run the target business handler.

## Choose a focused example

The snippets below are independent use cases, not one large combined policy.
Callback snippets belong in `Policy.Field`; method-body snippets belong inside your
`Contract(ctx, binder)` method. The snippets use `context`, `fmt` and `reflect` as
needed, alongside the two public packages above.

- [Hide commission](#use-case-hide-commission)
- [Include metrics only when requested](#use-case-include-metrics-only-when-requested)
- [Accept enum input](#use-case-accept-enum-input)
- [Format enum output](#use-case-format-enum-output)
- [Narrow input choices by role](#use-case-narrow-input-choices-by-role)
- [Sparse PATCH with primary keys](contract-operation.md)
- [All Field methods and schema fragments](contract-field-examples.md)

## Use case: hide commission

**When:** a response contains a commission field that some callers must not see.
Attach the hook to the output type for a response-only rule. Reusing the hooked type
in an input graph applies the rule there too.

```go
type CatalogOutput struct {
    Title      string  `json:"title"`
    Commission float64 `json:"commission"`
}
```

First bind your existing authorization component. Its result determines whether the
field is visible; the component itself owns verification:

```go
var state struct {
    Auth *Authorization `parameter:",kind=component,in=GET:/auth/context,required=true"`
}
if err := binder.Bind(ctx, &state); err != nil {
    return nil, err
}
if state.Auth == nil {
    return nil, fmt.Errorf("authorization context required")
}
```

Here `Authorization` is your application's result type and has a
`CanViewCommission bool` permission. In this method, select the policy once:

```go
if state.Auth.CanViewCommission {
    return nil, nil
}
return &contract.Policy{
    Name: "catalog:commission:hidden:r1",
    Field: func(_ string, parent reflect.Type, field *contract.Field) (*contract.Field, error) {
        if parent == reflect.TypeFor[CatalogOutput]() && field.Name == "Commission" {
            return field.Hide(), nil
        }
        return nil, nil
    },
}, nil
```

The same handler result yields different JSON responses:

```json
{"title":"Example","commission":0.15}
```

```json
{"title":"Example"}
```

The second caller's schema has no `commission` property. The original Go value still
contains `Commission`; the policy does not clear business data. A hidden field needs
no public description.

## Use case: include metrics only when requested

**When:** ordinary responses should contain only business data, but a caller can
request the existing metrics output with `X-Include-Metrics: true`.

Add the native metrics binding to your existing `Output` type:

```go
Metrics response.Metrics `parameter:",kind=output,in=metrics" json:"metrics,omitempty"`
```

Import `github.com/viant/xdatly/handler/response` as `response`.

The equivalent DQL declaration is:

```sql
#set($_ = $Metrics<?>(output/metrics).WithTag('json:"metrics,omitempty"'))
```

Put the Contract hook on `Output`. In its method body, bind the optional header:

```go
var state struct {
    IncludeMetrics bool `parameter:",kind=header,in=X-Include-Metrics"`
}
if err := binder.Bind(ctx, &state); err != nil {
    return nil, err
}
if state.IncludeMetrics {
    return nil, nil // Keep the declared metrics field visible.
}
```

When the header is absent or false, return a policy that hides only `Output.Metrics`:

```go
return &contract.Policy{
    Name: "output:metrics:hidden:r1",
    Field: func(_ string, parent reflect.Type, field *contract.Field) (*contract.Field, error) {
        if parent == reflect.TypeFor[Output]() && field.Name == "Metrics" {
            return field.Hide(), nil
        }
        return nil, nil
    },
}, nil
```

The header changes field exposure, not the `output/metrics` binding. Datly still
owns populating the metric values. Use a caller-provided header only as a presentation
preference; if metrics require a permission, check that permission before returning
the visible contract.

| Request header | Response and schema |
|---|---|
| Absent | No `metrics` property |
| `X-Include-Metrics: false` | No `metrics` property |
| `X-Include-Metrics: true` | The schema exposes `metrics`; JSON includes it when metrics are available |

Without the header, an example response is:

```json
{"data":[{"id":1}]}
```

With the header, the same response can include populated native metrics:

```json
{"data":[{"id":1}],"metrics":[{"view":"records","rows":1,"elapsedMs":2}]}
```

The metric values above are illustrative. `omitempty` still omits an empty metrics
collection; the header does not manufacture data or turn metric capture on. The
hidden policy has a stable cache name, while each request still reads its own header.
A request with the header set to true returns the original linked contract, so a
previous hidden selection cannot suppress metrics for that caller.

Send the same header when fetching caller-specific OpenAPI or MCP discovery if you
want the described schema to include `metrics`. The hook controls the JSON body
field; diagnostic response headers, logging, tracing and SQL disclosure keep their
existing configuration and authorization rules.

For this example, use your own `X-Include-Metrics` header; it is not an alias for the built-in diagnostic metrics header.

## Use case: accept enum input

**When:** request JSON should use one representation while Go storage uses another.
`enumAccept` controls input only. Define stable codes with explicit choices:

```go
type PricingInput struct {
    PricingModel int `json:"pricingModel" choice:"0:fixed" choice:"10:usage" choice:"20:subscription" enumAccept:"literal"`
}
```

The request is a JSON string, and the handler receives an integer:

```json
{"pricingModel":"usage"}
```

```go
input.PricingModel == 10
```

Different input settings give these results, using the same choice mapping:

| Go field type | `enumAccept` | Accepted request JSON | Go value after decoding | Input schema type |
|---|---|---|---|---|
| `int` | omitted or `ordinal` | `{"pricingModel":10}` | `10` | integer |
| `int` | `literal` | `{"pricingModel":"usage"}` | `10` | string |
| `string` | omitted or `literal` | `{"pricingModel":"usage"}` | `"usage"` | string |
| `string` | `ordinal` | `{"pricingModel":10}` | `"usage"` | integer |

To select the input form dynamically, the relevant field callback is small:

```go
if parentType == reflect.TypeFor[PricingInput]() && field.Name == "PricingModel" {
    return field.SetTag("enumAccept", "literal"), nil
}
return nil, nil
```

`PricingInput` is the hooked input/domain type containing that field. Use a policy
name that identifies this input decision. The selected JSON form is exclusive:
`literal` does not also accept numeric JSON just because the Go field is an integer.

## Use case: format enum output

**When:** the response should use a different representation from the stored Go value.
`enumStyle` controls output only; it does not change what requests accept.

```go
type PricingOutput struct {
    PricingModel int `json:"pricingModel" choice:"0:fixed" choice:"10:usage" choice:"20:subscription" enumStyle:"literal"`
}
```

A handler result with `PricingModel == 10` is encoded as:

```json
{"pricingModel":"usage"}
```

Different output settings give these results:

| Go field type and value | `enumStyle` | Response JSON | Output schema type |
|---|---|---|---|
| `int`, `10` | omitted or `ordinal` | `{"pricingModel":10}` | integer |
| `int`, `10` | `literal` | `{"pricingModel":"usage"}` | string |
| `string`, `"usage"` | omitted or `literal` | `{"pricingModel":"usage"}` | string |
| `string`, `"usage"` | `ordinal` | `{"pricingModel":10}` | integer |

The output-only field callback is:

```go
if parentType == reflect.TypeFor[PricingOutput]() && field.Name == "PricingModel" {
    return field.SetTag("enumStyle", "literal"), nil
}
return nil, nil
```

`PricingOutput` is the hooked response/domain type containing that field. Input and
output are independent: `enumAccept:"literal" enumStyle:"ordinal"` accepts
`{"pricingModel":"usage"}`, keeps Go integer `10`, and emits `{"pricingModel":10}`.
MCP/OpenAPI describe the corresponding input and output types separately.

## Use case: narrow input choices by role

**When:** callers should be offered different pricing choices. This example changes
only the input domain; it does not hide commission or change response formatting.
Use the same native auth binding shown above, with `Authorization.Role` supplied
as a verified string value by your existing auth component.

Select the choices once in the Contract method:

```go
var choices []string
switch state.Auth.Role {
case "editor":
    choices = []string{"0:fixed", "20:subscription"}
case "finance":
    choices = []string{"0:fixed", "10:usage", "20:subscription"}
default:
    return nil, fmt.Errorf("unsupported role %q", state.Auth.Role)
}
```

Return a named policy over the input type. The callback uses those decisions:

```go
return &contract.Policy{
    Name: "pricing-input:r1:" + state.Auth.Role,
    Field: func(_ string, parent reflect.Type, field *contract.Field) (*contract.Field, error) {
        if parent == reflect.TypeFor[PricingInput]() && field.Name == "PricingModel" {
            return field.SetEnum(choices...).SetTag("enumAccept", "literal"), nil
        }
        return nil, nil
    },
}, nil
```

| Role | Advertised input choices | Request example | Decoded integer |
|---|---|---|---|
| editor | fixed, subscription | `{"pricingModel":"subscription"}` | `20` |
| finance | fixed, usage, subscription | `{"pricingModel":"usage"}` | `10` |

`SetEnum` replaces the original choice group. Explicit codes ensure subscription
remains `20` when usage is omitted. Enforce allowed operations in the business
validation path as well; enum metadata is not an authorization mechanism.

Keep a complete choice mapping on a separate output type when responses must still
represent every stored pricing model. If a narrowed mapping is also used for output,
encoding a stored value missing from that mapping fails when conversion is needed.

## More Contract documentation

| Read next | Covers |
|---|---|
| [POST/PATCH Contract example](contract-operation.md) | Direct `IsPK()` / `Optional()` rule, pricing/commission schema results, and cache keys |
| [Field methods and schema output](contract-field-examples.md) | Each kind of field edit, descriptions/examples, enum forms, and renaming |
| [Field API reference below](#every-field-method) | All helper methods and copy-on-write behavior |
| [Policy cache keys below](#policy-cache-key-examples) | Stable names, empty names and explicit `NoCache` |
| [Enum reference below](#enum-wire-forms) | Choice syntax and accepted/emitted JSON forms |
| [Codec boundaries below](#consumers-and-codecs) | Configured codecs, custom JSON methods and supported formats |
| [MCP query selectors](mcp-query-selectors.md) | Root and sub-view selectors and their canonical HTTP bindings |

## DQL and named operations

`MCPRoutes` is optional. Existing tool activation and default naming work without it:

```sql
/* {
  "URI": "/catalog/items",
  "Method": "PATCH",
  "MCPTool": true
} */
```

The tool uses the route's top-level `Name` when supplied, otherwise the view name
(with the normal name cleanup). Add `"Name": "CatalogItems"` to choose that name
explicitly. Omitting both `MCPTool` and `MCPRoutes` does not automatically expose a
component. Existing internal-route and per-parameter MCP exposure controls still apply.

Use `MCPRoutes` to name specific declared method/path targets separately. A nonempty
mapping enables those mapped tools without requiring a separate `MCPTool: true`.
If several methods would share one default tool name, assign distinct names here:


```sql
/* {
  "URI": "/catalog/items",
  "Method": "POST,PATCH",
  "MCPRoutes": {
    "POST:/catalog/items": {"Name": "CreateCatalogItem", "Description": "Create catalog items"},
    "PATCH:/catalog/items": {"Name": "PatchCatalogItem", "Description": "Patch catalog items"}
  }
} */
```

Keep the component's existing input/output/handler declarations and executable DQL.
The fragment above is route configuration, not a complete writer implementation.
Only declared method/path targets can be mapped. Tool names must be distinct.
Mappings do not invent HTTP methods, change writer insert/update policy, or derive
operation authority from client arguments. V1 has its native `$mcp_route` directive;
the Go `Contract` source above is unchanged between versions.

For PATCH, `Optional()` affects exposed requiredness. Entity `validate` tags and
native presence-aware insert/update validation remain owned by the existing writer.
All schema fields are optional unless explicitly marked required. Optional and
nullable are independent; a scalar does not become nullable merely by being optional.
OpenAPI path parameters retain the specification's required-presence rule.

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

Helpers return `*Field` for copy-on-write chaining. A private `cloned` flag gates
copying: the first actual edit obtains a writable copy when needed; later helpers
reuse it. An explicitly writable clone needs no additional copy, and a
no-op helper retains its existing pointer. Datly supplies a read-only snapshot
to each callback, so comparing its original tags with the returned tags is safe. Unrelated tags are preserved.

Always use the returned value: `field = field.Optional()` or return the chain.
Use `Clone()` before assigning `Name`/`Tag` directly, or to start an independent
branch from an edited field. Writable results belong to one edit chain; the flag
is not a general alias tracker or a concurrency primitive. Helper errors are recorded on the field; Datly checks
them before publishing a variant. Returning `nil, nil` discards that callback's edit
and continues traversal. Returning a non-nil error aborts contract resolution.

| Method | Behavior |
|---|---|
| `Clone() *Field` | Always creates an independent writable copy of the name, tags, error and traversal flag, even when the source is already a clone. Use it for direct assignments or independent edit branches. |
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

For a matched `PricingInput.PricingModel` field using literal input, for example:

```go
return field.
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
alone does not override an explicit `json` tag: the pricing field has `json:"pricingModel"`,
so changing only `Name` would leave its JSON key as `pricingModel`.

To expose that field as `billingModel` in JSON/OpenAPI and `BillingModel` in MCP, a matching
callback can return:

```go
case "PricingModel":
    rewritten := field.Clone()
    rewritten.Name = "BillingModel" // effective metadata name; must be a Go identifier
    return rewritten.
        SetTag("json", "billingModel,omitempty").
        SetTag("mcp", "name=BillingModel,aliases=billingModel"), nil
```

With the literal input and literal output types shown above, JSON input and output
use `{"billingModel":"usage"}`. This edit changes the property name only; the enum
settings remain those of the selected input/output contract. MCP advertises `BillingModel` and normalizes its authored alias `billingModel`
to the JSON key. The handler still reads/writes **the original `PricingModel` Go field**, with type
**`int`**; it does not gain a `BillingModel` Go member. Presence flags likewise
continue to use the canonical field identity. Avoid exposed-name/alias collisions.
If the rename is conditional on role, include that decision in `Policy.Name` too.

The callback's `parentPath` is the canonical parent path, excluding the current
field; `parentType` is the original linked parent type. For example, a `PricingInput`
member inside a body slice has parent path `Body[]` and parent type `PricingInput`.
The root parent path is empty, and map values use `{}` in traversal paths. Parent
policies run after nested policies and inspect accumulated name/tag edits, so a
nested policy may already have changed `field.Name`. Match the original parent type
as well as the expected current field name to avoid touching an unrelated field.

This renames a field's exposed representation. It does not rename the Go type,
the `Contract` method, a business event value, or the MCP tool itself; tool names
are configured separately through `MCPRoutes`.

## Consumers and codecs

HTTP binding, JSON output, MCP discovery/invocation, OpenAPI and view metadata use
request-local metadata. Registered components, shared schemas and views are not
rewritten. OpenAPI/view metadata responses use `Cache-Control: private, no-store`.
Dynamic MCP registrations contain only a conservative schema shell; caller discovery
uses `Registry.ListToolsFor(ctx)` and never falls back to the static inventory on errors. View metadata filters hidden columns/relations and includes an
`EffectiveSchema` when the view contract changes. Sub-view query selectors keep
master's existing canonical HTTP binding behavior; see [MCP selectors](mcp-query-selectors.md).

The default `content.StructologyJSONRuntime` retains native format settings,
exclusions and filters. `content.StandardJSONRuntime` uses standard JSON names,
omission and time semantics; native case/date/global-omitempty transforms do not
apply. It maintains native presence holders through opt-in standard decoder tracking.
Both engines keep linked decoder destinations and encoder values.

Configured engines/codecs can implement shared `contract.JSONEncoder` and
`contract.JSONDecoder` capabilities. Unsupported configured codecs, legacy JSON
interceptors and non-JSON dynamic formats fail explicitly. Raw handler responses
cannot apply dynamic output metadata. Complete custom JSON/Text methods own their
representation: edits inside those owners are unsupported, while hiding the whole
field or changing an ordinary wrapper is supported. Unchanged recursive types work;
recursive edits remain unsupported by the shared walker.

## Enum wire forms

Repeated `choice:"fixed" choice:"usage"` declarations use zero-based ordinals.
`choice:"0:fixed" choice:"10:usage"` supplies explicit ordinals. Both formats work.
`enumAccept:"literal"` controls input JSON/schema and `enumStyle:"ordinal"` controls
output JSON/schema independently. Each defaults from Go storage: integer means
ordinal, string means literal. Neither annotation accepts both wire forms. MCP checks the declared JSON form
before URL encoding; explicit empty literals and bare scalar JSON bodies are preserved.
Storage can differ from either wire type. Unknown cross-form conversions fail;
ordinary choices do not install implicit membership validation.

The input and output use cases above show these settings independently. The
configured codec and the published schema use the same selected annotations. Large
explicit integer ordinals retain their exact values through the shared Tagly enum plan.

## Cache and validation

Each component generation owns bounded caches, partitioned by registered method,
path, direction and original type. Policy names include nested owner paths. Policy
selection still runs for each request; only immutable metadata is cached. Empty
names disable variant retention. `NoCache: true` explicitly bypasses reuse even for a
named policy; it does not invalidate older entries or disable unrelated codec caches. An MCP invocation shares its selected policy with
its routed HTTP handler within that invocation, without caching authentication.

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
