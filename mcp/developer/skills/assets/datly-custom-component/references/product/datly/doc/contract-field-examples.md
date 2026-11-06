# Field adjustments and their schema output

These examples use a neutral catalog item with a pricing model and commission.
They describe schema changes; they do not replace permission checks or change the
writer's insert/update policy. The Go fields and their storage types remain linked.

```go
type CatalogItem struct {
    ID string `json:"id" sqlx:"ID,primaryKey" required:"true"`
    Title string `json:"title" required:"true"`
    PricingModel int `json:"pricingModel" choice:"0:fixed" choice:"10:usage" choice:"20:subscription" enumAccept:"literal" enumStyle:"ordinal"`
    Commission float64 `json:"commission" required:"true"`
}
```

The initial OpenAPI **input** schema is equivalent to this fragment. Named `$ref`
components and optional host annotations are omitted for readability:

```yaml
type: object
required: [id, title, commission]
properties:
  id: {type: string}
  title: {type: string}
  pricingModel: {type: string, enum: [fixed, usage, subscription]}
  commission: {type: number, format: double}
```

Each example below starts from that original contract independently. Return the
adjusted field from the callback; the host validates it before publishing/caching.

## Visibility and requiredness

`field.Hide()` on `Commission` sets JSON/internal/MCP exclusion tags. Both
its property and its required-list entry disappear; the Go `Commission` member
still exists. Input cannot populate it through the effective JSON decoder.

```yaml
# After Hide() on Commission
required: [id, title]
properties:
  id: {type: string}
  title: {type: string}
  pricingModel: {type: string, enum: [fixed, usage, subscription]}
```

`field.Required()` on `PricingModel` adds its existing property name to the
required list. `Optional()` on `Title` removes that requirement. Neither changes
property types or makes a non-pointer value nullable.

```yaml
# Required() on PricingModel; other fields unchanged
required: [id, title, pricingModel, commission]
```

```yaml
# Optional() on Title; the title property is still present
required: [id, commission]
properties:
  title: {type: string}
  # id, pricingModel and commission unchanged
```

`Optional()` also removes inline `required` options from existing `parameter` and
`bind` tags. It preserves entity `validate` tags and does not add `omitempty`.
For all-field PATCH behavior with key preservation, use the
[direct PATCH example](contract-operation.md).

## Enum choices and representation

On `PricingModel`, this chain narrows the advertised choices while keeping their
stored codes stable:

```go
return field.SetEnum("0:fixed", "20:subscription"), nil
```

```yaml
# Input: enumAccept remains literal
pricingModel: {type: string, enum: [fixed, subscription]}
# Output: enumStyle remains ordinal
pricingModel: {type: integer, enum: [0, 20]}
```

`SetTag("enumStyle", "literal")` changes the output property to a string enum;
`SetTag("enumAccept", "ordinal")` changes the input property to an integer enum.
Both are independent of the Go field's `int` storage.

```yaml
# After both changes, keeping all original choices
# Input JSON: {"pricingModel": 10}
pricingModel: {type: integer, enum: [0, 10, 20]}
# Output JSON: {"pricingModel": "usage"}
pricingModel: {type: string, enum: [fixed, usage, subscription]}
```

`SetEnum("fixed", "usage", "subscription")` instead assigns codes 0, 1, and 2.
Use explicit codes when filtering an existing domain. Cross-form conversion fails
for values missing from its mapping; ordinary choices do not imply automatic
business membership validation.

## Description and example

```go
return field.
    SetDescription("Fraction of the amount retained as commission").
    SetExample("0.15"), nil
```

```yaml
commission:
  type: number
  format: double
  description: Fraction of the amount retained as commission
  example: 0.15
```

Both methods replace/add their respective tags and retain other annotations.
Use an example compatible with the selected wire type. MCP exposes the corresponding
annotation as `examples: [0.15]` rather than OpenAPI 3's `example: 0.15`.

## Name, JSON name and MCP name

`Name` is an editable member, not a method. For a matched `PricingModel` field:

```go
renamed := field.Clone()
renamed.Name = "BillingModel"
return renamed.SetTag("json", "billingModel").
    SetTag("mcp", "name=BillingModel,aliases=billingModel"), nil
```

```yaml
# OpenAPI input property (was pricingModel)
billingModel: {type: string, enum: [fixed, usage, subscription]}
```

MCP advertises `BillingModel`; its `billingModel` alias maps to the JSON key.
The application continues to use `CatalogItem.PricingModel`. Changing `Name` alone
would leave the explicit `json:"pricingModel"` key intact. A changed property name
also changes the corresponding required-list entry when that field is required.
This does not rename a business event value or the MCP tool/HTTP route.

## Generic tag replacement and removal

`SetTag(key, value)` replaces every occurrence of that key with one value or adds
it when absent. For example, `SetTag("description", "Commission rate")` produces
`description: Commission rate`. Use `SetEnum` for a repeated `choice` group.

`RemoveTag("description")` removes the description from the published property,
while leaving its type and requiredness intact. To remove enum conversion and the
domain together, remove all three related tags:

```go
return field.RemoveTag("choice").
    RemoveTag("enumAccept").RemoveTag("enumStyle"), nil
```

```yaml
# PricingModel falls back to its Go int wire type, without an enum
pricingModel: {type: integer}
```

Removing only `choice` while retaining an enum conversion annotation is invalid:
that conversion would have no ordinal/literal mapping. `SetTag` does not retarget
native parameter locations, install a codec, or guarantee that every arbitrary tag
has a schema meaning; each consumer interprets its supported annotations.

## Copying, traversal and validation methods

| Method | Schema effect |
|---|---|
| `Clone()` | None by itself. It makes an independent writable copy of metadata/error/traversal state. Helpers already clone when needed; use Clone for direct assignments or independent branches. |
| `SkipChildren()` | None by itself. Returning it skips descendants for this policy. Other policies, including nested owners, still apply. |
| `IsPK()` | None. Tests SQLX key semantics; use it to preserve keys when making PATCH fields optional. `Tag.Lookup("sqlx")` only tests tag presence. |
| `ChildrenSkipped()` | None. Reports the traversal flag. |
| `Err()` | None. Reports a recorded helper error. An error prevents publishing the proposed variant. |
| `Validate(originalType)` | None. Datly checks the name, tags and choices against the original type; invalid metadata fails resolution. |

For a linked `Details` field with no other policies, returning
`field.SkipChildren()` leaves its original schema unchanged:

```yaml
details:
  type: object
  properties:
    label: {type: string}
```

`Hide().SkipChildren()` hides `Details` and avoids this policy's descendant work.
It does not bypass a nested type's own Contract hook. A nil return means no edit
and normal descent; skip-only or equivalent edits preserve linked identity.
