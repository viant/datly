# POST and sparse PATCH schemas

Use a Contract hook when a shared type has explicit required fields for POST but
should publish a sparse PATCH schema. `Field.IsPK()` recognizes explicit SQLX key
metadata; `Optional()` removes other required flags. No operation utility is needed.

The native writer already handles supplied-field/presence-aware updates according
to its write policy. This hook adjusts exposed schema requiredness. It does not
change the writer's mutation policy, authorization or entity `validate` rules.

## A neutral record with pricing and commission

```go
package record

import (
    "context"
    "net/http"
    "reflect"

    "github.com/viant/bindly"
    "github.com/viant/xdatly/contract"
)

type Record struct {
    ID string `json:"id" sqlx:"ID,primaryKey" required:"true" description:"Record identifier"`
    Title string `json:"title" required:"true" description:"Display title"`
    PricingModel int `json:"pricingModel" required:"true" choice:"0:fixed" choice:"10:usage" choice:"20:subscription" description:"Pricing model"`
    Commission float64 `json:"commission" required:"true" description:"Commission rate"`
    Has *struct{ ID, Title, PricingModel, Commission bool } `json:"-" setMarker:"true"`
}

func (*Record) Contract(ctx context.Context, binder bindly.Binder) (*contract.Policy, error) {
    var state struct {
        Method string `parameter:",kind=contract,in=method,required=true"`
    }
    if err := binder.Bind(ctx, &state); err != nil {
        return nil, err
    }
    if state.Method != http.MethodPatch {
        return nil, nil // POST and other methods retain the linked contract.
    }
    return &contract.Policy{
        Name: "record:patch:v1", // equivalent edits reuse this named variant
        Field: func(_ string, _ reflect.Type, field *contract.Field) (*contract.Field, error) {
            if field.IsPK() {
                return nil, nil // preserve the key's authored requiredness
            }
            adjusted := field.Clone().Optional()
            if err := adjusted.Err(); err != nil {
                return nil, err
            }
            if adjusted.Tag == field.Tag {
                return nil, nil
            }
            return adjusted, nil
        },
    }, nil
}

type Input struct {
    Body *Record `parameter:",kind=body,required=true"`
}
```

The method belongs to `Record`, so the policy applies to its subtree. The outer
`Input.Body` requirement remains required. The same hook is delegated automatically
inside slices, arrays, maps and embedded shapes. Parent policies run last and can
override nested decisions. Presence holders remain internal; the writer uses them
to distinguish an omitted field from an explicit zero or empty value.

POST request: `{"id":"r-1","title":"Example","pricingModel":0,"commission":0.1}`.
PATCH request: `{"id":"r-1","commission":0.12}`.

## OpenAPI result

These request-body fragments are inlined for readability; the host may use `$ref`.

```yaml
# POST /records
requestBody:
  required: true
  content:
    application/json:
      schema:
        type: object
        required: [id, title, pricingModel, commission]
        properties:
          id: {type: string, description: Record identifier}
          title: {type: string, description: Display title}
          pricingModel: {type: integer, enum: [0, 10, 20], description: Pricing model}
          commission: {type: number, format: double, description: Commission rate}
```

```yaml
# PATCH /records
requestBody:
  required: true
  content:
    application/json:
      schema:
        type: object
        required: [id]
        properties:
          id: {type: string, description: Record identifier}
          title: {type: string, description: Display title}
          pricingModel: {type: integer, enum: [0, 10, 20], description: Pricing model}
          commission: {type: number, format: double, description: Commission rate}
```

MCP publishes the same change in required properties. Optional fields remain in the
schema; they are not hidden, made nullable, or automatically given `omitempty`.
`Has` is not exposed. Runtime validation remains with the existing writer.

## What IsPK recognizes

- `sqlx:"ID,primaryKey"` and `sqlx:"ID,primaryKey=true"` identify a primary key.
- `autoincrement` follows SQLX's key convention.
- Every explicitly tagged member of a composite key is retained.
- `primaryKey=false` does not identify a key. A field named `ID` alone does not
  imply a key. The predicate does not mutate the field.
- Keys retain authored requiredness. An optional generated ID stays optional;
  add `required:"true"` where the client must supply an identity for updates.

`HasTag("pk")` would check a different, standalone annotation and would miss the
SQLX option. No extra HasTag API is needed: use `_, exists := field.Tag.Lookup("sqlx")`
for plain tag presence, and `field.IsPK()` for key semantics. The ordinary IsPK path
scans the tag without allocating; quoted option values are not mistaken for flags.

## Bind the correct method

The Contract binder's `kind=contract` supplies the declared target operation. An
OpenAPI fetch may use GET while describing PATCH, and MCP discovery may have no
HTTP request for the target. Existing `http_request` selectors still work and keep
their provider/request meaning. If your hook already bound the actual target method
alongside auth state, reuse that value instead of binding it again. Path is not
needed for this rule. These snippets use the same public API in v0 and v1.

## Named cache keys and explicit bypass

`Name: "record:patch:v1"` is safe above because the edits are fixed. Hosts also
partition caches by component generation, registered route, direction and linked
type. Authorization still runs on every request; the cache stores immutable metadata,
not the hook receiver, bound state, field callback or business data.

For role-dependent edits, include every decision affecting the schema:

```go
policy := &contract.Policy{
    Name: fmt.Sprintf("catalog:v2:%s:commission=%t", role, canViewCommission),
    Field: rewrite, // callback using those already-computed decisions
}
```

`role`, `canViewCommission` and `rewrite` come from your hook. Include `fmt` if using
this key construction. Do not reuse one name for different edits within the same
host namespace. Include a policy revision or reload the component when rules change.

| Name | NoCache | Contract variant reuse |
|---|---|---|
| empty | false | Disabled |
| empty | true | Disabled |
| non-empty | false | Enabled; Name participates in the key |
| non-empty | true | Disabled for this selection |

```go
// Keep a descriptive name but opt out of named-variant reuse.
policy := &contract.Policy{Name: "catalog-live", NoCache: true, Field: rewrite}
```

`NoCache` is an opt-in bypass, not an invalidation command: an earlier cached entry
with that name is not overwritten or removed. A bypass on any composed policy also
bypasses the composed variant cache. Empty names retain their existing no-cache
meaning. This flag does not disable unrelated codec/type caches. Newly rebuilt
representations have distinct codec-cache identities, so there is no stale policy
reuse through those caches.
