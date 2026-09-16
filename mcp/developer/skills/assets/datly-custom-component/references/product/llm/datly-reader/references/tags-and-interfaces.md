# Go shapes, tags and public interfaces

Examples use the Datly 1.0 SDK module github.com/viant/xdatly, normally imported as xdatly or handler. Use the SDK/version advertised by the project; do not substitute the older SDK solely because its name is similar.

## Component declaration

~~~~go
type Components struct {
    ReadRecords xdatly.Component[ReadInput, ReadOutput] `component:"ReadRecords,path=/v1/records,method=GET,connector=main,view=Records"`
}
~~~~

In real Go, put the tag on the same field declaration (see examples). Component options: name, path, method, connector, marshaller, handler, input, output, view, source. A named factory supplies custom/generated handler behavior; it is not selected from the HTTP verb alone.

Component reporting options: report, reportMCPTool, reportLinkedInputType, reportDimensions, reportMeasures, reportFilters, reportOrderBy, reportLimit, reportOffset; composition options reportCompose, reportComposeMCPTool, reportComposeMaxCubes, reportComposeMaxLimit, reportComposeTimeoutMs.

Separate tags include routeName, apiKeyHeader, apiKeyValue, mcp, desc and example. Resolve exact exposure syntax using the server's tag schema. Do not place live secrets in Go examples.

## Tag value grammar

A tag is Go struct-tag syntax: key:"value". Datly option-bearing values generally use name,key=value,key=value. Preserve quoting for commas inside values; let the generator format complex tags rather than hand-escaping layers incorrectly.

Tag names and Go field names are not interchangeable. Go type names are case-sensitive. Write canonical option keys; infer no field or column spelling variations. Aliases must be user-defined, and duplicate output names are errors.

## Binding and request sources

~~~~text
parameter:"ID,kind=path,in=id,required=true"
parameter:"Name,kind=query,in=name"
bind:"kind=input"
bind:"kind=dml"
bind:"kind=component,in=GET:/internal-records,required"
~~~~

parameter is the application declaration spelling; bind supplies scoped field binding. Both are parsed by the supported binding system, not a second application injector.

Common facets: name/leading name, kind, in, required, cacheable, uri and supported error-code/message metadata. Use the advertised binding schema for additional options. Request kinds include query, path, header, cookie, form, body and http_request; named providers cover constants/resources/views/components and services. Never assume a provider exists because a tag can be parsed.

A view dependency can use:
~~~~text
parameter:"Existing,kind=view,in=Existing"
view:"Existing,table=records,connector=main"
sql:"SELECT id,name FROM records"
~~~~

URI resources use sql:"uri=namespace:sql/records.sql" or view URI authoring. WithURI activation can also produce a dedicated route/tool suffix. mcpEnabled and pathMcpEnabled control corresponding parameter activation. Verify collisions and resulting exposure rather than inventing a suffix.

## View options

view:"Name,..." supports:

- name, type, dest, entityHooks, uri, connector, table
- cache, cacheWarmup, orderBy, limit, offset
- batch, batchConcurrency, relationalConcurrency, publishParent
- match=read_all|read_matched|read_derived
- allowNulls, groupable, auxiliary
- partitioner, concurrency (partition concurrency)
- selectorNamespace
- selectorProjection, selectorOrderBy, selectorCriteria
- selectorLimit, selectorOffset, selectorPage
- selectorFilterable, selectorSQLMethods, selectorOrderable
- selectorDefaultOrder, selectorDefaultLimit, selectorNoLimit
- selectorOrderByColumns

Use grouped/quoted lists for multiple selector paths/aliases; ask the formatter to emit correct Go quoting. Enabled projection/criteria/order flags still require allowed paths and methods.

querySelector:"view=Records" (or querySelector:"Records") attaches a supported Fields/OrderBy/Limit/Offset/Page/Criteria parameter to the view.

## SQL fields, visibility and codecs

| Tag | Purpose |
| --- | --- |
| sqlx:"column" | physical SQL mapping |
| sqlx:"column,primaryKey=true" | identity mapping; keep every composite part |
| sqlx autoincrement/generator metadata | declared ID/default behavior; reconcile with pre-validation policy |
| sqlx unique/reference metadata | DB checks through the native validator's supported schema |
| sqlx:"-" | non-SQL/transient field, such as a hook-built logical pseudo value |
| json:"publicName" | client field name |
| json:"-" | no JSON exposure |
| internal:"true" | internal backing field; still SQL/DML-mapped |
| sql:"SELECT ..." | authored query |
| sql:"uri=namespace:path" | SQL resource |
| source:"..." | source metadata where the authoring contract uses it |
| desc:"...", example:"..." | documentation |
| groupable:"true" | grouping capability |
| codec:"name,...args..." | registered typed conversion |
| validate:"..." | declared validation rules |
| invariant:"GroupName" | membership in a cohesive validation/backfill group |
| setMarker:"true" | internal presence marker holder |

Do not conflate internal with sqlx:"-". Do not expose Has or internal backing fields in MCP schemas merely because they are exported Go fields.

Codec options include name, body, outputType and ordered arguments. Codec names/configuration must come from the project's registry. Treat SQL NULL separately from a missing selected column.

## Relation grammar

~~~~text
on:"ID:parent.id=ParentID:child.parent_id,TenantID:parent.tenant_id=TenantID:child.tenant_id"
self:"child=ID,parent=ParentID"
~~~~

An on side is [GoField:] [namespace.]column. Multiple equalities are comma-separated. An optional (true)/(false) inclusion suffix is on the parent side only. Do not lose tuple order or equate same ID values across different tenant/key parts.

A self tag names child and parent fields for that holder. Multiple self holders can have different links. A DerivedView is a typed query-derived relation/output slot; it is not limited to one summary field.

## Presence and invariants

A typical marker is a parallel boolean shape with one flag per field, hidden from clients. Application requests contain business data, not Has. Generated setters mark explicit assignments, including zero/false/null. SyncPresence reconciles application changes with the captured original state; it does not infer suppliedness from an ID's value.

For sparse updates, Has=false means omitted and skips that field's required check. Has=true means validate the supplied value. New rows receive complete checks. Invariant backfill hydrates omitted members only when a group is affected, using actual PreviousFields, without marking those members.

Convenience methods include Get<Field>, Set<Field>, SyncPresence, Has<Group>Changes and Backfill<Group>IfNeeded. Preserve user-authored methods. Linked types cannot acquire methods from another Go package; the authoring system must still provide equivalent supported behavior.

## Custom handler interface

~~~~go
type Contract[I any, O any] interface {
    Exec(context.Context, handler.Session, *I, *O) error
}
~~~~

A factory returns an implementation for the component's declared I/O types. Use compile-time interface assertions. A custom handler owns its orchestration; a generated writer owns its declared lifecycle.

Session exposes Binder() and Response(). Binder supports Bind(ctx,target) error and Lookup(ctx,key) (value,found,error). Use canonical input and supplied capability keys; missing capability is an explicit error.

## Read and entity hooks

Current public reader interfaces:
~~~~go
type OnFetcher interface {
    OnFetch(context.Context) error
}
type OnRelationer interface {
    OnRelation(context.Context)
}
~~~~

OnFetch is per row; OnRelation sees complete relation assembly, not one batch. Use the server-advertised signature if the SDK evolves; do not silently write a different method signature.

Writer hooks:
~~~~go
type EntityHooks[T, P, O any] interface {
    Init(context.Context, *T, handler.LifecycleContext[T, P, O]) error
    Validate(context.Context, *T, handler.LifecycleContext[T, P, O]) error
}
~~~~

LifecycleContext embeds EntityState and supplies Previous, PreviousFields, Original, Parent and SelfParent. Its typed Output pointer is the invocation-owned component response; hooks may append violations, warnings and other response metadata even when they return an error. Previous is detached/read-only. Root roles use NoParent. SelfParent is the immediate recursive parent; Parent remains the enclosing relation parent.

Optional same-instance interfaces:
~~~~go
type AfterSequenceHook[T, P, O any] interface {
    AfterSequence(context.Context, *T, handler.LifecycleContext[T, P, O]) error
}
type AfterQueueHook[T, P, O any] interface {
    AfterQueue(context.Context, *T, handler.LifecycleContext[T, P, O]) error
}
~~~~

After validation, business values/markers stay fixed. AfterSequence can perform only permitted identity/link work. AfterQueue observes already queued work; mutation of queued values must be rejected.

Unified component completion:
~~~~go
type Finalizer[I, O any] interface {
    Finalize(context.Context, *I, *O, handler.Outcome) error
}
~~~~

A prepared root hook can implement it once per component. An explicit definition finalizer can override it and handle earlier failures; a shared definition finalizer must be concurrency-safe. Input/output may be absent on early failure. Cancellation does not mean completion should be skipped. Only Outcome.CommitConfirmed() permits commit-dependent publication.

Ordinary custom outputs also have their own supported Finalize(ctx) or Finalize(ctx,error) contracts. Do not confuse these with the generic writer's outcome-aware completion. InitMCP remains relevant; do not add deprecated sampling behavior.

## Focused capabilities

~~~~go
type DML interface {
    Insert(string, any) error
    Update(string, any) error
    Delete(string, any) error
    Execute(string, ...any) error
}
type Sequencer interface {
    Allocate(context.Context, string, any, string) error
}
type Flusher interface {
    Flush(context.Context, string) error
}
type Validator interface {
    Validate(context.Context, any, ...any) (*handler.Validation, error)
}
type TransactionStarter interface {
    Start(context.Context) error
}
~~~~

Data combines DML, Sequencer and Flusher. Keys include input, dml, sequencer, flusher, data, validator, logger, mbus, transactionStarter and selectors. Bind only capabilities the component needs.

Logger exposes Debug/Info/Warn/Error(message,args...). MessageBus exposes Message(destination,data,options...) and Push(ctx,message); use it only at an authorized lifecycle point.

The opt-in connector.Provider has Connector(ctx,name) (*sql.DB,error). The DB is borrowed, unknown names fail, and direct operations do not silently join managed Data transactions. This is a niche escape hatch, not the normal writer path.

## Controlled errors

handler.Validation contains Failed, Violations and an optional code. A violation identifies Location, Field, Message and Check. Preserve the structured list, not just a joined string. response.StatusCoder supplies StatusCode() int through ordinary error wrapping.

An explicit response.BodyError supplies ResponseBody() any. response.Error separates Code/Payload from private Cause. Payload controls message/error/violation shape, including explicit empty strings and null; the cause remains available for internal error handling but is not copied into the client body. A typed payload is preferred; a deliberate map is valid for a dynamic error object. Verify connected SDK support and wire behavior rather than assuming every runtime has the new error contract.

## JWT input and authorization predicates

Preserve original Datly's primary authorization pattern: configure
`JWTValidator`, verify the credential through `JwtClaim`, bind the resulting
claims as an explicitly declared component input, and let a typed authorization
predicate consume that input. No JWT variable or principal is implicitly added
to components that omit it. Decoding a token is not verification.

The original `JWTValidator` type is `github.com/viant/scy/auth/jwt/verifier.Config`.
It supports `CertURL` (the native certificate/JWKS endpoint) or RSA public-key
resources. Keep the project's trust configuration; do not invent a key source
from request data. The current Go configuration entry point is
`runtime/auth.New(ctx, &auth.Config{JWTValidator: verifierConfig})`; register the
returned factory through the application's existing codec configuration
(`bootstrap.ArtifactInput.CodecFactory` for Go bootstrap). This configures a
service, not ambient request claims. Certificate cache expiry and key rotation
must be verified in the connected native dependency; successful token checks
alone do not establish freshness. The inherited Scy cache-expiry correction is
tracked separately from the verified-input adapter.

With imports `jwt "github.com/viant/scy/auth/jwt"` and the application package's
registered predicate identity:

```go
type Input struct {
    JWT *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required,errorCode=401" codec:"JwtClaim" predicate:"handler,example.com/app/security.Authorization" json:"-"`
}

type Authorization struct {
    Input *Input `bind:"kind=input,required"`
}
```

The predicate reads `p.Input.JWT.UserID` and `p.Input.JWT.Subject`. `Subject` is
promoted from the native registered-claims type; `UserID` is a custom claim, so
its meaning and required presence must follow the application's issuer contract.
Keep JWT out of client response JSON. The input type and predicate reference must
use the actual application's package/type authority, not these example names.

A predicate can apply row authorization with parameterized criteria, using the
SDK's `github.com/viant/xdatly/predicate` package:

```go
func (p *Authorization) Compute(ctx context.Context, _ any) (*predicate.Criteria, error) {
    if err := ctx.Err(); err != nil {
        return nil, err
    }
    if p.Input == nil || p.Input.JWT == nil || p.Input.JWT.Subject == "" {
        return nil, fmt.Errorf("authorization required")
    }
    claims := p.Input.JWT
    return &predicate.Criteria{
        Expression: "owner_id = ? AND owner_subject = ?",
        Placeholders: []any{claims.UserID, claims.Subject},
    }, nil
}
```

Use the application's actual ownership columns and denial policy. If the policy
requires rejection rather than an empty authorized result, return its typed
error. Never interpolate claim strings into SQL. Missing/invalid/expired tokens
and predicate denial must stop protected SQL. Authorization lookups are distinct
from the protected business query.

Typed Go predicates use the explicit `kind=input` field above. Claims are available only through that declared input; unrelated parameters never acquire ambient JWT authority.

Acceptance should verify valid UserID/Subject filtering, wrong signature,
expiry, missing credential, denied business identity, and zero protected-table
reads on failure. Also verify that a component without JWT input has no implicit
JWT dependency. Certificate/public-key verification and the predicate execution
path are separate checks; both must pass before claiming the full mechanism.
