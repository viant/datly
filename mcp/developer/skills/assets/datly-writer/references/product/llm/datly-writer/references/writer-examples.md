# Writer authoring examples

Select mutation behavior explicitly; the HTTP verb alone is not the generated policy.

## PATCH intent and linked types

~~~~sql
#package('example.com/app/records')
#import('model', 'example.com/app/model')
#import('hooks', 'example.com/app/recordhooks')
#setting($_ = $route('/v1/records', 'PATCH'))
#setting($_ = $connector('main'))
#define($_ = $TenantID<int>(query/tenantId).Required())
#define($_ = $Body<[]*model.Record>(body/))
#define($_ = $Existing<[]*model.Record>(view/Existing) /*
 SELECT id, tenant_id, name FROM records WHERE tenant_id=:TenantID
*/)
#define($_ = $Data<[]*model.Record>(output/view))
SELECT r.*, entity_hooks(r, 'hooks.RecordHooks')
FROM records r
~~~~

This illustrates intent grammar, not a deployment-ready authorization policy. Select generated PATCH mode and bind Body/Existing/Data through the developer server. A production Previous read must completely cover requested original identities without accidental pagination/truncation. Generate a safe original-key restriction with supported typed helpers. A tenant-only read is appropriate only when deliberately loading the complete bounded fixture/tenant set.

Auxiliary data remains readable but is not mutated:

~~~~sql
SELECT r.*, l.*
FROM records r
JOIN (lookup_values) l ON l.id=r.lookup_id
~~~~

## Go-shape declaration and marker

~~~~go
type Components struct {
    Patch xdatly.Component[Input, Output] `component:"Patch,path=/v1/records,method=PATCH,connector=main,handler=NewRecordPatch,view=Records"`
}
type Record struct {
    ID   *int64 `json:"id,omitempty" sqlx:"id,primaryKey"`
    Name string `json:"name" sqlx:"name" validate:"required"`
    Has  *RecordHas `json:"-" sqlx:"-" setMarker:"true"`
}
type RecordHas struct { ID, Name bool }
~~~~

Clients send business data, not Has. Omitted Name is skipped in sparse required checks and is not cleared. Supplied empty Name fails if the application's required rule forbids empty text. Do not equate every DB NOT NULL column with a nonzero business requirement.

## Cohesive invariant fields

~~~~go
type Window struct {
    Start *int64 `json:"start" invariant:"Range"`
    End   *int64 `json:"end" invariant:"Range"`
    Has   *WindowHas `json:"-" sqlx:"-" setMarker:"true"`
}
type WindowHas struct { Start, End bool }
~~~~

For an existing row with only End supplied, backfill Start from loaded Previous without setting its marker, then validate the group. New rows receive complete checks. Unknown/unselected previous values are not known zeros; failed backfill must not partially mutate the group.

## Hook responsibilities

~~~~go
func (h *RecordHooks) Init(ctx context.Context, row *model.Record,
    state handler.EntityState[model.Record, handler.NoParent]) error {
    // Application defaults/normalization, using marker-aware setters.
    return nil
}
func (h *RecordHooks) Validate(ctx context.Context, row *model.Record,
    state handler.EntityState[model.Record, handler.NoParent]) error {
    // Custom checks, only after framework/schema/database validation passed.
    return nil
}
~~~~

Comments identify application-specific rules, not a validation bypass. Reuse the same injected hook object. Do not write DB rows from validation hooks.

AfterSequence observes allocated IDs before Diff. Reconcile restores matched keys and parent/self links. AfterQueue must not alter queued values. Outcome-aware completion runs once; publish only after CommitConfirmed().

## Controlled error response

~~~~go
return &response.Error{
    Code: 401,
    Payload: map[string]any{
        "message": "Access denied",
        "error": map[string]any{"reason": "policy"},
        "violations": violations,
    },
    Cause: internalCause,
}
~~~~

Use 401 for the application's chosen authorization response; other validation may select another status. Preserve the public body through wrapping and keep the private cause out. Return an error when mutation must stop/roll back.

## Fixtures

Cover existing/new/supplied-zero identities, partial composite keys, same ID across tenants, mixed rows, reordered children, omitted/empty/null/false values, DB unique/reference failures, custom failures, cancellation, repeated sequence allocation, rollback, pending caller transactions and regeneration.
