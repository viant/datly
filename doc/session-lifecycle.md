# Root structural validation and queue observation

These independent options apply to native generated Go writers. Existing writers
keep early null-element rejection and physical queue callbacks unchanged.

## Root collection null policy

Declare the policy on the writable root SQL namespace:

```sql
SELECT r.*, type(r,'Record'),
       root_null_policy(r,'initial-validation'), lifecycle_type(r,'Lifecycle')
FROM (SELECT id,name FROM records) r
```

`initial-validation` is the only supported value. The annotation round-trips as
`spec.View.RootNullPolicy` (`rootNullPolicy` in serialized metadata) and the
canonical Go tag `view:"records,rootNullPolicy=initial-validation,..."`. Current
read views do not inherit it. Readers, auxiliary roles, child roles and to-one
roots cannot opt in. Nested invalid collection elements still fail early unless their exact relation separately opts into `nested_null_policy`; root policy never enables that behavior.

Capture retains the original null position and snapshots valid rows' original
presence before `Input.Init`. It neither replaces nor removes a null. Frame
construction excludes invalid elements from entity hooks. Input initialization
and other existing binding/index/entity errors retain their precedence.

At initial native structural validation, `*handler.RootNullRecordError` from
`github.com/viant/xdatly/handler` reports canonical `Location` and retained private
`Cause` (`errors.As` and `errors.Is` work). It is an operational error, not a
violation. It stops native row validation, aggregate `ValidateInput`, allocation
and queueing. Nulls introduced during initialization use the same policy;
captured diagnostics survive later input changes. Absent/null/empty collections
retain their existing contracts.

Applications project their existing envelope in outcome finalization. Observe
actual `PhaseInvocation`, `PhaseInputInitialization` and `PhaseValidation`
boundaries for logging; do not synthesize earlier events in finalization.

## Queue attempts

A physical root's authored lifecycle object opts in by implementing:

```go
func (*Lifecycle) ObserveQueueAttempt(ctx context.Context, event handler.QueueAttemptEvent) {
    // Apply the application's existing conditional diff/presence logging rule.
}
```

This exact nonvariadic signature has no return value. The compiler and runtime
reject incompatible signatures and declarations on children/auxiliary roots.
The root receives attempts for every writable graph role; no extra child
persistence loop is needed.

Each reached item produces `PhaseBegin`, followed by `PhaseEnd`. `InvocationID`
is stable across native retries; `Attempt` is zero-based and retry observers are
fresh objects. `Position` is zero-based in that attempt's queue traversal.
`Location` uses Go input holders and indexes; `Role` identifies the compiled
writer role. `Operation` is insert/update/delete. `Disposition` is physical/noop.
Terminal `Result` is queued/noop/failed/canceled/panicked, and `Queued` indicates
whether DML returned success (including an eventual `AfterQueue` failure).
False means queue acceptance was not confirmed.
Neither field proves flush or commit. Outcome finalization owns transaction
completion, including late failures and caller-owned pending transactions.

`Row` and `Previous` are detached pointers to the canonical entity type, with
fresh graphs at each boundary. `Presence` reflects working coverage and
`Original` preserves pre-initialization markers and marker availability.
`PreviousFields` describes actually loaded scalar fields; an unloaded optional
field is not evidence of a known null. This evidence lets applications apply
their source diff rules to identity-only rows and conditional child logs. Use
canonical fields/roles to select the appropriate concrete type. The event does
not expose output, parent working values, a binder, or DML.

A clone failure sets private `EvidenceError` and omits only unavailable row
evidence. It cannot alter execution. `Cause` retains private error identity via
`errors.Is`; it deliberately does not expose mutable operational errors through
`errors.As` or `Unwrap`. The invocation owner retains the original typed error.
Observers must follow their existing policy for logging private diagnostics.

Physical actions retain native dependency order and reverse-delete order.
Matched updates without mutable fields appear as observation-only entries in
that traversal. Filtering them out leaves the original physical action sequence.
Skipped deletes and unmatched-identity no-ops remain excluded. A no-op does not
start a transaction, resolve DML, allocate, flush, advance a token, or call
`AfterQueue`. Same-value fields with mutable presence retain existing physical
write behavior. Traversal stops at the first failure; unreached items emit nothing.

Observer panics are contained; at most one generic diagnostic is logged per
attempt, and a diagnostic logger panic is contained too. Observers cannot veto
writes or change retry/completion decisions. Do not retain injected services or
use them to mutate invocation state; callbacks are synchronous observation.

## Verification boundary

The framework regression fixtures prove metadata regeneration, root/null/default
boundaries, detached evidence, failure prefixes, physical order, no-op semantics,
SQLite rollback and caller-pending outcomes, native retry and concurrent request
isolation. These fixtures are not Platform integration or acceptance evidence.
Application-specific HTTP envelopes and exact source log assertions belong to
Platform integration. MySQL/backend parity remains a separate integration check.

## Exact relation null policy

`nested_null_policy(attribute, 'initial-validation')` opts only the named writable collection relation into deferred structural rejection. Roots, to-one relations, readers, auxiliary roles, Current views and unknown policy values are invalid targets. The policy does not inherit into descendants or siblings. Metadata uses `NestedNullPolicy`, serialized `nestedNullPolicy`, and canonical `view` tag `nestedNullPolicy=initial-validation`; generated Current views omit it.

Capture preserves null slots, original indexes and valid rows' original presence. Locations include parent occurrences, for example `Session[1].Attribute[2]`. Input initialization and valid entity hooks still run; invalid elements never receive entity hooks. Application parent hooks must tolerate the unchanged child collection. At initial structural validation, `handler.NestedNullRecordError{Location, Cause}` stops native row and business validation, allocation and queueing. Applications may project an existing error envelope; this is not a violation or a fabricated panic.

Root policy remains independent and RootNullRecordError stays root-specific. If both policies are enabled, the first captured deferred error wins in depth-first capture order (root indexes, compiled relation order, child indexes). Captured diagnostics survive later initialization changes and precede newly introduced nulls. A non-opted-in null or another operational binding/index/frame/initialization failure still fails at its normal earlier boundary. Absent/null/empty collection holders and optional nil to-one relations keep existing behavior.
