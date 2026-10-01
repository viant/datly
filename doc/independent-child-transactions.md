# Independent child transactions

Ordinary component composition shares the caller's managed unit. A successful
child return does not by itself prove a database commit.

A source-less custom orchestration component can explicitly opt into independent
child ownership using canonical component metadata:

```sql
#setting($_ = $independent_child_transactions(true))
```

Generated Go metadata preserves this as `independentChildTransactions:"true"`.
The setting is `spec.Settings.IndependentChildTransactions`. A trusted internal
caller may also set `exec.ComponentRequest.IndependentChildTransactions`; it is
not a body/query option and is never inherited as a transport field.

The engine retains the same context, verified inputs, providers, configured
logger, connector capability, request trace and cancellation. Only the otherwise
automatic connector-only neutral root is omitted. Each generated child opens and
completes its own native unit; `ComponentRequest.Completion` receives ordinary
canonical `handler.Outcome` evidence. Publish only after `CommitConfirmed()`.
After a publication failure, invoke a fresh generated compensation component.
Its independent failure does not roll back an already committed child.

The policy is rejected before binding/effects unless the handler is the native
custom contract adapter and the root has no DataSource, sequence strategy,
outcome finalizer, injector finalizer or Completion callback. Any inherited
managed unit is rejected, including a caller-owned pending transaction. No
context detachment, early commit of someone else's unit or transaction override
occurs. `exec.IndependentChildTransactionError` exposes a typed ownership reason.
Generated writers/readers themselves cannot request this source-less policy.

The existing cycle guard still checks exact component identities. An orchestrator
must use fixed, authorized internal targets; callers cannot choose a target,
provider or transaction policy through JSON. This mode permits deliberate partial
commit behavior, so the application's publication, compensation and public error
policy must be explicit. Default shared behavior is unchanged.
