# Generic mutation hook scaffolding

At the transcription boundary, select `HandlerGo`,
`GoHandlerOptions{Execution: GoExecutionMutation}` and
`HookOptions{Scaffold: true}`. `Destination` optionally names a package-local
`.go` file; its default is `lifecycle.go`.

`ScaffoldMutationHooks` consumes the same semantic plan and resolved record
types as `MutationProgram`. It proposes a detached plan plus source only for
explicit `lifecycle_type(view, 'Type')` declarations marked for local scaffolding
by orchestration. It never derives a lifecycle name from an entity or factory.
Unbound roles remain hookless. Known authored hooks retain their authority.
Different typed parent contracts require explicitly distinct lifecycle names.

The empty methods implement the SDK's `EntityHooks[T,P,O]`,
`AfterSequenceHook[T,P,O]` and `AfterQueueHook[T,P,O]` contracts. Each receives
`LifecycleContext[T,P,O]` with the invocation-owned output. The root also gets
`Finalize(ctx, input, output, outcome) error`. Root state uses `NoParent`;
children use their declared parent's entity type. Self descendants retain that
role's declared parent type and receive the self ancestor through `SelfParent`.
No application validation or other business behavior is generated.

The root orchestration owner validates actual source through the native method
owner and canonical entity-hook compiler before lowering the program. Adding
Bindly fields to the user-owned hook is supported; regeneration inspects the
current authored fields when selecting the existing invocation binding path.

The root also compiles the complete proposed scaffold and retains a private copy
of its native method signatures and role identities. Generation and direct plan
emission require that independent evidence and compare mutable source against
it. Editing a returned plan's hook signature or role metadata cannot approve
itself. The artifact owner's `RetainEntityHookEvidence` method is an orchestration
seam for already compiler-approved results, not an SDK signature compiler.

The existing generation transaction creates the file once and excludes it from
the generated manifest. Repeat generation preserves its bytes. Mandatory methods
must remain compatible; optional methods may be deleted. An added role or a
changed entity/parent contract that the existing file cannot implement fails
before persistence. The author must update the user-owned file explicitly.

Every binding comes from `lifecycle_type`, including on regeneration. The high-level
pure Go CLI enables create-once local scaffolding. Unresolved imported types outside
the destination package fail and must be authored there; known types must pass
canonical signature validation. `#import('hooks','example.com/shop/hooks')` declares
package authority, not a request to generate a foreign package. No registration
list or second source registry is needed.

Direct execution retains its separate `ScaffoldHooks` input `Init` / output
`Finalize` contract. Generic hooks use the existing mutation lifecycle and
outcome semantics. In particular, `AfterQueue` is not a commit notification;
publication belongs in outcome-aware finalization after confirmed commit.
