# Generic mutation hook scaffolding

At the transcription boundary, select `HandlerGo`,
`GoHandlerOptions{Execution: GoExecutionMutation}` and
`HookOptions{Scaffold: true}`. `Destination` optionally names a package-local
`.go` file; its default is `<component>_hooks.go`.

`ScaffoldMutationHooks` consumes the same semantic plan and resolved record
types as `MutationProgram`. It proposes a detached plan plus source. Each role
without an authored `Entity.Hooks` declaration receives its own hook type. Its
stable name includes the factory and a digest of canonical view identity and
input path. Distinct roles sharing an entity type retain distinct hook objects.

The empty methods implement the SDK's existing `EntityHooks[T,P]`,
`AfterSequenceHook[T,P]` and `AfterQueueHook[T,P]` contracts. The root also gets
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

Keep the scaffold option enabled when regenerating automatically bound roles,
or declare the authored hook explicitly through the existing `entity_hooks`
metadata. With the option off, ordinary authored hook declarations still work;
standalone scaffold types do not implicitly activate themselves.

Direct execution retains its separate `ScaffoldHooks` input `Init` / output
`Finalize` contract. Generic hooks use the existing mutation lifecycle and
outcome semantics. In particular, `AfterQueue` is not a commit notification;
publication belongs in outcome-aware finalization after confirmed commit.
