# Finite same-parent reconciliation

A SQL-derived PATCH writer may opt into `finite_reconciliation(root, '<JSON descriptor>')`. The only initial mode is `same-parent-root-first`. The descriptor names `rootFields` and ordered `roles`; each role names its exact `holder`, scalar `fields`, and optional `adoptIdentity`. Only one adoption role is admitted. All direct writable leaf collections must appear in the existing graph order. The graph needs complete identities, explicit parent links, bound Current, and a physical non-delete root.

The canonical root lifecycle declares:

```go
ReconcileInput(context.Context, *Input, *Output, writer.ReconciliationContext) (writer.ReconciliationPlan, error)
```

The native allocator runs once over the initialized, initially validated population. The callback follows ordinary AfterSequence, before WriteEligible and final native validation. The callback observes canonical Input/Output plus detached observations from `ReconciliationContext.Roots()`. It must keep canonical values fixed. Managed allocation, DML, SQL reads, component invocations and completion are denied, including when a retained capability is called with a replacement context. Native capability guards do not sandbox arbitrary application Go; raw network/filesystem/SQL effects are outside this lifecycle contract.

Each observation supplies an opaque invocation-scoped `OccurrenceRef`, detached Row/Previous, ClientIdentity and Original presence, and separate Preallocation/Allocated evidence. Root and holder orders remain occurrence orders. Bound Current observations are filtered to the receiving parent and preserve native enumeration order. Detached values can be used for independent premerge counts, eligibility and event snapshots.

A plan must name every root in the same order and every declared holder. `Selected` contains captured request occurrences or bound Current occurrences. Omission discards a request occurrence without reclaiming its allocation. `AdoptCurrent` rematches a captured request to a specific same-parent Current row in the declared adoption role; a literal identity cannot grant authority. `Deletes` names bound Current occurrences and produces internal native deletes, outside the public collections. Declared scalar assignments use exact Go value types and optional source setter presence. Parent-link assignments must equal their native parent tuple. Identity fields cannot be scalar assignments.

The writer validates the entire finite data plan before publishing working holders, constructs effective frames without recapturing Original or reallocating, rebuilds native reference producers, and runs final native validation. Internal deletion evidence uses the existing native UPDATE validation contract because the validator does not accept a DELETE candidate; the physical frame/action remains DELETE. Selected Current and adopted rows keep authoritative immutable Previous. Distinct request occurrences with the same physical ID remain distinct actions. Cross-role or unexplained cross-parent pointer associations fail.

For each root, native traversal admits the eligible root write and its AfterQueue, then each declared role's deletes followed by writes, preserving list order. Native DML and completion keep ownership. This does not establish cross-connector physical execution order. Root suppression retains linked direct-child writes only when the native persisted parent-producer guard passes. AfterQueueInput remains the terminal batch boundary.

Scoped sequences, assigned/missing-identity policy, insert-delete policy, source-row queue contracts, explicit delete markers, concurrency tokens, mutation predicates, auxiliary roots, recursive/derived writable graphs and grandchildren are initially rejected. Writers with no opt-in retain their existing paths. Retained queued-state guards include effective holders, occurrence/frame/action associations, Original, Current/Previous and superseded allocation evidence. A consumed reconciliation attempt cannot be reused; owner-scope recovery and composing-parent replay are vetoed from allocation admission onward.
