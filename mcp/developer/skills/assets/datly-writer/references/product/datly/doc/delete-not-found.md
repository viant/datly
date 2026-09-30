# Opt-in idempotent deletion

Generated PATCH/PUT leaf views with an explicit delete marker may opt into
ignoring complete identities that have no matched authorized Previous row:

```sql
SELECT sessions.id, sessions.should_delete,
       type(sessions,'SessionDelete'),
       delete_marker(sessions.should_delete),
       delete_not_found(sessions,'ignore')
FROM (SELECT id, 0 AS should_delete FROM session) sessions
```

This fragment belongs in ordinary writer DQL with package, connector, route,
input/output holders and CAST declarations. Transcription emits the view option
`onDeleteNotFound=ignore`. Omission or `error` keeps strict matching.

Missing rows are no-ops: no insert, DML, entity hooks, or transaction solely for
the skipped row. Their request entries remain in the transformed body. Known
rows retain native delete ordering, transactions and database constraints.
Incomplete identity still fails. Parent/link and authorization checks are not
relaxed; a row excluded by authored read scope is never deleted through an
unscoped fallback. Only leaf roles may use `ignore`; missing parents cannot
silently suppress descendants. Readers, POST, auxiliary roles and marker-free
views reject this policy.

A role declaring a concurrency token remains strict even with `ignore`. A
mutation predicate remains strict when its invocation evaluates active criteria;
an omitted optional predicate group adds no guard and permits the ordinary
idempotent missing-delete policy. Predicate evaluation errors fail closed.
Expected-state failures must not become success. Execution-time
guard misses continue returning Conflict. This is a deletion policy, not a
second predicate language or an SQLX condition option.

Generated SQLite tests cover metadata roundtrip, missing/mixed deletion,
incomplete identity, scope exclusion and retained mutation guards. Agently's
legacy comparison additionally covers empty/repeated IDs and atomic rollback.
