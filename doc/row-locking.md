# Trusted reader row locks

Declare the physical source a reader may lock with metadata:

```sql
SELECT data_rows.*,
       row_lock(data_rows, 'message m', 'm.id')
FROM (
  SELECT m.* FROM message m WHERE m.conversation_id = $ConversationId
) data_rows
```

The first argument names the canonical view. The second identifies its physical
table and alias. The optional third argument is a qualified physical column for
stable ascending lock order. It is applied only to locking invocations, before
any existing order. The annotation is consumed during transcription and never
sent to the database. It declares a capability; ordinary reads remain unlocked.

Request that capability through an existing native component request:

```go
request.ReaderOptions = &exec.ReaderOptions{
    ForUpdate: []string{exec.RootView},
}
result, err := invoker.InvokeComponent(ctx, request)
```

`exec.RootView` selects only the invoked reader's root. Canonical view names can
select other explicitly capable views in its result graph. Unknown, duplicate,
and noncapable targets fail. The option is trusted invocation metadata, never
an HTTP query/header/body parameter. Component request options apply after
canonical binding; dependency binding retains its own invocation policy.

The caller must already own an active managed transaction. The reader neither
begins nor completes that transaction. Locking reads bypass shared caches;
cache-only reads under a transaction fail. The builder locates exactly one
SELECT directly reading the declared physical table/alias and places the lock
there, including inside derived sources. Nested scalar/EXISTS subqueries of
that physical block retain their independent scopes; they do not create a
second lock target. Independent sibling matches are ambiguous. An absent source,
aggregate/union target, invalid order, existing lock clause, missing transaction,
or unsupported dialect fails before row execution. SQL arguments keep their
original order.

MySQL and PostgreSQL emit `FOR UPDATE`. SQLite emits no SELECT lock clause and
uses the caller's transaction locks. The existing `$View.ForUpdate()` template
helper shares the same transaction/dialect checks, but native options remove
the need for private DQL lock parameters and conditional templates.

Regression coverage includes ordinary ordering, SQLite caller rollback/cache
bypass, generated metadata/bootstrap round trips, and two independent MySQL
pools. The MySQL contender waits for the owner to commit and receives the new
row despite an earlier repeatable-read snapshot; an unrelated row remains
available. Set `SQLX_SCOPED_MYSQL_DSN` only in the test process to enable that
optional live fixture.
