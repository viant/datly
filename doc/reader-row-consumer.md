# Consuming generated reader rows

A trusted internal caller can supply `Rows: handler.ConsumeRows(...)` on an
Xdatly `handler.ComponentRequest`. The callback's argument must be the exact
pointer type of the generated root row. SQL, connector selection, binding,
authorization, decoding and `OnFetch` remain owned by the generated reader.

```go
request.Rows = handler.ConsumeRows(func(ctx context.Context, row *export.Row) error {
    return workbook.WriteRow(row.ID, row.Name)
})
_, err := invoker.Invoke(ctx, request)
```

The callback runs synchronously for each decoded row. The returned output keeps
its normal envelope but contains no collected root rows. The callback may retain
a row explicitly; otherwise database-row memory does not grow with result count.
A workbook or other destination may still buffer its final output.

This opt-in supports flat, typed collection readers without relations,
self-references, partitions, shared caches or relation-completion hooks. It does
not support an active transaction: ordinary transactional readers drain their
cursor before running hooks so nested database work remains safe. A consumer and
its row hooks must not invoke nested database work while the cursor is open.
Use ordinary collecting reads for those cases.

The consumer belongs only to the explicitly targeted result read. It is not
installed in context and is not inherited by authorization, input dependencies
or subsequent invocations. HTTP and MCP input cannot supply it. Callback errors
and cancellation stop reading and close the cursor; streaming queries do not
retry, so previously delivered rows are not replayed into the destination.
Normal readers retain their existing collection, retry and transaction behavior.
