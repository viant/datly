package exec

import (
	"context"
	"database/sql"
)

type invocationTransactionLookupKey struct{}

// WithInvocationTransactionLookup lets SQL readers find the transaction owned
// by the first endpoint invocation. The lookup runs when a connector resolves,
// after an imperative child writer may have started that transaction.
func WithInvocationTransactionLookup(ctx context.Context, lookup func(context.Context, *sql.DB) (*sql.Tx, error)) context.Context {
	return context.WithValue(ctx, invocationTransactionLookupKey{}, lookup)
}

// InvocationTransaction returns the active transaction for one exact database
// handle. It does not transfer commit or rollback ownership to the caller.
func InvocationTransaction(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	lookup, _ := ctx.Value(invocationTransactionLookupKey{}).(func(context.Context, *sql.DB) (*sql.Tx, error))
	if lookup == nil {
		return nil, nil
	}
	return lookup(ctx, db)
}
