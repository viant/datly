package drainowner

import "context"

type transactionLifetimeKey struct{}

// WithTransactionLifetime keeps operation cancellation and values intact while
// associating transactions started by that operation with the root invocation.
func WithTransactionLifetime(ctx, lifetime context.Context) context.Context {
	if lifetime == nil {
		return ctx
	}
	return context.WithValue(ctx, transactionLifetimeKey{}, lifetime)
}

// TransactionLifetime returns the exact invocation lifetime when supplied.
// Standalone callers retain their existing transaction context.
func TransactionLifetime(ctx context.Context) context.Context {
	if ctx != nil {
		if lifetime, ok := ctx.Value(transactionLifetimeKey{}).(context.Context); ok && lifetime != nil {
			return lifetime
		}
	}
	return ctx
}
