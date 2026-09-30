package exec

import "context"

// TransactionIsolation is a trusted execution policy for a managed database
// transaction. It is independent of HTTP input or component parameters.
type TransactionIsolation string

const (
	IsolationReadCommitted  TransactionIsolation = "read_committed"
	IsolationRepeatableRead TransactionIsolation = "repeatable_read"
	IsolationSerializable   TransactionIsolation = "serializable"
)

type transactionIsolationKey struct{}

// WithTransactionIsolation requests the isolation before TransactionStarter
// starts the managed unit. It cannot replace or reconfigure an existing unit.
func WithTransactionIsolation(ctx context.Context, isolation TransactionIsolation) context.Context {
	return context.WithValue(ctx, transactionIsolationKey{}, isolation)
}

// RequestedTransactionIsolation reports the policy on this execution scope.
func RequestedTransactionIsolation(ctx context.Context) (TransactionIsolation, bool) {
	if ctx == nil {
		return "", false
	}
	isolation, ok := ctx.Value(transactionIsolationKey{}).(TransactionIsolation)
	return isolation, ok
}
