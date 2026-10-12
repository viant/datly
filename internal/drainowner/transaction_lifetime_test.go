package drainowner

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTransactionLifetimePreservesOperationContext(t *testing.T) {
	type operationKey struct{}
	root, cancelRoot := context.WithCancel(t.Context())
	defer cancelRoot()
	operation, cancelOperation := context.WithDeadline(context.WithValue(root, operationKey{}, "operation"), time.Now().Add(time.Minute))
	defer cancelOperation()
	ctx := WithTransactionLifetime(operation, root)
	if TransactionLifetime(ctx) != root || ctx.Value(operationKey{}) != "operation" {
		t.Fatal("transaction lifetime or operation values changed")
	}
	wantDeadline, _ := operation.Deadline()
	if got, ok := ctx.Deadline(); !ok || got != wantDeadline || ctx.Done() != operation.Done() {
		t.Fatal("operation deadline or cancellation changed")
	}
	cancelOperation()
	if !errors.Is(ctx.Err(), context.Canceled) || root.Err() != nil {
		t.Fatal("operation cancellation changed root lifetime")
	}
	cancelRoot()
	if !errors.Is(TransactionLifetime(ctx).Err(), context.Canceled) {
		t.Fatal("root cancellation lost")
	}
}

func TestTransactionLifetimeFallback(t *testing.T) {
	ctx := t.Context()
	if WithTransactionLifetime(ctx, nil) != ctx || TransactionLifetime(ctx) != ctx {
		t.Fatal("absent lifetime changed caller context")
	}
	invalid := context.WithValue(ctx, transactionLifetimeKey{}, "invalid")
	if TransactionLifetime(invalid) != invalid {
		t.Fatal("invalid lifetime changed caller context")
	}
}
