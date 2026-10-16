package engine

import (
	"context"
	"errors"
	"reflect"
	"sync"

	dexec "github.com/viant/datly/exec"
)

// A receipt is local to one explicit lifecycle Invoke. It changes neither the
// returned error nor its public interfaces. Nested calls cannot claim it again.
type readerInvocationKey struct{}
type readerInvocation struct {
	mu       sync.Mutex
	owner    *dataScope
	claimed  bool
	returned error
}

func claimReaderCall(ctx context.Context) *readerInvocation {
	receipt, _ := ctx.Value(readerInvocationKey{}).(*readerInvocation)
	if receipt == nil {
		return nil
	}
	scope, _ := ctx.Value(dataScopeContextKey{}).(*dataScope)
	if scope == nil {
		return nil
	}
	owner := scope
	if scope.root != nil {
		owner = scope.root
	}
	if receipt.owner != owner {
		return nil
	}
	receipt.mu.Lock()
	defer receipt.mu.Unlock()
	if receipt.claimed {
		return nil
	}
	receipt.claimed = true
	return receipt
}
func (r *readerInvocation) publish(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.returned = err
}
func (r *readerInvocation) handled(err error) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Exact identity, never errors.As over a mixed joined result or a reused
	// receipt. Uncomparable custom errors are conservatively sticky.
	return sameReaderError(err, r.returned)
}
func ordinaryReaderCause(err error) bool {
	if err == nil {
		return false
	}
	var panicErr *dexec.PanicError
	if errors.As(err, &panicErr) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	_, ok := singleReaderCause(err)
	return ok
}

// Every branch must terminate in the same stable error identity. A read error
// and its response projection may represent that cause twice; any distinct
// cause makes the aggregate fatal. No selected branch certifies the others.
func singleReaderCause(err error) (error, bool) {
	if err == nil {
		return nil, false
	}
	switch cause := err.(type) {
	case interface{ Unwrap() []error }:
		children := cause.Unwrap()
		if len(children) == 0 {
			return nil, false
		}
		leaf, ok := singleReaderCause(children[0])
		if !ok {
			return nil, false
		}
		for _, child := range children[1:] {
			other, ok := singleReaderCause(child)
			if !ok || !sameReaderError(leaf, other) {
				return nil, false
			}
		}
		return leaf, true
	case interface{ Unwrap() error }:
		if child := cause.Unwrap(); child != nil {
			return singleReaderCause(child)
		}
	}
	return err, reflect.ValueOf(err).Comparable()
}

func sameReaderError(a, b error) bool {
	return a != nil && b != nil && reflect.ValueOf(a).Comparable() && reflect.ValueOf(b).Comparable() && a == b
}
