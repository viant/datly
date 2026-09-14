package dml

import (
	"context"
	"errors"
	"fmt"

	xhandler "github.com/viant/xdatly/handler"
)

var _ xhandler.TransactionStarter = (*Data)(nil)

// Start opens the managed transaction early without executing queued DML.
// The same root owns completion whether this frame or a descendant starts it.
func (d *Data) Start(ctx context.Context) error {
	if d == nil || ctx == nil {
		return fmt.Errorf("transaction startup requires data and context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	owner := d.owner()
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	owner.mu.Lock()
	var err error
	switch {
	case owner.completed:
		err = ErrInvocationCompleted
	case owner.failed != nil:
		err = errors.Join(ErrInvocationFailed, owner.failed)
	case !owner.invocation:
		err = fmt.Errorf("transaction startup requires a managed invocation")
	case !d.open:
		err = ErrComponentSealed
	}
	owner.mu.Unlock()
	if err != nil {
		return err
	}
	_, err = owner.transaction(ctx)
	if err != nil {
		owner.markFailed(err)
	}
	return err
}
