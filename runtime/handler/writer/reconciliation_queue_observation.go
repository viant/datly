package writer

import (
	"context"
	"errors"
	"fmt"

	"github.com/viant/datly/runtime/handler/engine"
)

// Source-phase queue hooks observe the reached row after native publication.
// They cannot change the working graph or use injected mutation capabilities.
// This boundary grants neither admission nor source-program completion.
func (p *Program) callFiniteAfterQueueHook(ctx context.Context, frame *Frame) (err error) {
	var before, projection string
	var finish func() error
	defer func() {
		panicValue := recover()
		// Validation can itself panic after a hook damages runtime metadata.
		// Preserve that panic, but close the observation and retire first.
		func() {
			defer func() {
				if value := recover(); value != nil {
					if panicValue == nil {
						panicValue = value
					}
					err = errors.Join(err, fmt.Errorf("source AfterQueue observation validation panicked"))
				}
			}()
			if before != "" {
				after, e := p.finiteAllocationClassificationState()
				err = errors.Join(err, e)
				if e == nil && after != before {
					err = errors.Join(err, fmt.Errorf("source AfterQueue changed protected graph, Output or native actions"))
				}
				after, e = finiteRootProjectionState(p.reconciliation.rootProjections)
				err = errors.Join(err, e)
				if e == nil && after != projection {
					err = errors.Join(err, fmt.Errorf("source AfterQueue changed projected payloads"))
				}
				err = errors.Join(err, p.validateFiniteRetainedActions(), p.validateQueueSlots())
			}
		}()
		if finish != nil {
			err = errors.Join(err, finish())
		}
		err = errors.Join(err, ctx.Err())
		if panicValue != nil {
			err = errors.Join(err, fmt.Errorf("source AfterQueue hook panicked"))
		}
		if err != nil {
			if p.reconciliation != nil {
				p.reconciliation.active = false
			}
			p.retainExecutionFailure(err)
		}
		if panicValue != nil {
			panic(panicValue)
		}
	}()
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = p.validateFiniteRetainedActions(); err != nil {
		return err
	}
	before, err = p.finiteAllocationClassificationState()
	if err != nil {
		return err
	}
	projection, err = finiteRootProjectionState(p.reconciliation.rootProjections)
	if err != nil {
		return err
	}
	finish, err = engine.BeginReconciliation(ctx)
	if err != nil {
		return err
	}
	return p.callEntityHookNative(ctx, "AfterQueue", frame)
}
