package writer

import (
	"context"
	"errors"
	"fmt"

	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/engine"
	h "github.com/viant/xdatly/handler"
)

// admitFinitePhaseRoots consumes only the primary root stage. Physical append
// evidence survives hook failures; it never grants child or completion rights.
func (p *Program) admitFinitePhaseRoots(ctx context.Context, plan *finitePhasePlan) (err error) {
	p.guardMu.Lock()
	attempted := p.rootAdmissionAttempted
	p.rootAdmissionAttempted = true
	binder, binding, registered := p.guardBinder, p.guardBinding, p.executionGuardRegistered
	p.guardMu.Unlock()
	a := p.reconciliation
	complete, appended := false, false
	appendCount := 0
	defer func() {
		panicValue := recover()
		if appended && a != nil {
			a.rootAppendPrefix += appendCount
		}
		if panicValue != nil {
			err = errors.Join(err, fmt.Errorf("source root admission panicked"))
		}
		if !complete || err != nil || panicValue != nil {
			if a != nil {
				a.active = false
				a.rootAdmitted = false
			}
			p.retainExecutionFailure(err)
		}
		if panicValue != nil {
			panic(panicValue)
		}
	}()
	if attempted {
		return fmt.Errorf("source root admission already attempted")
	}
	if !registered || binder == nil || binding == nil || a == nil || !a.active || a.selectionSealed != plan || a.rootAdmitted || a.rootAppendPrefix != 0 {
		return fmt.Errorf("source root admission requires its captured registered guard and exact plan")
	}
	if err = p.validateFiniteRootProjections(plan); err != nil {
		return err
	}
	if p.actions == nil || len(p.actions.Rows) != 0 || len(p.queueItems) != 0 {
		return fmt.Errorf("source root admission requires empty native action span")
	}
	service, err := lookup[h.DML](ctx, binder, h.DMLKey)
	if err != nil {
		return err
	}
	if err = engine.ValidateBoundDML(ctx, service, binding); err != nil {
		return err
	}
	guarded, ok := service.(interface{ ValidateExecutionGuards(context.Context) error })
	if !ok {
		return fmt.Errorf("source root admission requires native retained guards")
	}
	if err = guarded.ValidateExecutionGuards(ctx); err != nil {
		return err
	}
	root := p.metadata.Root
	if root.Auxiliary || root.Table == "" || root.QueueContract != "source-slice" || root.ConcurrencyToken != nil || root.MutationPredicateGroup != nil || p.queueObserver() != nil {
		return fmt.Errorf("source root admission requires unobserved physical source-slice roots without matched/criteria options")
	}
	if _, ok := service.(rhandler.QueueContractDML); !ok {
		return fmt.Errorf("source root admission requires native queue contract")
	}
	for _, occurrence := range a.roots {
		frame := occurrence.frame
		if frame == nil || frame.Record != root || frame.Parent != nil || !frame.holderIndexed || frame.Action != p.finiteRootDecision.action {
			return fmt.Errorf("source root admission has invalid canonical root occurrence")
		}
	}
	actions, err := p.mintFiniteRootActions(plan)
	if err != nil {
		return err
	}
	p.actions.Rows = append([]*Action(nil), actions...)
	p.rootAdmissionSpan = append([]*Action(nil), actions...)
	p.rootAdmissionContainer = p.actions
	p.rootAdmissionPublished = true
	check := func() error {
		if e := ctx.Err(); e != nil {
			return e
		}
		if e := p.validateFiniteRetainedActions(); e != nil {
			return e
		}
		if e := engine.ValidateBoundDML(ctx, service, binding); e != nil {
			return e
		}
		return guarded.ValidateExecutionGuards(ctx)
	}
	if err = check(); err != nil {
		return err
	}
	if len(actions) != 0 && p.finiteRootDecision.action == h.WriteInsert {
		if err = p.mintSourceSliceGroup(actions); err != nil {
			return err
		}
		if err = check(); err != nil {
			return err
		}
		appendCount = len(actions)
		err = p.queueSourceSlice(ctx, binder, service, actions, &appended)
		if err != nil {
			return err
		}
		a.rootAppendPrefix += appendCount
		appended = false
	} else {
		for _, action := range actions {
			if err = check(); err != nil {
				return err
			}
			appendCount = 1
			err = p.queuePhysical(ctx, binder, service, action, p.actionFrame(action), &appended)
			if err != nil {
				return err
			}
			a.rootAppendPrefix++
			appended = false
		}
	}
	if err = check(); err != nil {
		return err
	}
	a.rootAdmitted = true
	// Retain the predecessor at the successful native boundary, rather than
	// trusting whatever live values a later cursor initializer happens to see.
	a.rootAdmissionState, err = p.finiteAllocationClassificationState()
	if err != nil {
		return err
	}
	complete = true
	return nil
}
