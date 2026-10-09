package writer

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/runtime/handler/engine"
)

// selectFinitePhasePlan is the existing observational reconciliation callback
// at source-phase timing. It does not initialize children or admit mutation.
func (p *Program) selectFinitePhasePlan(ctx context.Context, compiled *finiteSourcePhases) (sealed *finitePhasePlan, err error) {
	p.guardMu.Lock()
	attempted := p.phaseSelectionAttempted
	p.phaseSelectionAttempted = true
	p.guardMu.Unlock()
	complete := false
	defer func() {
		if panicValue := recover(); panicValue != nil {
			if p.reconciliation != nil {
				p.reconciliation.active = false
				p.reconciliation.observationsClosed = true
			}
			p.retainExecutionFailure(errors.Join(err, fmt.Errorf("source phase selection panicked")))
			panic(panicValue)
		}
		if !complete {
			sealed = nil
			if p.reconciliation != nil {
				p.reconciliation.active = false
				p.reconciliation.observationsClosed = true
			}
			p.retainExecutionFailure(err)
		}
	}()
	p.guardMu.Lock()
	issued, binding := p.guardIssued, p.guardBinding
	p.guardMu.Unlock()
	if issued && !drainowner.GuardRegistered(binding) {
		return nil, fmt.Errorf("source phase execution requires bound native guard registration")
	}
	if attempted {
		return nil, fmt.Errorf("source phase selection already attempted; fresh capture required")
	}
	if err = p.prepareFinitePhaseOccurrences(ctx, compiled); err != nil {
		return nil, err
	}
	a := p.reconciliation
	if !p.hook.IsValid() {
		return nil, fmt.Errorf("source phase selection requires the canonical ReconcileInput hook")
	}
	method := p.hook.MethodByName("ReconcileInput")
	if !method.IsValid() {
		return nil, fmt.Errorf("source phase selection requires the canonical ReconcileInput hook")
	}
	before, err := p.reconciliationState()
	if err != nil {
		return nil, err
	}
	beforeOutput, err := immutableValues([]reflect.Value{reflect.ValueOf(p.output)})
	if err != nil {
		return nil, err
	}
	finish, err := engine.BeginReconciliation(ctx)
	if err != nil {
		return nil, err
	}
	var plan ReconciliationPlan
	func() {
		defer func() {
			a.observationsClosed = true
			err = errors.Join(err, finish(), ctx.Err())
			afterOutput, e := immutableValues([]reflect.Value{reflect.ValueOf(p.output)})
			err = errors.Join(err, e)
			if e == nil && afterOutput != beforeOutput {
				err = errors.Join(err, fmt.Errorf("ReconcileInput changed Output"))
			}
			after, e := p.reconciliationState()
			err = errors.Join(err, e)
			if e == nil && after != before {
				err = errors.Join(err, fmt.Errorf("ReconcileInput changed Input, Current, Original, allocation or native associations"))
			}
		}()
		results := method.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(p.input), reflect.ValueOf(p.output), reflect.ValueOf(ReconciliationContext{a})})
		plan = results[0].Interface().(ReconciliationPlan)
		if !results[1].IsNil() {
			err = results[1].Interface().(error)
		}
	}()
	if err != nil {
		return nil, err
	}
	sealed, err = p.sealFinitePhasePlan(ctx, compiled, plan)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	selectionState, err := p.reconciliationState()
	if err != nil {
		return nil, err
	}
	selectionOutput, err := immutableValues([]reflect.Value{reflect.ValueOf(p.output)})
	if err != nil {
		return nil, err
	}
	a.selectionState, a.selectionOutput = selectionState, selectionOutput
	a.selectionSealed = sealed
	complete = true
	return sealed, nil
}
