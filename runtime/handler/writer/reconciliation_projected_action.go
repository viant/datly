package writer

import (
	"fmt"
	h "github.com/viant/xdatly/handler"
	"reflect"
)

// A private association grants a payload image its canonical frame. It is not
// admission evidence, a stage cursor or permission to complete the program.
type projectedRootAction struct {
	owner                      *Program
	attempt                    *reconciliationAttempt
	plan                       *finitePhasePlan
	action                     *Action
	frame                      *Frame
	position                   int
	entity                     reflect.Value
	canonical                  reflect.Value
	kind                       h.WriteAction
	projectionState, planState string
	slot                       queueSlotSeal
}

// Mint the complete ordered primary root population, never a caller-chosen
// subset/payload/action. Publishing into p.actions is a later native stage.
func (p *Program) mintFiniteRootActions(plan *finitePhasePlan) (result []*Action, err error) {
	a := p.reconciliation
	defer func() {
		if value := recover(); value != nil {
			if a != nil {
				a.active = false
			}
			p.retainExecutionFailure(fmt.Errorf("projected root action mint panicked"))
			panic(value)
		}
		if err != nil {
			if a != nil {
				a.active = false
			}
			p.retainExecutionFailure(err)
		}
	}()
	if a == nil || a.projectedActionsAttempted {
		return nil, fmt.Errorf("projected root action mint already attempted or unavailable")
	}
	a.projectedActionsAttempted = true
	p.projectedRootActionsIssued = true
	if err = p.validateFiniteRootProjections(plan); err != nil {
		return nil, err
	}
	planState, err := finiteProjectionPlanState(plan)
	if err != nil {
		return nil, err
	}
	for i, image := range a.rootProjections {
		frame := a.roots[i].frame
		if image.root.ticket == nil || image.root.ticket.frame != frame || frame.Record != p.metadata.Root || frame.Parent != nil || image.action != p.finiteRootDecision.action || !image.primary.IsValid() || image.primary.Type() != frame.Entity.Type() {
			return nil, fmt.Errorf("projected root action has invalid canonical image")
		}
		slot, e := p.captureQueueSlots(frame)
		if e != nil {
			return nil, e
		}
		if e = slot.validate(reflect.ValueOf(p.input)); e != nil {
			return nil, e
		}
		action := &Action{Kind: image.action, Entity: image.primary, frame: frame}
		action.projected = &projectedRootAction{owner: p, attempt: a, plan: plan, action: action, frame: frame, position: i, entity: reflect.ValueOf(image.primary.Interface()), canonical: reflect.ValueOf(frame.Entity.Interface()), kind: image.action, projectionState: a.projectionState, planState: planState, slot: slot}
		result = append(result, action)
	}
	p.projectedRootActionCount = len(result)
	a.projectedActions = append([]*Action(nil), result...)
	for _, action := range result {
		a.projectedAuthorities = append(a.projectedAuthorities, action.projected)
	}
	return result, nil
}

// Validate ownership without replaying a whole-stage snapshot that includes
// the action list. Actual stage transitions still require their own seals.
func (p *Program) validateProjectedRootAction(action *Action) (err error) {
	defer func() {
		if value := recover(); value != nil {
			if p.reconciliation != nil {
				p.reconciliation.active = false
			}
			p.retainExecutionFailure(fmt.Errorf("projected root action validation panicked"))
			panic(value)
		}
		if err != nil {
			if p.reconciliation != nil {
				p.reconciliation.active = false
			}
			p.retainExecutionFailure(err)
		}
	}()
	if action == nil || action.projected == nil {
		return fmt.Errorf("projected root action authority unavailable")
	}
	token := action.projected
	a := p.reconciliation
	if token.owner != p || token.attempt != a || a == nil || !a.active || !a.projectedActionsAttempted || token.plan != a.selectionSealed || token.action != action || token.position < 0 || token.position >= len(a.projectedActions) || a.projectedActions[token.position] != action || token.position >= len(a.projectedAuthorities) || a.projectedAuthorities[token.position] != token || token.position >= len(a.rootProjections) || token.position >= len(a.roots) {
		return fmt.Errorf("projected root action ownership changed")
	}
	image := a.rootProjections[token.position]
	if image.root.ticket == nil || image.root.ticket != a.roots[token.position].ref.ticket || image.root.ticket.frame != token.frame || action.frame != token.frame || token.frame.Record != p.metadata.Root || token.frame.Parent != nil || token.frame.Entity.Type() != token.canonical.Type() || token.frame.Entity.Pointer() != token.canonical.Pointer() || token.frame.Action != token.kind || action.Kind != token.kind || image.action != token.kind || action.Entity.Type() != token.entity.Type() || action.Entity.Pointer() != token.entity.Pointer() || image.primary.Type() != token.entity.Type() || image.primary.Pointer() != token.entity.Pointer() {
		return fmt.Errorf("projected root action association changed")
	}
	if err = token.slot.validate(reflect.ValueOf(p.input)); err != nil {
		return err
	}
	if err = p.validateFiniteAllocatedRoots(token.plan); err != nil {
		return err
	}
	state, e := finiteProjectionPlanState(token.plan)
	if e != nil {
		return e
	}
	if state != token.planState {
		return fmt.Errorf("projected root action plan changed")
	}
	state, e = finiteRootProjectionState(a.rootProjections)
	if e != nil {
		return e
	}
	if state != token.projectionState || a.projectionState != token.projectionState {
		return fmt.Errorf("projected root action payload changed")
	}
	return nil
}

// Retained guard validation does not require ordinary execution readiness.
func (p *Program) validateFiniteRetainedActions() (err error) {
	defer func() {
		if value := recover(); value != nil {
			if p.reconciliation != nil {
				p.reconciliation.active = false
			}
			p.retainExecutionFailure(fmt.Errorf("retained source payload validation panicked"))
			panic(value)
		}
		if err != nil {
			if p.reconciliation != nil {
				p.reconciliation.active = false
			}
			p.retainExecutionFailure(err)
		}
	}()
	a := p.reconciliation
	if a == nil || !a.active {
		return fmt.Errorf("source phase attempt is unavailable or retired")
	}
	if !p.projectedRootActionsIssued {
		return nil
	}
	if !a.projectedActionsAttempted || len(a.roots) != p.projectedRootActionCount || len(a.rootProjections) != p.projectedRootActionCount || len(a.projectedActions) != p.projectedRootActionCount || len(a.projectedAuthorities) != p.projectedRootActionCount {
		return fmt.Errorf("projected root action registry changed")
	}
	if err := p.validateFiniteAllocatedRoots(a.selectionSealed); err != nil {
		return err
	}
	state, err := finiteRootProjectionState(a.rootProjections)
	if err != nil {
		return err
	}
	if state != a.projectionState {
		return fmt.Errorf("projected root payload evidence changed")
	}
	for i, action := range a.projectedActions {
		if action == nil || action.projected == nil || action.projected.position != i || action.projected != a.projectedAuthorities[i] {
			return fmt.Errorf("projected root action order changed")
		}
		if err := p.validateProjectedRootAction(action); err != nil {
			return err
		}
	}
	return nil
}
