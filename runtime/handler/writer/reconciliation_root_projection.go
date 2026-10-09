package writer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"

	h "github.com/viant/xdatly/handler"
)

// These are payload images, never a second action journal or admission proof.
type finiteRootProjection struct {
	root      OccurrenceRef
	action    h.WriteAction
	primary   reflect.Value
	followup  reflect.Value
	phase     int
	placement string
}

func (p *Program) projectFinitePhaseRoots(ctx context.Context, plan *finitePhasePlan) (err error) {
	p.guardMu.Lock()
	attempted := p.rootProjectionAttempted
	p.rootProjectionAttempted = true
	p.guardMu.Unlock()
	a := p.reconciliation
	complete := false
	check := func() error {
		if e := ctx.Err(); e != nil {
			return e
		}
		state, e := p.finiteAllocationClassificationState()
		if e != nil {
			return e
		}
		if a == nil || state != a.allocationState {
			return fmt.Errorf("source root projection changed allocation authority")
		}
		return p.validateFiniteAllocatedRoots(plan)
	}
	defer func() {
		panicValue := recover()
		if panicValue != nil {
			err = errors.Join(err, fmt.Errorf("source root projection panicked"))
		}
		if !complete || err != nil || panicValue != nil {
			if a != nil {
				a.active = false
				a.rootProjections = nil
				a.projectionState = ""
			}
			p.retainExecutionFailure(err)
		}
		if panicValue != nil {
			panic(panicValue)
		}
	}()
	if attempted {
		return fmt.Errorf("source root projection already attempted")
	}
	if a == nil || !a.active || !a.rootAllocated || a.selectionSealed != plan || a.allocationState == "" {
		return fmt.Errorf("source root projection requires exact successful allocation")
	}
	if err = check(); err != nil {
		return err
	}
	batch := reflect.MakeSlice(reflect.SliceOf(reflect.PointerTo(p.metadata.Root.EntityType)), len(a.roots), len(a.roots))
	for i, root := range a.roots {
		batch.Index(i).Set(root.frame.Entity)
	}
	// Clone the complete graph once per image generation to preserve aliases.
	futureValue, e := detachedRow(batch)
	if e != nil {
		return e
	}
	earlyValue, e := detachedRow(batch)
	if e != nil {
		return e
	}
	future, early := reflect.ValueOf(futureValue), reflect.ValueOf(earlyValue)
	projections := make([]finiteRootProjection, len(a.roots))
	for i, root := range a.roots {
		projections[i] = finiteRootProjection{root: root.ref, action: p.finiteRootDecision.action, primary: early.Index(i), phase: -1}
	}
	for phaseIndex, phase := range plan.phases {
		if phase.phase.followup == nil {
			continue
		}
		for i, selection := range phase.roots {
			if len(selection.followup) == 0 {
				continue
			}
			if projections[i].phase >= 0 {
				return fmt.Errorf("source root projection repeats a followup")
			}
			if err = p.applyReconciliationAssignments(future.Index(i), a.roots[i].frame, phase.phase.followup.fields, selection.followup, nil); err != nil {
				return err
			}
			projections[i].phase, projections[i].placement = phaseIndex, phase.phase.followup.placement
			if projections[i].placement == "after-group" {
				value, e := detachedRow(future.Index(i))
				if e != nil {
					return e
				}
				projections[i].followup = reflect.ValueOf(value)
			}
		}
		for i := range projections {
			if projections[i].phase == phaseIndex && projections[i].placement == "after-phase" {
				value, e := detachedRow(future.Index(i))
				if e != nil {
					return e
				}
				projections[i].followup = reflect.ValueOf(value)
			}
		}
	}
	for i := range projections {
		if projections[i].action == h.WriteInsert {
			projections[i].primary = future.Index(i)
		} else if projections[i].action == h.WriteUpdate {
			if err = p.copyFiniteProjectionMarkers(projections[i].primary, future.Index(i)); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("source root projection has unsupported action")
		}
		if projections[i].followup.IsValid() {
			// UPDATE is a shallow source snapshot: earlier scalar values, later Has.
			if err = p.copyFiniteProjectionMarkers(projections[i].followup, future.Index(i)); err != nil {
				return err
			}
		}
	}
	if err = check(); err != nil {
		return err
	}
	state, e := finiteRootProjectionState(projections)
	if e != nil {
		return e
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	a.rootProjections, a.projectionState = projections, state
	complete = true
	return nil
}

func (p *Program) copyFiniteProjectionMarkers(destination, future reflect.Value) error {
	for _, field := range p.metadata.Root.Fields {
		if len(field.Has) < 2 {
			continue
		}
		path := field.Has[:len(field.Has)-1]
		target, source := destination.Elem().FieldByIndex(path), future.Elem().FieldByIndex(path)
		if !target.CanSet() || target.Type() != source.Type() {
			return fmt.Errorf("source root projection has unsupported presence holder")
		}
		target.Set(source)
	}
	return nil
}

func finiteRootProjectionState(projections []finiteRootProjection) (string, error) {
	values := []reflect.Value{reflect.ValueOf(len(projections))}
	for _, projection := range projections {
		values = append(values, reflect.ValueOf(reflect.ValueOf(projection.root.ticket).Pointer()), reflect.ValueOf(projection.action), projection.primary, projection.followup, reflect.ValueOf(projection.phase), reflect.ValueOf(projection.placement))
	}
	return immutableValues(values)
}

// Seal only explicit plan authority, avoiding reflective traversal of runtime
// metadata capabilities and compiled accessors.
func finiteProjectionPlanState(plan *finitePhasePlan) (string, error) {
	if plan == nil || plan.compiled == nil {
		return "", fmt.Errorf("source projection plan is unavailable")
	}
	values := []reflect.Value{reflect.ValueOf(reflect.ValueOf(plan).Pointer()), reflect.ValueOf(plan.roots)}
	appendFields := func(fields map[string]Field) {
		names := make([]string, 0, len(fields))
		for name := range fields {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			field := fields[name]
			values = append(values, reflect.ValueOf(name), reflect.ValueOf(field.Name), reflect.ValueOf(field.Column), reflect.ValueOf(field.Index), reflect.ValueOf(field.Has), reflect.ValueOf(field.AutoIncrement), reflect.ValueOf(field.RefDB), reflect.ValueOf(field.RefTable), reflect.ValueOf(field.RefColumn), reflect.ValueOf(reflect.ValueOf(field.marker).Pointer()), reflect.ValueOf(field.markerIndex))
		}
	}
	appendFields(plan.compiled.fields)
	rootFields := map[string]Field{}
	for _, field := range plan.compiled.root.Fields {
		rootFields[field.Name] = field
	}
	appendFields(rootFields)
	values = append(values, reflect.ValueOf(plan.compiled.root.Table))
	for _, phase := range plan.phases {
		if phase.phase == nil || phase.phase.role == nil || phase.phase.role.Child == nil {
			return "", fmt.Errorf("source projection phase has no canonical role")
		}
		metadata := phase.phase
		values = append(values, reflect.ValueOf(metadata.name), reflect.ValueOf(metadata.scope), reflect.ValueOf(metadata.workflow), reflect.ValueOf(metadata.updateBasis), reflect.ValueOf(reflect.ValueOf(metadata.role).Pointer()), reflect.ValueOf(metadata.role.Field))
		appendFields(metadata.fields)
		values = append(values, reflect.ValueOf(reflect.ValueOf(metadata.role.Child).Pointer()), reflect.ValueOf(metadata.role.Child.Table))
		keys := map[string]Field{}
		for _, field := range metadata.role.Child.Keys {
			keys[field.Name] = field
		}
		appendFields(keys)
		for _, link := range metadata.role.Links {
			values = append(values, reflect.ValueOf(link.Parent.Name), reflect.ValueOf(link.Parent.Index), reflect.ValueOf(link.Child.Name), reflect.ValueOf(link.Child.Index))
		}
		if metadata.followup != nil {
			values = append(values, reflect.ValueOf(metadata.followup.placement))
			appendFields(metadata.followup.fields)
		}
		for _, root := range phase.roots {
			values = append(values, reflect.ValueOf(reflect.ValueOf(root.root.ticket).Pointer()), reflect.ValueOf(root.followup))
			for _, selection := range root.selected {
				values = append(values, reflect.ValueOf(reflect.ValueOf(selection.Occurrence.ticket).Pointer()), reflect.ValueOf(selection.Assignments))
			}
			for _, ref := range root.deletes {
				values = append(values, reflect.ValueOf(reflect.ValueOf(ref.ticket).Pointer()))
			}
		}
		for _, ref := range phase.current {
			values = append(values, reflect.ValueOf(reflect.ValueOf(ref.ticket).Pointer()))
		}
	}
	return immutableValues(values)
}

func (p *Program) validateFiniteRootProjections(plan *finitePhasePlan) (err error) {
	a := p.reconciliation
	defer func() {
		panicValue := recover()
		if panicValue != nil {
			err = errors.Join(err, fmt.Errorf("source root projection validation panicked"))
		}
		if err != nil {
			if a != nil {
				a.active = false
				a.rootProjections = nil
				a.projectionState = ""
			}
			p.retainExecutionFailure(err)
		}
		if panicValue != nil {
			panic(panicValue)
		}
	}()
	if a == nil || !a.active || a.selectionSealed != plan || !p.rootProjectionAttempted || a.projectionState == "" || len(a.rootProjections) != len(a.roots) {
		return fmt.Errorf("source root projection authority is unavailable")
	}
	state, e := p.finiteAllocationClassificationState()
	if e != nil {
		return e
	}
	if state != a.allocationState {
		return fmt.Errorf("source root projection changed allocation authority")
	}
	if err = p.validateFiniteAllocatedRoots(plan); err != nil {
		return err
	}
	state, e = finiteRootProjectionState(a.rootProjections)
	if e != nil {
		return e
	}
	if state != a.projectionState {
		return fmt.Errorf("source root projection image changed")
	}
	return nil
}
