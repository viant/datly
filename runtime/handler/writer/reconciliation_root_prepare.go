package writer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/runtime/handler/engine"
	h "github.com/viant/xdatly/handler"
)

// prepareFinitePhaseRoots advances only the root preparation prefix. It does
// not allocate, admit actions, or enable the public source-phase execution mode.
func (p *Program) prepareFinitePhaseRoots(ctx context.Context, plan *finitePhasePlan, validator h.Validator) (err error) {
	p.guardMu.Lock()
	attempted := p.rootPreparationAttempted
	p.rootPreparationAttempted = true
	p.guardMu.Unlock()
	a := p.reconciliation
	complete := false
	var before string
	check := func() error {
		if e := ctx.Err(); e != nil {
			return e
		}
		after, e := p.finiteRootPreparationState(plan)
		if e != nil {
			return e
		}
		if after != before {
			return fmt.Errorf("source root preparation changed protected child, Current, Original, Input, Output or native authority")
		}
		return nil
	}
	defer func() {
		panicValue := recover()
		if before != "" {
			err = errors.Join(err, check())
		}
		if !complete || err != nil || panicValue != nil {
			if a != nil {
				a.active = false
				a.rootPrepared = false
			}
			if panicValue != nil {
				err = errors.Join(err, fmt.Errorf("source root preparation panicked"))
			}
			p.retainExecutionFailure(err)
		}
		if panicValue != nil {
			panic(panicValue)
		}
	}()
	if attempted {
		return fmt.Errorf("source root preparation already attempted")
	}
	if a == nil || !a.active || plan == nil || a.selectionSealed != plan || a.selectionState == "" || a.selectionOutput == "" {
		return fmt.Errorf("source root preparation requires retained pure selection state")
	}
	selectionState, e := p.reconciliationState()
	if e != nil {
		return e
	}
	selectionOutput, e := immutableValues([]reflect.Value{reflect.ValueOf(p.output)})
	if e != nil {
		return e
	}
	if selectionState != a.selectionState || selectionOutput != a.selectionOutput {
		return fmt.Errorf("source root preparation changed data after pure selection")
	}
	before, err = p.finiteRootPreparationState(plan)
	if err != nil {
		return err
	}
	var roots []*Frame
	for i, root := range a.roots {
		frame := root.frame
		roots = append(roots, frame)
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = p.applyReconciliationAssignments(frame.Entity, frame, plan.compiled.fields, plan.roots[i], nil); err != nil {
			return err
		}
		if err = check(); err != nil {
			return err
		}
		if err = p.applyInvariants(frame); err != nil {
			return err
		}
		if err = p.checkConcurrency(frame); err != nil {
			return err
		}
		if err = check(); err != nil {
			return err
		}
		if err = p.callFiniteRootPreparationHook(ctx, "Init", frame); err != nil {
			return err
		}
		if err = check(); err != nil {
			return err
		}
	}
	if err = p.validateFrameSubset(ctx, validator, false, roots); err != nil {
		return err
	}
	if err = check(); err != nil {
		return err
	}
	for _, frame := range roots {
		if frame.Action == h.WriteInsert {
			if err = validateInsertIdentity(frame.Record, frame.Entity.Elem(), nil); err != nil {
				return err
			}
		}
		if err = p.callFiniteRootPreparationHook(ctx, "Validate", frame); err != nil {
			return err
		}
		if err = check(); err != nil {
			return err
		}
	}
	// Check before publishing readiness: protected state includes the unset bit.
	if err = check(); err != nil {
		return err
	}
	preparedState, e := p.reconciliationState()
	if e != nil {
		return e
	}
	preparedOutput, e := immutableValues([]reflect.Value{reflect.ValueOf(p.output)})
	if e != nil {
		return e
	}
	a.preparedState, a.preparedOutput = preparedState, preparedOutput
	a.rootPrepared = true
	complete = true
	before = ""
	return nil
}

func (p *Program) callFiniteRootPreparationHook(ctx context.Context, name string, frame *Frame) (err error) {
	finish, err := engine.BeginReconciliation(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, finish()) }()
	return p.callEntityHook(ctx, name, frame)
}

// Root business values may change; all remaining observations retain their
// pointer/slot identities as well as values. In particular a permitted root
// setter cannot change protected child/Current data through a shared pointee.
func (p *Program) finiteRootPreparationState(plan *finitePhasePlan) (string, error) {
	return p.finiteRootProtectedState(plan, false, false)
}

func (p *Program) finiteRootProtectedState(plan *finitePhasePlan, prepared, allocating bool) (string, error) {
	a := p.reconciliation
	if a == nil || !a.active || !a.observationsClosed || a.rootPrepared != prepared || a.rootAllocated || plan == nil || a.selectionSealed != plan || plan.owner != a || plan.compiled == nil || plan.compiled.root != p.metadata.Root || len(plan.roots) != len(a.roots) || p.finiteRootDecision == nil {
		return "", fmt.Errorf("source root preparation requires active sealed selection authority")
	}
	input := reflect.ValueOf(p.input).Elem()
	rows := input.Field(p.metadata.InputField)
	if allocating {
		if err := p.validateFiniteRootAllocationFacts(rows, nil); err != nil {
			return "", err
		}
	} else if err := p.validateFiniteRootDecision(rows); err != nil {
		return "", err
	}
	if err := p.validateFinitePhaseFrames(plan.compiled, rows); err != nil {
		return "", err
	}
	if len(p.reconciliationFrames) != len(p.frames.Rows) || p.actions == nil || len(p.actions.Rows) != 0 || len(p.queueItems) != 0 {
		return "", fmt.Errorf("source root preparation changed native frame or action authority")
	}
	values := []reflect.Value{reflect.ValueOf(reflect.ValueOf(p.frames).Pointer()), reflect.ValueOf(reflect.ValueOf(a).Pointer()), reflect.ValueOf(reflect.ValueOf(plan).Pointer()), reflect.ValueOf(p.output), reflect.ValueOf(p.rootPreparationAttempted)}
	for i, frame := range p.frames.Rows {
		if p.reconciliationFrames[i] != frame || frame.Original == nil {
			return "", fmt.Errorf("source root preparation changed captured native frame")
		}
		allocation := a.allocations[frame]
		if allocation == nil || allocation.frame != frame || allocation.allocated.IsValid() {
			return "", fmt.Errorf("source root preparation changed allocation authority")
		}
		values = append(values, reflect.ValueOf(reflect.ValueOf(frame).Pointer()), frame.Previous, allocation.preallocation, reflect.ValueOf(frame.Original.Available()))
		for _, field := range frame.Record.Fields {
			values = append(values, reflect.ValueOf(frame.Original.Has(field.Name)))
			if original, ok := frame.Original.(originalPresence); ok {
				values = append(values, original.identityValues[field.Name])
			}
		}
		if frame.Parent == nil {
			if allocating {
				row := frame.Entity.Elem()
				for j := 0; j < row.NumField(); j++ {
					if j != p.finiteRootDecision.metadata.key.Index[0] && row.Type().Field(j).PkgPath == "" {
						values = append(values, row.Field(j))
					}
				}
			}
			if frame.Record != p.metadata.Root || frame.Action != p.finiteRootDecision.action {
				return "", fmt.Errorf("source root preparation changed captured root action")
			}
			for _, role := range plan.compiled.roles {
				values = append(values, frame.Entity.Elem().FieldByIndex(role.field))
				key := p.finiteRootDecision.metadata.key
				marker := frame.Entity.Elem().FieldByIndex(key.Has[:len(key.Has)-1])
				if !isNil(marker) {
					field := indirect(marker).FieldByName(role.holder)
					if field.IsValid() {
						values = append(values, field)
					}
				}
			}
		} else {
			values = append(values, frame.Entity, reflect.ValueOf(frame.Action))
		}
	}
	for i, root := range a.roots {
		if root.frame != root.ref.ticket.frame || root.ref.ticket.owner != a || !a.tickets[root.ref.ticket] {
			return "", fmt.Errorf("source root preparation changed root occurrence authority")
		}
		values = append(values, reflect.ValueOf(reflect.ValueOf(root.ref.ticket).Pointer()), reflect.ValueOf(plan.roots[i]))
		if p.finiteRootDecision.action == h.WriteUpdate {
			previous := root.frame.Previous
			if !previous.IsValid() || previous.Kind() != reflect.Pointer || previous.IsNil() || previous.Type() != reflect.PointerTo(p.metadata.Root.EntityType) {
				return "", fmt.Errorf("source root preparation requires real canonical Current")
			}
			found := false
			for _, current := range p.database.ByRecord[p.metadata.Root] {
				if current.IsValid() && current.Type() == previous.Type() && current.Pointer() == previous.Pointer() {
					found = true
					break
				}
			}
			_, complete := p.metadata.Root.loadedKey(previous.Elem())
			if !found || !complete || p.finiteRootDecision.metadata.readKey(previous.UnsafePointer()) != p.finiteRootDecision.occurrences[i].key {
				return "", fmt.Errorf("source root preparation requires matching bound Current")
			}
		}
	}
	var collect func(*Record)
	collect = func(record *Record) {
		values = append(values, reflect.ValueOf(len(p.database.ByRecord[record])))
		for _, row := range p.database.ByRecord[record] {
			values = append(values, row)
		}
		for _, relation := range record.Relations {
			collect(relation.Child)
		}
	}
	collect(p.metadata.Root)
	for i := 0; i < input.NumField(); i++ {
		field := input.Type().Field(i)
		if i == p.metadata.InputField || field.PkgPath != "" {
			continue
		}
		parts := strings.Split(field.Tag.Get("parameter"), ",")
		if field.Tag.Get("setMarker") == "true" {
			values = append(values, input.Field(i))
			continue
		}
		for _, part := range parts {
			switch part {
			case "kind=body", "kind=view", "kind=query", "kind=path", "kind=header", "kind=cookie", "kind=const", "kind=env":
				values = append(values, input.Field(i))
			}
		}
	}
	for _, phase := range plan.phases {
		values = append(values, reflect.ValueOf(phase.phase.name), reflect.ValueOf(phase.phase.scope), reflect.ValueOf(phase.phase.workflow))
		for _, root := range phase.roots {
			values = append(values, reflect.ValueOf(root.followup))
			for _, selected := range root.selected {
				values = append(values, reflect.ValueOf(reflect.ValueOf(selected.Occurrence.ticket).Pointer()), reflect.ValueOf(selected.Assignments))
			}
			for _, deleted := range root.deletes {
				values = append(values, reflect.ValueOf(reflect.ValueOf(deleted.ticket).Pointer()))
			}
		}
	}
	return immutableValues(values)
}
