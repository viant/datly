package writer

import (
	"context"
	"errors"
	"fmt"
	h "github.com/viant/xdatly/handler"
	"reflect"
)

// allocateFinitePhaseRoots uses the existing sequencer for exactly the prepared
// root batch. Child lifecycle, linking and admission remain unreached.
func (p *Program) allocateFinitePhaseRoots(ctx context.Context, plan *finitePhasePlan, sequencer h.Sequencer) (err error) {
	p.guardMu.Lock()
	attempted := p.rootAllocationAttempted
	p.rootAllocationAttempted = true
	p.guardMu.Unlock()
	a := p.reconciliation
	complete := false
	var before string
	check := func() error {
		after, e := p.finiteRootProtectedState(plan, true, true)
		if e != nil {
			return e
		}
		if before != after {
			return fmt.Errorf("source root allocation changed protected state")
		}
		return ctx.Err()
	}
	defer func() {
		panicValue := recover()
		if before != "" {
			err = errors.Join(err, check())
		}
		if !complete || err != nil || panicValue != nil {
			if a != nil {
				a.active = false
				a.rootAllocated = false
				a.allocatedRootKeys = nil
				for _, root := range a.roots {
					if value := a.allocations[root.frame]; value != nil {
						value.allocated = reflect.Value{}
					}
				}
			}
			if panicValue != nil {
				err = errors.Join(err, fmt.Errorf("source root allocation panicked"))
			}
			p.retainExecutionFailure(err)
		}
		if panicValue != nil {
			panic(panicValue)
		}
	}()
	if attempted {
		return fmt.Errorf("source root allocation already attempted")
	}
	if a == nil || !a.active || !a.rootPrepared || a.rootAllocated || a.selectionSealed != plan || a.preparedState == "" || a.preparedOutput == "" {
		return fmt.Errorf("source root allocation requires sealed prepared roots")
	}
	state, e := p.reconciliationState()
	if e != nil {
		return e
	}
	output, e := immutableValues([]reflect.Value{reflect.ValueOf(p.output)})
	if e != nil {
		return e
	}
	if state != a.preparedState || output != a.preparedOutput {
		return fmt.Errorf("source root allocation changed data after preparation")
	}
	if err = p.validateFiniteRootDecision(reflect.ValueOf(p.input).Elem().Field(p.metadata.InputField)); err != nil {
		return err
	}
	before, err = p.finiteRootProtectedState(plan, true, true)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	batch := reflect.MakeSlice(reflect.SliceOf(reflect.PointerTo(p.metadata.Root.EntityType)), 0, len(a.roots))
	for _, root := range a.roots {
		batch = reflect.Append(batch, root.frame.Entity)
	}
	if p.finiteRootDecision.action == h.WriteInsert && batch.Len() > 0 {
		if sequencer == nil {
			return fmt.Errorf("source root allocation requires native sequencer")
		}
		if err = sequencer.Allocate(ctx, p.metadata.Root.Table, batch.Interface(), p.metadata.Root.Sequence.Name); err != nil {
			return err
		}
	}
	if err = check(); err != nil {
		return err
	}
	keys := make([]int64, len(a.roots))
	images := make([]reflect.Value, len(a.roots))
	for i, root := range a.roots {
		keys[i] = p.finiteRootDecision.metadata.readKey(root.frame.Entity.UnsafePointer())
		if p.finiteRootDecision.occurrences[i].key == 0 && keys[i] <= 0 {
			return fmt.Errorf("source root allocation left unresolved identity")
		}
		image, e := detachedRow(root.frame.Entity)
		if e != nil {
			return e
		}
		images[i] = reflect.ValueOf(image)
	}
	if err = check(); err != nil {
		return err
	}
	for i, root := range a.roots {
		a.allocations[root.frame].allocated = images[i]
	}
	a.allocatedRootKeys = keys
	a.rootAllocated = true
	complete = true
	before = ""
	return nil
}

// Original decision facts never advance with a sequence allocation. Once the
// boundary succeeds, only this attempt's recorded keys authorize live IDs.
func (p *Program) validateFiniteRootAllocationFacts(rows reflect.Value, allocated []int64) error {
	decision := p.finiteRootDecision
	if decision == nil || rows.Kind() != reflect.Slice || rows.Len() != len(decision.occurrences) || allocated != nil && len(allocated) != rows.Len() {
		return fmt.Errorf("source root allocation changed root population")
	}
	for i, original := range decision.occurrences {
		row := rows.Index(i)
		if row.IsNil() || row.Pointer() != original.row.Pointer() || presenceAvailable(row.Elem()) != original.available || supplied(row.Elem(), decision.metadata.key) != original.supplied {
			return fmt.Errorf("source root allocation changed original occurrence facts")
		}
		key := decision.metadata.readKey(row.UnsafePointer())
		if original.key != 0 && key != original.key || allocated != nil && key != allocated[i] {
			return fmt.Errorf("source root allocation changed authorized identity")
		}
	}
	return nil
}

func (p *Program) validateFiniteAllocatedRoots(plan *finitePhasePlan) error {
	a := p.reconciliation
	if a == nil || !a.active || !a.rootPrepared || !a.rootAllocated || a.selectionSealed != plan || len(a.allocatedRootKeys) != len(a.roots) {
		return fmt.Errorf("source roots have no successful allocation authority")
	}
	rows := reflect.ValueOf(p.input).Elem().Field(p.metadata.InputField)
	if err := p.validateFinitePhaseFrames(plan.compiled, rows); err != nil {
		return err
	}
	return p.validateFiniteRootAllocationFacts(rows, a.allocatedRootKeys)
}
