package writer

import (
	"context"
	"fmt"
	"reflect"

	xhandler "github.com/viant/xdatly/handler"
)

// finitePhasePlan is detached native selection data, not a second action queue.
// It is validated in full before allocation. Assignments stay unpublished until
// the native execution cursor reaches the corresponding phase/group.
type finitePhasePlan struct {
	owner    *reconciliationAttempt
	compiled *finiteSourcePhases
	roots    [][]ReconciliationAssignment
	phases   []finitePhaseSelections
}
type finitePhaseSelections struct {
	phase   *finiteSourcePhase
	roots   []finiteRootPhaseSelection
	current []OccurrenceRef
}
type finiteRootPhaseSelection struct {
	root     OccurrenceRef
	selected []ReconciliationSelection
	deletes  []OccurrenceRef
	followup []ReconciliationAssignment
}

func (p *Program) sealFinitePhasePlan(ctx context.Context, compiled *finiteSourcePhases, plan ReconciliationPlan) (*finitePhasePlan, error) {
	a := p.reconciliation
	if a == nil || !a.active || compiled == nil || compiled.root != p.metadata.Root || p.finiteRootDecision == nil {
		return nil, fmt.Errorf("source phase plan requires active canonical occurrence authority")
	}
	rows := reflect.ValueOf(p.input).Elem().Field(p.metadata.InputField)
	if err := p.validateFiniteRootDecision(rows); err != nil {
		return nil, err
	}
	if err := p.validateFinitePhaseFrames(compiled, rows); err != nil {
		return nil, err
	}
	if len(plan.Roots) != len(a.roots) {
		return nil, fmt.Errorf("source phase plan requires every root in canonical order")
	}
	result := &finitePhasePlan{owner: a, compiled: compiled}
	for i, root := range plan.Roots {
		if root.Root != a.roots[i].ref || root.Roles != nil {
			return nil, fmt.Errorf("source phase plan changed root order or supplied legacy role plans")
		}
		assignments, err := p.copyPhaseAssignments(a.roots[i].frame, compiled.fields, root.Assignments, nil)
		if err != nil {
			return nil, err
		}
		result.roots = append(result.roots, assignments)
	}
	branch := compiled.insert
	if p.finiteRootDecision.action == xhandler.WriteUpdate {
		branch = compiled.update
	}
	if len(plan.Phases) != len(branch) {
		return nil, fmt.Errorf("source phase plan requires every compiled phase")
	}
	usedWorking := map[*reconciliationTicket]bool{}
	usedCurrent := map[*reconciliationTicket]bool{}
	for i, selection := range plan.Phases {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		phase := &branch[i]
		if selection.Phase != phase.name || len(selection.Roots) != len(a.roots) {
			return nil, fmt.Errorf("source phase plan changed phase or root order")
		}
		out := finitePhaseSelections{phase: phase}
		for j, root := range selection.Roots {
			occurrence := &a.roots[j]
			if root.Root != occurrence.ref {
				return nil, fmt.Errorf("source phase plan changed root occurrence")
			}
			entry := finiteRootPhaseSelection{root: root.Root}
			var role *reconciliationRoleOccurrences
			for k := range occurrence.roles {
				if occurrence.roles[k].role.relation == phase.role {
					role = &occurrence.roles[k]
					break
				}
			}
			if role == nil {
				return nil, fmt.Errorf("source phase plan has no captured canonical holder")
			}
			positions := map[*reconciliationTicket]int{}
			for k, ref := range role.working {
				positions[ref.ticket] = k
			}
			last := -1
			for _, selected := range root.Selected {
				working := false
				ticket, err := a.resolve(selected.Occurrence, occurrence.frame, phase.role.Child, &working)
				if err != nil {
					return nil, err
				}
				position, found := positions[ticket]
				if !found || position <= last || usedWorking[ticket] || selected.AdoptCurrent.ticket != nil || phase.workflow == "current-deletes" {
					return nil, fmt.Errorf("source phase plan reordered, repeated or adopted a working occurrence")
				}
				last = position
				usedWorking[ticket] = true
				assignments, err := p.copyPhaseAssignments(ticket.frame, phase.fields, selected.Assignments, phase.role)
				if err != nil {
					return nil, err
				}
				entry.selected = append(entry.selected, ReconciliationSelection{Occurrence: selected.Occurrence, Assignments: assignments})
			}
			if len(root.Deletes) > 0 && phase.workflow != "current-deletes" && phase.workflow != "current-deletes-then-working-inserts" && phase.workflow != "working-updates-current-deletes-then-inserts" {
				return nil, fmt.Errorf("source phase plan supplied undeclared deletes")
			}
			for _, ref := range root.Deletes {
				current := true
				ticket, err := a.resolve(ref, occurrence.frame, phase.role.Child, &current)
				if err != nil {
					return nil, err
				}
				if usedCurrent[ticket] {
					return nil, fmt.Errorf("source phase plan repeated a Current delete")
				}
				if err = p.requireReconciliationCurrent(phase.role, occurrence.frame, ticket.previous, true); err != nil {
					return nil, err
				}
				usedCurrent[ticket] = true
				entry.deletes = append(entry.deletes, ref)
			}
			var err error
			entry.deletes, err = a.orderedPhaseCurrent(phase.role.Child, entry.deletes)
			if err != nil {
				return nil, err
			}
			out.current = append(out.current, entry.deletes...)
			if len(root.Followup) > 0 {
				if phase.followup == nil {
					return nil, fmt.Errorf("source phase plan supplied an undeclared root followup")
				}
				entry.followup, err = p.copyPhaseAssignments(occurrence.frame, phase.followup.fields, root.Followup, nil)
				if err != nil {
					return nil, err
				}
			}
			out.roots = append(out.roots, entry)
		}
		var err error
		out.current, err = a.orderedPhaseCurrent(phase.role.Child, out.current)
		if err != nil {
			return nil, err
		}
		result.phases = append(result.phases, out)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (p *Program) copyPhaseAssignments(frame *Frame, fields map[string]Field, assignments []ReconciliationAssignment, relation *Relation) ([]ReconciliationAssignment, error) {
	row, err := detachedRow(frame.Entity)
	if err != nil {
		return nil, err
	}
	if err = p.applyReconciliationAssignments(reflect.ValueOf(row), frame, fields, assignments, relation); err != nil {
		return nil, err
	}
	result := append([]ReconciliationAssignment(nil), assignments...)
	for i := range result {
		var value reflect.Value
		if result[i].Value != nil {
			value = reflect.ValueOf(result[i].Value)
		}
		result[i].Value, err = detachedRow(value)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
