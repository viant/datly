package writer

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/viant/datly/runtime/handler/engine"
	h "github.com/viant/xdatly/handler"
)

// A cursor retains selection positions, never actions or a second journal.
// No position can advance until native stage evidence has been implemented.
type finitePhaseCursor struct {
	self                           *finitePhaseCursor
	owner                          *Program
	attempt                        *reconciliationAttempt
	plan                           *finitePhasePlan
	steps                          []finitePhaseSegment
	position                       int
	checkpoint, evidence, schedule string
	next                           *finitePhaseTransition
}
type finitePhaseSegment struct {
	phase, root int // root=-1 is a single aggregate group, including empty groups.
	kind        string
	occurrences []OccurrenceRef
}
type finitePhaseTransition struct {
	cursor      *finitePhaseCursor
	position    int
	predecessor string
}

// Workflows determine segment order. Working references are not classified,
// initialized or allocated here: UPDATE and INSERT candidates are resolved
// only when their eventual native execution segment is reached.
func finitePhaseSegments(plan *finitePhasePlan) ([]finitePhaseSegment, error) {
	if plan == nil || plan.owner == nil || plan.compiled == nil {
		return nil, fmt.Errorf("source cursor requires a sealed phase plan")
	}
	var result []finitePhaseSegment
	for pi, selection := range plan.phases {
		phase := selection.phase
		if phase == nil || len(selection.roots) != len(plan.owner.roots) {
			return nil, fmt.Errorf("source cursor changed phase membership")
		}
		var kinds []string
		switch phase.workflow {
		case "current-deletes":
			kinds = []string{"deletes"}
		case "working-inserts":
			kinds = []string{"inserts"}
		case "current-deletes-then-working-inserts":
			kinds = []string{"deletes", "inserts"}
		case "working-updates-then-inserts":
			kinds = []string{"updates", "inserts"}
		case "working-updates-current-deletes-then-inserts":
			kinds = []string{"updates", "deletes", "inserts"}
		default:
			return nil, fmt.Errorf("source cursor has unsupported workflow")
		}
		groups := len(selection.roots)
		if phase.scope == "all-roots" {
			groups = 1
		} else if phase.scope != "each-root" {
			return nil, fmt.Errorf("source cursor has unsupported scope")
		}
		for gi := 0; gi < groups; gi++ {
			ri := gi
			if phase.scope == "all-roots" {
				ri = -1
			}
			for _, kind := range kinds {
				step := finitePhaseSegment{phase: pi, root: ri, kind: kind}
				if kind == "deletes" && ri < 0 {
					step.occurrences = append([]OccurrenceRef(nil), selection.current...)
				} else {
					for i, root := range selection.roots {
						if ri >= 0 && ri != i {
							continue
						}
						if kind == "deletes" {
							step.occurrences = append(step.occurrences, root.deletes...)
						} else {
							for _, selected := range root.selected {
								step.occurrences = append(step.occurrences, selected.Occurrence)
							}
						}
					}
				}
				result = append(result, step)
			}
			// Source setters are reached per group even when root UPDATE admission
			// is delayed until all groups have finished.
			if phase.followup != nil {
				for i, root := range selection.roots {
					if (ri >= 0 && ri != i) || len(root.followup) == 0 {
						continue
					}
					result = append(result, finitePhaseSegment{phase: pi, root: i, kind: "followup-values", occurrences: []OccurrenceRef{root.root}})
					if phase.followup.placement == "after-group" {
						result = append(result, finitePhaseSegment{phase: pi, root: i, kind: "followup-admission", occurrences: []OccurrenceRef{root.root}})
					}
				}
			}
		}
		if phase.followup != nil && phase.followup.placement == "after-phase" {
			for i, root := range selection.roots {
				if len(root.followup) > 0 {
					result = append(result, finitePhaseSegment{phase: pi, root: i, kind: "followup-admission", occurrences: []OccurrenceRef{root.root}})
				}
			}
		}
	}
	return result, nil
}

func finiteCursorScheduleState(c *finitePhaseCursor) (string, error) {
	values := []reflect.Value{reflect.ValueOf(len(c.steps))}
	for _, step := range c.steps {
		values = append(values, reflect.ValueOf(step.phase), reflect.ValueOf(step.root), reflect.ValueOf(step.kind), reflect.ValueOf(len(step.occurrences)))
		for _, ref := range step.occurrences {
			values = append(values, reflect.ValueOf(reflect.ValueOf(ref.ticket).Pointer()))
		}
	}
	return immutableValues(values)
}

func (p *Program) finiteCursorEvidenceState(plan *finitePhasePlan) (string, error) {
	a := p.reconciliation
	planState, err := finiteProjectionPlanState(plan)
	if err != nil {
		return "", err
	}
	return immutableValues([]reflect.Value{reflect.ValueOf(planState), reflect.ValueOf(a.selectionState), reflect.ValueOf(a.selectionOutput), reflect.ValueOf(a.preparedState), reflect.ValueOf(a.preparedOutput), reflect.ValueOf(a.allocationState), reflect.ValueOf(a.projectionState), reflect.ValueOf(a.rootAppendPrefix), reflect.ValueOf(a.rootAdmitted), reflect.ValueOf(a.rootAdmissionState)})
}

func (p *Program) startFinitePhaseCursor(ctx context.Context, plan *finitePhasePlan) (err error) {
	complete := false
	defer func() {
		v := recover()
		if v != nil {
			err = errors.Join(err, fmt.Errorf("source cursor initialization panicked"))
		}
		if !complete || err != nil || v != nil {
			p.retireFiniteCursor(err)
		}
		if v != nil {
			panic(v)
		}
	}()
	if p.finiteCursorAttempted {
		return fmt.Errorf("source cursor already attempted")
	}
	p.finiteCursorAttempted = true
	a := p.reconciliation
	if a == nil || !a.active || !a.rootAdmitted || a.rootAppendPrefix != len(a.roots) || a.selectionSealed != plan || !p.executionGuardRegistered {
		return fmt.Errorf("source cursor requires exact successful root admission")
	}
	if err = p.validateFiniteRetainedActions(); err != nil {
		return err
	}
	service, e := lookup[h.DML](ctx, p.guardBinder, h.DMLKey)
	if e != nil {
		return e
	}
	if err = engine.ValidateBoundDML(ctx, service, p.guardBinding); err != nil {
		return err
	}
	guarded, ok := service.(interface{ ValidateExecutionGuards(context.Context) error })
	if !ok {
		return fmt.Errorf("source cursor requires native execution guards")
	}
	if err = guarded.ValidateExecutionGuards(ctx); err != nil {
		return err
	}
	c := &finitePhaseCursor{owner: p, attempt: a, plan: plan}
	c.self = c
	if c.steps, err = finitePhaseSegments(plan); err != nil {
		return err
	}
	if c.checkpoint, err = p.finiteAllocationClassificationState(); err != nil {
		return err
	}
	if c.evidence, err = p.finiteCursorEvidenceState(plan); err != nil {
		return err
	}
	if c.schedule, err = finiteCursorScheduleState(c); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	p.finiteCursor, p.finiteCursorPublished = c, true
	if err = p.validateFiniteCursorCheckpoint(); err != nil {
		return err
	}
	complete = true
	return nil
}

func (p *Program) validateFiniteCursorCheckpoint() error {
	c := p.finiteCursor
	if c == nil || c.self != c || c.owner != p || c.attempt != p.reconciliation || !c.attempt.active || c.plan != c.attempt.selectionSealed || !c.attempt.rootAdmitted || c.position != 0 {
		return fmt.Errorf("source cursor authority changed or advanced without native stage evidence")
	}
	state, err := p.finiteAllocationClassificationState()
	if err != nil {
		return err
	}
	if state != c.checkpoint {
		return fmt.Errorf("source cursor predecessor live state changed")
	}
	state, err = p.finiteCursorEvidenceState(c.plan)
	if err != nil {
		return err
	}
	if state != c.evidence {
		return fmt.Errorf("source cursor immutable evidence changed")
	}
	state, err = finiteCursorScheduleState(c)
	if err != nil {
		return err
	}
	if state != c.schedule {
		return fmt.Errorf("source cursor schedule changed")
	}
	if c.next != p.finiteCursorNext || c.next != nil && (c.next.cursor != c || c.next.position != c.position || c.next.predecessor != c.checkpoint) {
		return fmt.Errorf("source cursor transition changed")
	}
	return nil
}

// Observation only: this ticket cannot be submitted to a DML API or acknowledge
// stage success. There is deliberately no advancement method yet.
func (p *Program) nextFinitePhaseTransition(ctx context.Context) (ticket *finitePhaseTransition, err error) {
	defer func() {
		v := recover()
		if v != nil {
			err = errors.Join(err, fmt.Errorf("source cursor observation panicked"))
		}
		if err != nil {
			ticket = nil
			p.retireFiniteCursor(err)
		}
		if v != nil {
			panic(v)
		}
	}()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = p.validateFiniteCursorCheckpoint(); err != nil {
		return nil, err
	}
	c := p.finiteCursor
	if len(c.steps) == 0 {
		return nil, nil
	}
	if c.next == nil {
		c.next = &finitePhaseTransition{cursor: c, position: c.position, predecessor: c.checkpoint}
		p.finiteCursorNext = c.next
	}
	return c.next, nil
}

func (p *Program) validateFinitePhaseTransition(ctx context.Context, ticket *finitePhaseTransition) (err error) {
	defer func() {
		v := recover()
		if v != nil {
			err = errors.Join(err, fmt.Errorf("source cursor transition validation panicked"))
		}
		if err != nil {
			p.retireFiniteCursor(err)
		}
		if v != nil {
			panic(v)
		}
	}()
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = p.validateFiniteCursorCheckpoint(); err != nil {
		return err
	}
	if ticket == nil || ticket != p.finiteCursor.next || ticket.cursor != p.finiteCursor || ticket.position != p.finiteCursor.position || ticket.predecessor != p.finiteCursor.checkpoint {
		return fmt.Errorf("source cursor requires its exact next transition")
	}
	return nil
}

func (p *Program) retireFiniteCursor(err error) {
	if p.reconciliation != nil {
		p.reconciliation.active = false
	}
	p.retainExecutionFailure(err)
}
