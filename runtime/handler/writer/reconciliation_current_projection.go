package writer

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	h "github.com/viant/xdatly/handler"
	"github.com/viant/xunsafe"
)

// Pending values are detached selection evidence, not an Action or a completed
// stage. Later native admission must establish cumulative Current advancement.
type finitePendingUpdate struct {
	self                *finitePendingUpdate
	owner               *Program
	transition          *finitePhaseTransition
	occurrence, current OccurrenceRef
	predecessor         string
	storage, candidate  reflect.Value // cloned complete Current domain, preserving aliases
	payload             reflect.Value // source shallow scalar capture from candidate Current
	ordinal             int
	seal                string
}

func (p *Program) observeFiniteCurrentUpdate(ctx context.Context, transition *finitePhaseTransition) (result *finitePendingUpdate, err error) {
	defer func() {
		v := recover()
		if v != nil {
			err = errors.Join(err, fmt.Errorf("source Current projection panicked"))
		}
		if err != nil {
			result = nil
			p.retireFiniteCursor(err)
		}
		if v != nil {
			panic(v)
		}
	}()
	if err = p.validateFinitePhaseTransition(ctx, transition); err != nil {
		return nil, err
	}
	if p.finitePendingUpdate != nil {
		return p.finitePendingUpdate, nil
	}
	c := p.finiteCursor
	step := c.steps[c.position]
	if step.kind != "updates" {
		return nil, fmt.Errorf("source Current projection requires reached UPDATE segment")
	}
	if len(step.occurrences) == 0 {
		return nil, nil
	}
	phase := c.plan.phases[step.phase]
	if phase.phase.updateBasis != "current" {
		return nil, fmt.Errorf("source Current projection requires declared Current basis")
	}
	ref := step.occurrences[0]
	working := false
	ticket, e := p.reconciliation.resolve(ref, ref.ticket.root, phase.phase.role.Child, &working)
	if e != nil {
		return nil, e
	}
	reader, e := compileFiniteRootDecision(ticket.record)
	if e != nil {
		return nil, e
	}
	action, e := p.classifyFinitePhaseOccurrence(phase.phase, ticket, reader)
	if e != nil {
		return nil, e
	}
	if action.kind != h.WriteUpdate {
		// This occurrence belongs to the later INSERT segment. Observing it
		// cannot skip ahead, initialize it or classify a successor.
		return nil, nil
	}
	current := action.current.ticket
	rows := p.database.ByRecord[ticket.record]
	if current == nil || current.currentOrdinal < 0 || current.currentOrdinal >= len(rows) || rows[current.currentOrdinal].Pointer() != current.previous.Pointer() {
		return nil, fmt.Errorf("source Current projection changed bound occurrence")
	}
	domain := reflect.MakeSlice(reflect.SliceOf(reflect.PointerTo(ticket.record.EntityType)), len(rows), len(rows))
	for i, row := range rows {
		domain.Index(i).Set(row)
	}
	base, e := detachedRow(domain)
	if e != nil {
		return nil, e
	}
	candidate, e := detachedRow(reflect.ValueOf(base))
	if e != nil {
		return nil, e
	}
	u := &finitePendingUpdate{owner: p, transition: transition, occurrence: ref, current: action.current, predecessor: c.checkpoint, storage: reflect.ValueOf(base), candidate: reflect.ValueOf(candidate), ordinal: current.currentOrdinal}
	u.self = u
	var selection *ReconciliationSelection
	for _, root := range phase.roots {
		for i := range root.selected {
			if root.selected[i].Occurrence == ref {
				selection = &root.selected[i]
			}
		}
	}
	if selection == nil {
		return nil, fmt.Errorf("source Current projection lost sealed selection")
	}
	frame := *ticket.frame
	frame.Previous = current.previous
	frame.Entity = u.candidate.Index(u.ordinal)
	frame.Action = h.WriteUpdate
	if err = p.applyReconciliationAssignments(frame.Entity, &frame, phase.phase.fields, selection.Assignments, phase.phase.role); err != nil {
		return nil, err
	}
	// Pinned legacy SQLX copies the struct, retaining pointer fields and Has.
	// Use the same fast shallow copy, rather than deep-cloning each UPDATE.
	u.payload = reflect.New(ticket.record.EntityType)
	xunsafe.Copy(u.payload.UnsafePointer(), frame.Entity.UnsafePointer(), int(ticket.record.EntityType.Size()))
	if u.seal, err = u.state(); err != nil {
		return nil, err
	}
	if err = p.validateFinitePhaseTransition(ctx, transition); err != nil {
		return nil, err
	}
	p.finitePendingUpdate = u
	c.pendingUpdate = u
	if err = p.validateFinitePendingUpdate(); err != nil {
		return nil, err
	}
	return u, nil
}

func (u *finitePendingUpdate) state() (string, error) {
	return immutableValues([]reflect.Value{reflect.ValueOf(u.predecessor), reflect.ValueOf(u.ordinal), reflect.ValueOf(reflect.ValueOf(u.occurrence.ticket).Pointer()), reflect.ValueOf(reflect.ValueOf(u.current.ticket).Pointer()), u.storage, u.candidate, u.payload})
}
func (p *Program) validateFinitePendingUpdate() error {
	u := p.finitePendingUpdate
	c := p.finiteCursor
	if u == nil || u.self != u || u.owner != p || c == nil || u.transition != c.next || u.predecessor != c.checkpoint || u.ordinal < 0 || u.current.ticket == nil || u.occurrence.ticket == nil || u.current.ticket.owner != c.attempt || u.occurrence.ticket.owner != c.attempt {
		return fmt.Errorf("source pending Current projection authority changed")
	}
	state, err := u.state()
	if err != nil {
		return err
	}
	if state != u.seal {
		return fmt.Errorf("source pending Current projection changed")
	}
	return nil
}
