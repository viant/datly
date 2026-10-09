package writer

import (
	"context"
	"fmt"
	"reflect"
	"sort"
)

// prepareFinitePhaseOccurrences captures selection authority before allocation.
// This internal preparation does not enable source-phase execution. In
// particular it neither runs child hooks nor changes holders or allocated IDs.
func (p *Program) prepareFinitePhaseOccurrences(ctx context.Context, phases *finiteSourcePhases) error {
	if phases == nil || phases.root != p.metadata.Root || p.finiteRootDecision == nil || p.reconciliation != nil {
		return fmt.Errorf("source phases require compiled authority and a fresh sealed root decision")
	}
	if p.metadata.Root.reconciliation == nil || len(p.metadata.Root.reconciliation.roles) != len(phases.roles) || len(p.metadata.Root.Relations) != len(phases.roles) {
		return fmt.Errorf("source phases require every canonical reconciliation role")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	roots := reflect.ValueOf(p.input).Elem().Field(p.metadata.InputField)
	if err := p.validateFiniteRootDecision(roots); err != nil {
		return err
	}
	if err := p.validateFinitePhaseFrames(phases, roots); err != nil {
		return err
	}
	if err := p.captureReconciliationAllocation(ctx); err != nil {
		return err
	}
	attempt := p.reconciliation
	if attempt == nil {
		return fmt.Errorf("source phases require canonical reconciliation metadata")
	}
	// Failed preparation must not leave usable partially minted authority.
	complete := false
	defer func() {
		if !complete {
			attempt.active = false
		}
	}()
	for _, frame := range p.frames.Rows {
		if frame.Record != p.metadata.Root {
			continue
		}
		position := len(attempt.roots)
		if position >= len(p.finiteRootDecision.occurrences) || !frame.Entity.IsValid() || frame.Entity.Kind() != reflect.Pointer || frame.Entity.IsNil() || frame.Entity.Pointer() != p.finiteRootDecision.occurrences[position].row.Pointer() {
			return fmt.Errorf("source phases changed canonical root frame order")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		root := reconciliationRootOccurrences{frame: frame, ref: attempt.mint(frame, frame.Record, frame, frame.Previous, false)}
		for i := range p.metadata.Root.reconciliation.roles {
			role := &p.metadata.Root.reconciliation.roles[i]
			if i >= len(p.metadata.Root.Relations) || role.relation != p.metadata.Root.Relations[i] {
				return fmt.Errorf("source phases changed canonical role authority")
			}
			entries := reconciliationRoleOccurrences{role: role}
			for _, child := range p.frames.Rows {
				if child.Parent == frame && child.Record == role.relation.Child {
					entries.working = append(entries.working, attempt.mint(frame, child.Record, child, child.Previous, false))
				}
			}
			for ordinal, previous := range p.database.ByRecord[role.relation.Child] {
				if err := p.requireReconciliationCurrent(role.relation, frame, previous, false); err != nil {
					return err
				}
				if relationValuesEqual(frame.Entity.Elem(), previous.Elem(), role.relation.Links) {
					ref := attempt.mint(frame, role.relation.Child, nil, previous, true)
					ref.ticket.currentOrdinal = ordinal
					entries.current = append(entries.current, ref)
				}
			}
			root.roles = append(root.roles, entries)
		}
		attempt.roots = append(attempt.roots, root)
	}
	if len(attempt.roots) != roots.Len() {
		return fmt.Errorf("source phases changed canonical root occurrences")
	}
	complete = true
	return nil
}

// orderedPhaseCurrent restores the source's bound Current enumeration for an
// aggregate phase. It does not deduplicate repeated rows or equal keys: tickets
// continue to name occurrences rather than pointer/key identities.
func (a *reconciliationAttempt) orderedPhaseCurrent(record *Record, refs []OccurrenceRef) ([]OccurrenceRef, error) {
	if a == nil || !a.active {
		return nil, fmt.Errorf("source phase occurrence authority is retired")
	}
	ordered := append([]OccurrenceRef(nil), refs...)
	for _, ref := range ordered {
		t := ref.ticket
		if t == nil || t.owner != a || !a.active || !a.tickets[t] || t.record != record || !t.current || t.currentOrdinal < 0 {
			return nil, fmt.Errorf("source phases require native bound Current occurrences")
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].ticket.currentOrdinal < ordered[j].ticket.currentOrdinal })
	return ordered, nil
}

func (p *Program) validateFinitePhaseFrames(phases *finiteSourcePhases, rows reflect.Value) error {
	for i, role := range phases.roles {
		entry := p.metadata.Root.reconciliation.roles[i]
		if entry.relation != role.relation || p.metadata.Root.Relations[i] != role.relation || entry.declaration.Holder != role.holder || !reflect.DeepEqual(role.relation.Field, role.field) {
			return fmt.Errorf("source phases changed compiled role authority")
		}
	}
	var roots []*Frame
	for _, frame := range p.frames.Rows {
		if frame.Record == phases.root {
			position := len(roots)
			if position >= rows.Len() || frame.Parent != nil || !frame.holderIndexed || !frame.holderTracked || frame.holderPosition != position || !frame.Entity.IsValid() || frame.Entity.Kind() != reflect.Pointer || frame.Entity.IsNil() || frame.Entity.Pointer() != rows.Index(position).Pointer() {
				return fmt.Errorf("source phases changed canonical root frame slots")
			}
			roots = append(roots, frame)
		}
	}
	if len(roots) != rows.Len() {
		return fmt.Errorf("source phases changed root frame population")
	}
	seen := map[*Frame]bool{}
	for _, root := range roots {
		seen[root] = true
		for _, role := range phases.roles {
			holder := root.Entity.Elem().FieldByIndex(role.field)
			position := 0
			for _, frame := range p.frames.Rows {
				if frame.Parent != root || frame.Record != role.relation.Child {
					continue
				}
				if position >= holder.Len() || seen[frame] || !frame.holderIndexed || !frame.holderTracked || frame.holderPosition != position || !frame.Entity.IsValid() || frame.Entity.Kind() != reflect.Pointer || frame.Entity.IsNil() || holder.Index(position).IsNil() || frame.Entity.Pointer() != holder.Index(position).Pointer() {
					return fmt.Errorf("source phases changed canonical child frame slots")
				}
				seen[frame] = true
				position++
			}
			if position != holder.Len() {
				return fmt.Errorf("source phases changed child frame population")
			}
		}
	}
	if len(seen) != len(p.frames.Rows) {
		return fmt.Errorf("source phases contain noncanonical or repeated frames")
	}
	return nil
}
