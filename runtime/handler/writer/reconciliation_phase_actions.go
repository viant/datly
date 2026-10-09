package writer

import (
	"context"
	"fmt"
	"reflect"

	h "github.com/viant/xdatly/handler"
)

// Classification is retained selection evidence, not another action journal.
// This internal step neither admits actions nor enables source-phase execution.
type finitePhaseActionPlan struct {
	owner  *reconciliationAttempt
	phases []finitePhaseActions
}
type finitePhaseActions struct{ roots [][]finitePhaseAction }
type finitePhaseAction struct {
	occurrence OccurrenceRef
	kind       h.WriteAction
	identity   identityKey
	current    OccurrenceRef
	basis      string
}

func (p *Program) classifyFinitePhasePlan(ctx context.Context, plan *finitePhasePlan) (*finitePhaseActionPlan, error) {
	a := p.reconciliation
	if a == nil || !a.active || plan == nil || plan.owner != a || plan.compiled == nil || plan.compiled.root != p.metadata.Root || p.finiteRootDecision == nil {
		return nil, fmt.Errorf("source phase classification requires active sealed selection authority")
	}
	if err := p.validateFiniteRootDecision(reflect.ValueOf(p.input).Elem().Field(p.metadata.InputField)); err != nil {
		return nil, err
	}
	if err := p.validateFinitePhaseFrames(plan.compiled, reflect.ValueOf(p.input).Elem().Field(p.metadata.InputField)); err != nil {
		return nil, err
	}
	result := &finitePhaseActionPlan{owner: a}
	readers := map[*Record]*finiteRootDecisionMetadata{}
	updated := map[rowIdentity]bool{}
	deleted := map[rowIdentity]bool{}
	for _, phase := range plan.phases {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out := finitePhaseActions{}
		for _, root := range phase.roots {
			var actions []finitePhaseAction
			for _, selection := range root.selected {
				working := false
				ticket, err := a.resolve(selection.Occurrence, root.root.ticket.frame, phase.phase.role.Child, &working)
				if err != nil {
					return nil, err
				}
				var reader *finiteRootDecisionMetadata
				if phase.phase.updateBasis == "current" {
					reader = readers[ticket.record]
					if reader == nil {
						reader, err = compileFiniteRootDecision(ticket.record)
						if err != nil {
							return nil, err
						}
						readers[ticket.record] = reader
					}
				}
				action, err := p.classifyFinitePhaseOccurrence(phase.phase, ticket, reader)
				if err != nil {
					return nil, err
				}
				if action.kind == h.WriteUpdate {
					updated[rowIdentity{record: ticket.record, key: action.identity}] = true
				}
				actions = append(actions, action)
			}
			for _, ref := range root.deletes {
				current := true
				ticket, err := a.resolve(ref, root.root.ticket.frame, phase.phase.role.Child, &current)
				if err != nil {
					return nil, err
				}
				if err = p.requireReconciliationCurrent(phase.phase.role, ticket.root, ticket.previous, true); err != nil {
					return nil, err
				}
				key, ok := ticket.record.loadedKey(ticket.previous.Elem())
				if !ok {
					return nil, fmt.Errorf("source phase delete identity is incomplete")
				}
				deleted[rowIdentity{record: ticket.record, key: key}] = true
			}
			out.roots = append(out.roots, actions)
		}
		result.phases = append(result.phases, out)
	}
	for identity := range updated {
		if deleted[identity] {
			return nil, fmt.Errorf("source phase Current identity is both updated and deleted")
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (p *Program) classifyFinitePhaseOccurrence(phase *finiteSourcePhase, ticket *reconciliationTicket, metadata *finiteRootDecisionMetadata) (finitePhaseAction, error) {
	out := finitePhaseAction{occurrence: OccurrenceRef{ticket}, kind: h.WriteInsert, basis: "working"}
	record := ticket.record
	allocation := p.reconciliation.allocations[ticket.frame]
	if allocation == nil {
		return out, fmt.Errorf("source phase classification has no captured working occurrence")
	}
	original, ok := ticket.frame.Original.(originalPresence)
	if !ok || original.identityValues == nil {
		return out, fmt.Errorf("source phase classification requires captured original identity")
	}
	keyRow := reflect.New(record.EntityType)
	for _, key := range record.Keys {
		value, found := original.identityValues[key.Name]
		destination := keyRow.Elem().FieldByIndex(key.Index)
		if !found || !value.IsValid() || value.Type() != destination.Type() {
			return out, fmt.Errorf("source phase captured identity is incomplete or mistyped")
		}
		if err := assignLinkedValue(destination, cloneTokenValue(value)); err != nil {
			return out, err
		}
	}
	capturedKey, complete := record.loadedKey(keyRow.Elem())
	if !complete {
		return out, fmt.Errorf("source phase captured identity is incomplete")
	}
	out.identity = capturedKey
	switch phase.workflow {
	case "working-inserts", "current-deletes-then-working-inserts":
		// Previous does not turn a source insert workflow into an update.
		return out, nil
	case "working-updates-then-inserts", "working-updates-current-deletes-then-inserts":
	default:
		return out, fmt.Errorf("source phase workflow cannot classify working writes")
	}
	if phase.updateBasis == "current" {
		if metadata == nil {
			return out, fmt.Errorf("source phase marked key accessor is unavailable")
		}
		if !original.Available() || !original.Has(metadata.key.Name) || metadata.readKey(keyRow.UnsafePointer()) <= 0 {
			return out, nil
		}
	} else if phase.updateBasis == "working" {
		for _, link := range phase.role.Links {
			for _, key := range record.Keys {
				if !sameIdentityField(key, link.Child) {
					continue
				}
				// Native root allocation must resolve a new parent before a linked key
				// can classify an upsert. Do not treat lack of an early match as INSERT.
				value := ticket.root.Entity.Elem().FieldByIndex(link.Parent.Index)
				if p.finiteRootDecision.action == h.WriteInsert || !linkValueResolved(value) {
					return out, fmt.Errorf("source phase linked identity is pending native parent allocation")
				}
				if err := assignLinkedValue(keyRow.Elem().FieldByIndex(key.Index), cloneTokenValue(value)); err != nil {
					return out, err
				}
			}
		}
	} else {
		return out, fmt.Errorf("source phase update basis is unavailable")
	}
	key, ok := record.loadedKey(keyRow.Elem())
	if !ok {
		return out, fmt.Errorf("source phase captured identity is incomplete")
	}
	out.identity = key
	var matched OccurrenceRef
	matchedOrdinal := -1
	for _, root := range p.reconciliation.roots {
		for _, role := range root.roles {
			if role.role.relation.Child != record {
				continue
			}
			for _, ref := range role.current {
				currentKey, valid := record.loadedKey(ref.ticket.previous.Elem())
				if !valid || currentKey != key {
					continue
				}
				// Distinct bound occurrences with the same identity are a documented
				// activation gap. Never choose a first/last row implicitly here.
				if matchedOrdinal >= 0 && matchedOrdinal != ref.ticket.currentOrdinal {
					return out, fmt.Errorf("source phase Current identity is ambiguous; duplicate source fidelity pending")
				}
				matchedOrdinal = ref.ticket.currentOrdinal
				if ref.ticket.root == ticket.root && role.role.relation == phase.role {
					matched = ref
				}
			}
		}
	}
	if matched.ticket == nil {
		if phase.updateBasis == "current" || matchedOrdinal >= 0 {
			return out, fmt.Errorf("source phase selected identity requires authorized Current in its canonical parent")
		}
		return out, nil
	}
	current := true
	if _, err := p.reconciliation.resolve(matched, ticket.root, record, &current); err != nil {
		return out, err
	}
	if err := p.requireReconciliationCurrent(phase.role, ticket.root, matched.ticket.previous, true); err != nil {
		return out, err
	}
	out.kind = h.WriteUpdate
	out.current = matched
	out.basis = phase.updateBasis
	return out, nil
}
