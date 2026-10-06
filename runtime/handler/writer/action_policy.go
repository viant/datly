package writer

import (
	"fmt"
	"reflect"

	xhandler "github.com/viant/xdatly/handler"
)

const insertDeleteActionPolicy = "insert-delete"

func isPolicyInsert(frame *Frame) bool {
	return frame != nil && frame.Record != nil && frame.Record.WriterActionPolicy == insertDeleteActionPolicy && frame.Action == xhandler.WriteInsert
}

func validateWriterActionPolicy(record *Record, operation string) error {
	if record == nil {
		return nil
	}
	if hasWriterActionPolicy(record) && hasScopedSequences(record) {
		return fmt.Errorf("writerActionPolicy insert-delete does not support graph-wide scoped recovery")
	}
	if record.WriterActionPolicy != "" {
		if record.WriterActionPolicy != insertDeleteActionPolicy {
			return fmt.Errorf("writerActionPolicy must be insert-delete")
		}
		if operation != "patch" || record.Auxiliary || record.Table == "" || record.CurrentField < 0 || len(record.Keys) == 0 || record.DeleteMarker == nil {
			return fmt.Errorf("writerActionPolicy insert-delete requires a PATCH physical leaf with Current, keys and delete marker")
		}
		if len(record.Relations) != 0 {
			return fmt.Errorf("writerActionPolicy insert-delete requires a leaf role")
		}
		if record.WriterIdentityPolicy != "" || record.ConcurrencyToken != nil || record.MutationPredicateGroup != nil || len(record.ScopedSequences) != 0 {
			return fmt.Errorf("writerActionPolicy insert-delete does not support identity overrides, concurrency tokens, mutation predicates or scoped recovery")
		}
	}
	for _, relation := range record.Relations {
		if err := validateWriterActionPolicy(relation.Child, operation); err != nil {
			return err
		}
	}
	// Root recovery can replay descendant actions; reject its combination with
	// any opted-in role until that combination has a separately proven contract.
	if hasWriterActionPolicy(record) && record.HookType != nil {
		for _, name := range []string{"Recover", "RetryTransaction"} {
			if _, found := reflect.PointerTo(record.HookType).MethodByName(name); found {
				return fmt.Errorf("writerActionPolicy insert-delete does not support %s recovery", name)
			}
		}
	}
	return nil
}

func hasWriterActionPolicy(record *Record) bool {
	if record == nil {
		return false
	}
	if record.WriterActionPolicy != "" {
		return true
	}
	for _, relation := range record.Relations {
		if hasWriterActionPolicy(relation.Child) {
			return true
		}
	}
	return false
}

// Explicit replacement permits one DELETE and one INSERT in a single role;
// physical identity is never shared between distinct graph roles.
func (p *Program) validateActionPolicyActions() error {
	if p == nil || p.metadata == nil || !hasWriterActionPolicy(p.metadata.Root) {
		return nil
	}
	type physical struct {
		table string
		key   identityKey
	}
	type association struct {
		record *Record
		mask   uint8
	}
	seen := map[physical]association{}
	for _, action := range p.actions.Rows {
		frame := p.actionFrame(action)
		if frame == nil || frame.Record == nil {
			return fmt.Errorf("writer action has no authoritative frame")
		}
		record := frame.Record
		if record.Auxiliary {
			continue
		}
		key, complete := record.key(action.Entity.Elem())
		if !complete {
			continue
		}
		identity := physical{table: record.Table, key: key}
		prior, found := seen[identity]
		mask := uint8(4)
		if action.Kind == xhandler.WriteDelete {
			mask = 1
		} else if action.Kind == xhandler.WriteInsert {
			mask = 2
		}
		if found {
			if prior.record != record {
				return fmt.Errorf("physical identity collides between writer roles")
			}
			if record.WriterActionPolicy != insertDeleteActionPolicy || prior.mask&mask != 0 || prior.mask|mask != 3 {
				return fmt.Errorf("duplicate physical writer action in role %s", record.Path)
			}
			mask |= prior.mask
		}
		seen[identity] = association{record: record, mask: mask}
	}
	return nil
}

// Policy facts belong to a record role and an entity, independently of mutable
// markers. Packed scalar keys do not retain the caller's pointer aliases.
type actionPolicyKey struct {
	value           identityKey
	valid, resolved bool
}
type actionPolicyFacts struct {
	owner           *Frame
	keys            map[string]actionPolicyKey
	deleteRequested bool
}

func policyKeyValue(value reflect.Value) actionPolicyKey {
	if isNil(value) {
		return actionPolicyKey{}
	}
	return actionPolicyKey{value: scalarKey(indirect(value)), valid: true, resolved: linkValueResolved(value)}
}
func policyDeleteRequested(record *Record, entity reflect.Value) bool {
	return record.DeleteMarker != nil && supplied(entity, *record.DeleteMarker) && boolValue(entity.FieldByIndex(record.DeleteMarker.Index))
}
func (p *Program) captureActionPolicyFacts(record *Record, entity reflect.Value) {
	if record.WriterActionPolicy == insertDeleteActionPolicy {
		p.capturePhysicalActionFacts(record, entity)
	}
}
func (p *Program) capturePhysicalActionFacts(record *Record, entity reflect.Value) *actionPolicyFacts {
	if p.actionPolicyFacts == nil {
		p.actionPolicyFacts = map[frameIdentity]*actionPolicyFacts{}
	}
	identity := frameIdentity{record: record, pointer: entity.Pointer()}
	if existing := p.actionPolicyFacts[identity]; existing != nil {
		return existing
	}
	facts := &actionPolicyFacts{keys: map[string]actionPolicyKey{}, deleteRequested: policyDeleteRequested(record, entity.Elem())}
	for _, key := range record.Keys {
		facts.keys[key.Name] = policyKeyValue(entity.Elem().FieldByIndex(key.Index))
	}
	p.actionPolicyFacts[identity] = facts
	return facts
}
func (p *Program) captureActionPolicyOwner(frame *Frame) {
	if frame.Record.WriterActionPolicy != insertDeleteActionPolicy {
		return
	}
	facts := p.capturePhysicalActionFacts(frame.Record, frame.Entity)
	if facts.owner == nil {
		owned := *frame
		owned.Entity = reflect.ValueOf(frame.Entity.Interface())
		owned.framedEntity = owned.Entity
		facts.owner = &owned
	}
}

// Default-policy rows retain their normal initialization/allocation behavior.
// Once they join a graph's physical actions, their collision facts are frozen
// alongside the opted-in rows before queue observers can run.
func (p *Program) freezeActionPolicyParticipants() {
	if p.metadata == nil || !hasWriterActionPolicy(p.metadata.Root) {
		return
	}
	for _, action := range p.actions.Rows {
		frame := p.actionFrame(action)
		if frame == nil || frame.Record.Auxiliary {
			continue
		}
		facts := p.capturePhysicalActionFacts(frame.Record, frame.Entity)
		if facts.owner == nil {
			owned := *frame
			owned.Entity = reflect.ValueOf(frame.Entity.Interface())
			owned.framedEntity = owned.Entity
			facts.owner = &owned
		}
	}
}
func (p *Program) validateActionPolicyFrameFacts(frame *Frame) error {
	if frame == nil || frame.Record == nil {
		return nil
	}
	facts := p.actionPolicyFacts[identityOfFrame(frame)]
	if facts == nil {
		if frame.Record.WriterActionPolicy != insertDeleteActionPolicy {
			return nil
		}
		return fmt.Errorf("writer role %s has no captured physical facts", frame.Record.Path)
	}
	for _, key := range frame.Record.Keys {
		if facts.keys[key.Name] != policyKeyValue(frame.Entity.Elem().FieldByIndex(key.Index)) {
			return fmt.Errorf("writer role %s captured identity %s changed", frame.Record.Path, key.Name)
		}
	}
	if facts.deleteRequested != policyDeleteRequested(frame.Record, frame.Entity.Elem()) {
		return fmt.Errorf("writer role %s captured deletion decision changed", frame.Record.Path)
	}
	return nil
}
func (p *Program) validateActionPolicyFacts() error {
	for _, facts := range p.actionPolicyFacts {
		frame := facts.owner
		if frame == nil {
			continue
		}
		if p.skippedAuxiliaryAncestor(frame) != nil {
			continue
		}
		for owner := frame; owner != nil; owner = owner.Parent {
			live := p.liveFrameEntity(owner)
			original := owner.framedEntity
			if !original.IsValid() || original.Kind() != reflect.Pointer || original.IsNil() || !live.IsValid() || live.Kind() != reflect.Pointer || live.IsNil() || live.Pointer() != original.Pointer() {
				return fmt.Errorf("writer row at %s changed after captured framing", owner.Location)
			}
		}
		if err := p.validateActionPolicyFrameFacts(frame); err != nil {
			return err
		}
	}
	return nil
}

// Only a native producer can advance an unresolved key. Hooks never recapture
// working facts, and a supplied nonzero key cannot be overwritten by a producer.
func (p *Program) advanceActionPolicyKey(frame *Frame, key Field) error {
	if frame.Record.WriterActionPolicy != insertDeleteActionPolicy {
		return nil
	}
	facts := p.actionPolicyFacts[identityOfFrame(frame)]
	if facts == nil {
		return fmt.Errorf("writer role %s has no captured physical facts", frame.Record.Path)
	}
	previous, exists := facts.keys[key.Name]
	if !exists {
		return nil
	}
	current := policyKeyValue(frame.Entity.Elem().FieldByIndex(key.Index))
	if current == previous {
		return nil
	}
	if previous.resolved || facts.deleteRequested {
		return fmt.Errorf("writer role %s captured identity %s changed by producer", frame.Record.Path, key.Name)
	}
	facts.keys[key.Name] = current
	return nil
}
