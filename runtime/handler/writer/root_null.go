package writer

import (
	"fmt"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
)

// Only root collection elements qualify. A structural diagnostic captured
// before Input.Init is retained even if Init changes the collection afterwards.
func (p *Program) deferRootNull(record *Record, position int, indexed bool) bool {
	if record != p.metadata.Root || record.RootNullPolicy != "initial-validation" || !indexed {
		return false
	}
	if p.structuralError == nil {
		p.structuralError = &xhandler.RootNullRecordError{Location: p.rowLocation(record, nil, position, true), Cause: fmt.Errorf("writer row %d is nil", position)}
	}
	return true
}

// skipAuxiliaryNull applies only to an explicitly selected auxiliary collection.
// Keeping the original holders intact preserves positions and parent presence.
func (p *Program) skipAuxiliaryNull(record *Record, indexed bool) bool {
	if p.metadata == nil || record == nil || !record.Auxiliary || !indexed {
		return false
	}
	if record == p.metadata.Root {
		return record.RootNullPolicy == "skip-auxiliary"
	}
	return record.NestedNullPolicy == "skip-auxiliary"
}

// An earlier Init can clear a live auxiliary slot after its descendants were
// framed. Skip that subtree until the existing post-Init frame rebuild. Later
// mutation phases must reject topology changes rather than queue stale writes.
func (p *Program) skippedAuxiliaryAncestor(frame *Frame) *Frame {
	for current := frame; current != nil; current = current.Parent {
		live := p.liveFrameEntity(current)
		if (!live.IsValid() || live.Kind() == reflect.Pointer && live.IsNil()) && p.skipAuxiliaryNull(current.Record, true) {
			return current
		}
	}
	return nil
}

func validateRootNullPolicy(root *Record, inputType reflect.Type) error {
	var visit func(*Record, reflect.Type) error
	visit = func(record *Record, holder reflect.Type) error {
		if record.NestedNullPolicy != "" {
			valid := record != root && holder.Kind() == reflect.Slice && ((record.NestedNullPolicy == "initial-validation" && !record.Auxiliary) || (record.NestedNullPolicy == "skip-auxiliary" && record.Auxiliary))
			if !valid {
				if record.NestedNullPolicy == "skip-auxiliary" {
					return fmt.Errorf("nestedNullPolicy requires an auxiliary collection relation and skip-auxiliary")
				}
				return fmt.Errorf("nestedNullPolicy requires a writable collection relation and initial-validation")
			}
		}
		if record.RootNullPolicy != "" {
			valid := record == root && inputType.Kind() == reflect.Slice && ((record.RootNullPolicy == "initial-validation" && !record.Auxiliary) || (record.RootNullPolicy == "skip-auxiliary" && record.Auxiliary))
			if !valid {
				if record.RootNullPolicy == "skip-auxiliary" {
					return fmt.Errorf("rootNullPolicy requires an auxiliary root collection and skip-auxiliary")
				}
				return fmt.Errorf("rootNullPolicy requires a writable root collection and initial-validation")
			}
		}
		for _, relation := range record.Relations {
			if err := visit(relation.Child, record.EntityType.FieldByIndex(relation.Field).Type); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(root, inputType)
}

func (p *Program) deferRootNullFrame(frame *Frame) bool {
	if p.metadata == nil || frame.Record == nil || frame.Record != p.metadata.Root || frame.Record.RootNullPolicy != "initial-validation" {
		return false
	}
	// The policy is compiled only for a collection root.
	if p.structuralError == nil {
		p.structuralError = &xhandler.RootNullRecordError{Location: frame.Location, Cause: fmt.Errorf("writer row at %s is nil", frame.Location)}
	}
	return true
}

// Phase boundaries reject any late invalidated auxiliary subtree, including a
// previously visited sibling. This runs once per phase rather than per hook.
// Preserve Original, Previous and presence ownership by rejecting a frame
// detached from its live non-null entity; never silently rebind it.
func (p *Program) validateAuxiliaryFrameIdentity(frame *Frame) error {
	for current := frame; current != nil; current = current.Parent {
		if !p.skipAuxiliaryNull(current.Record, true) {
			continue
		}
		live := p.liveFrameEntity(current)
		if !live.IsValid() || live.Kind() == reflect.Pointer && live.IsNil() {
			continue
		}
		original := current.framedEntity
		if !current.holderTracked {
			original = current.Entity
		}
		if !original.IsValid() || original.Kind() != reflect.Pointer || original.IsNil() || !current.Entity.IsValid() || current.Entity.Kind() != reflect.Pointer || current.Entity.IsNil() || live.Kind() != reflect.Pointer || current.Entity.Pointer() != original.Pointer() || live.Pointer() != original.Pointer() {
			return fmt.Errorf("writer row at %s changed after framing", current.Location)
		}
	}
	return nil
}

func (p *Program) validateAuxiliaryTopology() error {
	if p.frames == nil {
		return nil
	}
	for _, frame := range p.frames.Rows {
		if invalid := p.skippedAuxiliaryAncestor(frame); invalid != nil {
			return fmt.Errorf("writer row at %s is nil", invalid.Location)
		}
		if err := p.validateAuxiliaryFrameIdentity(frame); err != nil {
			return err
		}
	}
	return nil
}

// Second-pass Init may clear a newly introduced auxiliary slot. Filter only
// those invalid subtrees before graph indexing; do not rerun hooks or rebuild
// indefinitely, and retain original request facts for the surviving graph.
func (p *Program) discardInitializedAuxiliaryNullFrames() error {
	if p.frames == nil {
		return nil
	}
	kept := p.frames.Rows[:0]
	for _, frame := range p.frames.Rows {
		if p.skippedAuxiliaryAncestor(frame) == nil {
			if err := p.validateAuxiliaryFrameIdentity(frame); err != nil {
				return err
			}
			kept = append(kept, frame)
		}
	}
	p.frames.Rows = kept
	return nil
}

// Frame slots can become detached when a hook replaces a collection. Resolve
// the current holder through the bounded parent chain and compiled field paths.
func (p *Program) liveFrameEntity(frame *Frame) reflect.Value {
	if frame == nil {
		return reflect.Value{}
	}
	if !frame.holderTracked {
		return frame.Entity
	}
	var holder reflect.Value
	if frame.Parent == nil {
		input := reflect.ValueOf(p.input)
		if !input.IsValid() || input.Kind() != reflect.Pointer || input.IsNil() {
			return reflect.Value{}
		}
		holder = input.Elem().Field(p.metadata.InputField)
	} else {
		parent := p.liveFrameEntity(frame.Parent)
		if !parent.IsValid() || parent.Kind() != reflect.Pointer || parent.IsNil() {
			return reflect.Value{}
		}
		relation := relationFor(frame.Parent.Record, frame.Record)
		if relation == nil {
			return reflect.Value{}
		}
		holder = parent.Elem().FieldByIndex(relation.Field)
	}
	if frame.holderIndexed {
		if holder.Kind() != reflect.Slice || frame.holderPosition >= holder.Len() {
			return reflect.Value{}
		}
		return holder.Index(frame.holderPosition)
	}
	return holder
}
