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

func validateRootNullPolicy(root *Record, inputType reflect.Type) error {
	var visit func(*Record, reflect.Type) error
	visit = func(record *Record, holder reflect.Type) error {
		if record.NestedNullPolicy != "" && (record == root || record.Auxiliary || holder.Kind() != reflect.Slice || record.NestedNullPolicy != "initial-validation") {
			return fmt.Errorf("nestedNullPolicy requires a writable collection relation and initial-validation")
		}
		if record.RootNullPolicy != "" && (record != root || record.Auxiliary || inputType.Kind() != reflect.Slice || record.RootNullPolicy != "initial-validation") {
			return fmt.Errorf("rootNullPolicy requires a writable root collection and initial-validation")
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
