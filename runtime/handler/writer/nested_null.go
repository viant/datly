package writer

import (
	"fmt"
	xhandler "github.com/viant/xdatly/handler"
)

// The declaration applies to one exact collection relation, never descendants.
// Capture order determines the retained diagnostic; later initialization cannot erase it.
func (p *Program) deferNestedNull(record *Record, location string, indexed bool) bool {
	if record == nil || record == p.metadata.Root || record.NestedNullPolicy != "initial-validation" || !indexed {
		return false
	}
	if p.structuralError == nil {
		p.structuralError = &xhandler.NestedNullRecordError{Location: location, Cause: fmt.Errorf("writer row at %s is nil", location)}
	}
	return true
}
