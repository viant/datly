package collector

import (
	"fmt"
	"reflect"

	"github.com/viant/xunsafe"
)

// attachmentTargets keeps the existing positional join index bound to the
// parent objects it indexed. Hook-time rebinds must not reinterpret those
// positions against a collection that a hook has reordered or removed.
type attachmentTargets struct {
	parent *Collector
	frozen []any
	hooked bool
}

func (r *Collector) attachmentTargets(snapshot *hookTopology) attachmentTargets {
	result := attachmentTargets{parent: r.parent}
	if snapshot != nil {
		result.hooked = true
		result.frozen = snapshot.targets[r.parent]
	}
	return result
}
func (t attachmentTargets) len() int {
	if t.hooked {
		return len(t.frozen)
	}
	return t.parent.Len()
}
func (t attachmentTargets) at(index int) (any, error) {
	if index < 0 || index >= t.len() {
		return nil, fmt.Errorf("relation parent position %d is outside its indexed targets", index)
	}
	var row any
	if t.hooked {
		row = t.frozen[index]
	} else {
		row = t.parent.slice.ValuePointerAt(xunsafe.AsPointer(t.parent.DestPtr()), index)
	}
	if row == nil || reflect.ValueOf(row).Kind() == reflect.Pointer && reflect.ValueOf(row).IsNil() {
		return nil, nil
	}
	return row, nil
}
