package dml

import (
	"sort"

	xhandler "github.com/viant/xdatly/handler"
)

const (
	ComponentBinding    = "binding"
	ComponentImperative = "imperative"
)

type componentMarker struct {
	at    int
	child *Data
}

// ComponentData creates a private component view over the same root journal.
// It is consumed by Datly's engine through an optional internal capability;
// xdatly's public Data interface remains unchanged.
func (d *Data) ComponentData(relation, order string) xhandler.Data {
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	// A dispatcher can retain the context of a completed child invocation.
	// Repeated dispatch from that context creates a sibling under the nearest
	// open ancestor, matching the root unit-of-work frame semantics.
	for d != nil && !d.open && d.parent != nil {
		d = d.parent
	}
	child := &Data{
		root: owner, parent: d, relation: relation, order: order, open: true,
		db: owner.db, externalTx: owner.externalTx, onCommit: owner.onCommit,
		insertServices: owner.insertServices, updateServices: owner.updateServices,
		deleteServices: owner.deleteServices,
	}
	if relation == ComponentBinding {
		d.bindings = append(d.bindings, child)
		sort.SliceStable(d.bindings, func(i, j int) bool { return d.bindings[i].order < d.bindings[j].order })
	} else {
		d.markers = append(d.markers, componentMarker{at: len(d.queue), child: child})
	}
	return child
}

func (d *Data) SealComponent() {
	owner := d.owner()
	owner.mu.Lock()
	d.open = false
	owner.mu.Unlock()
}

func flattenData(frame *Data) []*dataOperation {
	if frame == nil {
		return nil
	}
	result := make([]*dataOperation, 0, len(frame.queue))
	for index := 0; index <= len(frame.queue); index++ {
		for _, marker := range frame.markers {
			if marker.at == index {
				result = append(result, flattenData(marker.child)...)
			}
		}
		if index < len(frame.queue) {
			result = append(result, frame.queue[index])
		}
	}
	for _, binding := range frame.bindings {
		result = append(result, flattenData(binding)...)
	}
	return result
}
