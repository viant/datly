package collector

import (
	"context"
	"reflect"

	"github.com/viant/xunsafe"
)

// BootstrapFromParentHolder seeds this collector from already-materialised
// parent holder data. Useful when parent OnFetch hooks populate a virtual
// relation in-memory and nested collectors still need a real source.
func (r *Collector) BootstrapFromParentHolder() bool {
	if r == nil || r.parent == nil || r.relation == nil || r.relation.HolderField == nil {
		return false
	}
	if r.Len() > 0 {
		return false
	}
	parentPtr := xunsafe.AsPointer(r.parent.DestPtr())
	if parentPtr == nil {
		return false
	}
	parentLen := r.parent.slice.Len(parentPtr)
	if parentLen == 0 {
		return false
	}

	visitorRelations := Relations(r.view.Relations).PopulateWithVisitor()
	indexer := r.valueIndexer(context.Background(), visitorRelations)
	appended := 0

	for i := 0; i < parentLen; i++ {
		parentItem := r.parent.slice.ValuePointerAt(parentPtr, i)
		if parentItem == nil {
			continue
		}
		holderValue := r.relation.HolderField.Value(xunsafe.AsPointer(parentItem))
		appended += r.appendBootstrapHolder(holderValue, indexer)
	}
	return appended > 0
}

func (r *Collector) appendBootstrapHolder(holderValue interface{}, indexer func(value interface{}) error) int {
	if holderValue == nil {
		return 0
	}
	value := reflect.ValueOf(holderValue)
	if !value.IsValid() {
		return 0
	}
	for value.Kind() == reflect.Interface || value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return 0
		}
		value = value.Elem()
	}
	switch value.Kind() {
	case reflect.Slice, reflect.Array:
		appended := 0
		for i := 0; i < value.Len(); i++ {
			item := value.Index(i)
			if !item.IsValid() {
				continue
			}
			if item.Kind() == reflect.Ptr && item.IsNil() {
				continue
			}
			r.appender.Append(item.Interface())
			if r.provenance {
				r.evidence(r.indexCounter)
			}
			_ = indexer(item.Interface())
			appended++
		}
		return appended
	default:
		r.appender.Append(holderValue)
		if r.provenance {
			r.evidence(r.indexCounter)
		}
		_ = indexer(holderValue)
		return 1
	}
}
