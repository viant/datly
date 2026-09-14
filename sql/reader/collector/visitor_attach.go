package collector

import (
	"fmt"

	"github.com/viant/xunsafe"
)

func (r *Collector) visitorOne(relation *Relation) func(value interface{}) error {
	links := relation.Of.On
	holderField := relation.HolderField
	dest := r.parent.Dest()
	destPtr := xunsafe.AsPointer(dest)
	return func(owner interface{}) error {
		position := r.indexCounter - 1
		if relation.IsComposite() {
			keyParts := make([]interface{}, 0, len(links))
			for _, link := range links {
				key, err := r.linkKeyAt(owner, link, position)
				if err != nil {
					return fmt.Errorf("resolve relation %s: %w", relation.Name, err)
				}
				keyParts = append(keyParts, key)
			}
			positions, ok := r.parentCompositePositions(relation)[buildCompositeKey(keyParts)]
			if !ok {
				return nil
			}
			for _, index := range positions {
				item := r.parent.slice.ValuePointerAt(destPtr, index)
				holderField.SetValue(xunsafe.AsPointer(item), owner)
				r.attachEvidence(index, position)
			}
			return nil
		}
		for j, link := range links {
			aKey, err := r.linkKeyAt(owner, link, position)
			if err != nil {
				return fmt.Errorf("resolve relation %s: %w", relation.Name, err)
			}

			parentLink := relation.On[j]
			valuePosition := r.parentValuesPositions(parentLink.Namespace, parentLink.Column)
			positions, ok := valuePosition[aKey]
			if !ok {
				return nil
			}
			for _, index := range positions {
				item := r.parent.slice.ValuePointerAt(destPtr, index)
				holderField.SetValue(xunsafe.AsPointer(item), owner)
				r.attachEvidence(index, position)
			}
		}
		return nil
	}
}

func (r *Collector) visitorMany(relation *Relation) func(value interface{}) error {
	links := relation.Of.On
	holderField := relation.HolderField
	dest := r.parent.Dest()
	destPtr := xunsafe.AsPointer(dest)

	return func(owner interface{}) error {
		position := r.indexCounter - 1
		if relation.IsComposite() {
			keyParts := make([]interface{}, 0, len(links))
			for _, link := range links {
				key, err := r.linkKeyAt(owner, link, position)
				if err != nil {
					return fmt.Errorf("resolve relation %s: %w", relation.Name, err)
				}
				keyParts = append(keyParts, key)
			}
			positions, ok := r.parentCompositePositions(relation)[buildCompositeKey(keyParts)]
			if !ok {
				return nil
			}
			for _, index := range positions {
				parentItem := r.parent.slice.ValuePointerAt(destPtr, index)
				r.Lock().Lock()
				sliceAddPtr := holderField.Pointer(xunsafe.AsPointer(parentItem))
				holderSlice := relation.HolderSlice
				if holderSlice == nil {
					holderSlice = xunsafe.NewSlice(holderField.Type)
				}
				appender := holderSlice.Appender(sliceAddPtr)
				appendRelationHolderValue(appender, holderField.Type, owner)
				r.Lock().Unlock()
				r.attachEvidence(index, position)
			}
			return nil
		}
		var key interface{}
		for i, link := range links {
			var err error
			key, err = r.linkKeyAt(owner, link, position)
			if err != nil {
				return fmt.Errorf("resolve relation %s: %w", relation.Name, err)
			}
			valuePosition := r.parentValuesPositions(relation.On[i].Namespace, relation.On[i].Column)
			positions, ok := valuePosition[key]
			if !ok {
				return nil
			}
			for _, index := range positions {
				parentItem := r.parent.slice.ValuePointerAt(destPtr, index)
				r.Lock().Lock()
				sliceAddPtr := holderField.Pointer(xunsafe.AsPointer(parentItem))
				holderSlice := relation.HolderSlice
				if holderSlice == nil {
					holderSlice = xunsafe.NewSlice(holderField.Type)
				}
				appender := holderSlice.Appender(sliceAddPtr)
				appendRelationHolderValue(appender, holderField.Type, owner)
				r.Lock().Unlock()
				r.attachEvidence(index, position)
			}
		}
		return nil
	}
}
