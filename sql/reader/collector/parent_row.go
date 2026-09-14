package collector

import (
	"fmt"

	"github.com/viant/xunsafe"
)

// ParentRow returns a function that resolves the parent struct pointer for a
// child row. Used by codec pipelines that need to reach back to the parent.
func (r *Collector) ParentRow() func(value interface{}) (interface{}, error) {
	relation := r.relation
	if relation == nil {
		return nil
	}
	links := relation.Of.On
	dest := r.parent.Dest()
	destPtr := xunsafe.AsPointer(dest)

	if len(links) == 1 {
		column := relation.On[0].Column
		namespace := relation.On[0].Namespace
		return func(child interface{}) (interface{}, error) {
			key, err := r.linkKeyAt(child, links[0], r.indexCounter)
			if err != nil {
				return nil, err
			}
			valuePosition := r.parentValuesPositions(namespace, column)
			positions, ok := valuePosition[key]
			if !ok {
				return nil, fmt.Errorf(`key "%v" is not found`, key)
			}
			if len(positions) > 1 {
				return nil, fmt.Errorf(`key "%v" has more than one value`, key)
			}
			return r.parent.slice.ValuePointerAt(destPtr, positions[0]), nil
		}
	}

	return func(child interface{}) (interface{}, error) {
		if relation.IsComposite() {
			keyParts := make([]interface{}, 0, len(links))
			for _, link := range links {
				key, err := r.linkKeyAt(child, link, r.indexCounter)
				if err != nil {
					return nil, err
				}
				keyParts = append(keyParts, key)
			}
			positions, ok := r.parentCompositePositions(relation)[buildCompositeKey(keyParts)]
			if !ok {
				return nil, fmt.Errorf(`composite key "%v" is not found`, keyParts)
			}
			if len(positions) > 1 {
				return nil, fmt.Errorf(`composite key "%v" has more than one value`, keyParts)
			}
			return r.parent.slice.ValuePointerAt(destPtr, positions[0]), nil
		}
		var key interface{}
		var parentPosition int
		for i, link := range links {
			var err error
			key, err = r.linkKeyAt(child, link, r.indexCounter)
			if err != nil {
				return nil, err
			}
			valuePosition := r.parentValuesPositions(relation.On[i].Namespace, relation.On[i].Column)
			positions, ok := valuePosition[key]
			if !ok {
				return nil, fmt.Errorf(`key "%v" is not found`, key)
			}
			if len(positions) > 1 {
				return nil, fmt.Errorf(`key "%v" has more than one value`, key)
			}
			parentPosition = positions[0]
		}
		return r.parent.slice.ValuePointerAt(destPtr, parentPosition), nil
	}
}
