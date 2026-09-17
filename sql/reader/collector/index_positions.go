package collector

import (
	"github.com/viant/xunsafe"
)

// Collector position-index ownership:
//   - indexing buffered parent/collector positions
//   - parent delegation for value/composite positions

func (r *Collector) indexPositions(link *Link) {
	values := r.values[link.Column]
	if values == nil {
		return
	}
	key := relationIndexIdentity(link)
	positions := r.valuePosition[key]
	if positions == nil {
		positions = map[interface{}][]int{}
		r.valuePosition[key] = positions
	}
	for position := range *values {
		val, ok := r.sqlKeyAt(link.Column, position)
		if !ok {
			continue
		}
		positions[val] = append(positions[val], position)
	}
}

func (r *Collector) indexCompositePositions(relation *Relation) {
	if relation == nil {
		return
	}
	signature := relationCompositeSignature(relation.On)
	index := r.compositeValuePosition[signature]
	if index == nil {
		index = map[compositeKey][]int{}
		r.compositeValuePosition[signature] = index
	}
	destPtr := xunsafe.AsPointer(r.DestPtr())
	for position := 0; position < r.slice.Len(destPtr); position++ {
		parent := r.slice.ValuePointerAt(destPtr, position)
		valueSets := make([][]interface{}, 0, len(relation.On))
		for _, link := range relation.On {
			if link == nil {
				valueSets = nil
				break
			}
			if link.XField != nil {
				valueSets = append(valueSets, normalizeValues(link.XField.Value(xunsafe.AsPointer(parent))))
				continue
			}
			value, ok := r.sqlKeyAt(link.Column, position)
			if !ok {
				valueSets = nil
				break
			}
			valueSets = append(valueSets, normalizeValues(value))
		}
		if valueSets == nil {
			continue
		}
		for _, row := range compositeRows(valueSets) {
			index[buildCompositeKey(row)] = append(index[buildCompositeKey(row)], position)
		}
	}
}
