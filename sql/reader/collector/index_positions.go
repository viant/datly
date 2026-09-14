package collector

import (
	sqlxio "github.com/viant/sqlx/io"
	"github.com/viant/xunsafe"
)

// Collector position-index ownership:
//   - indexing buffered parent/collector positions
//   - parent delegation for value/composite positions

func (r *Collector) indexPositions(ns, name string) {
	values := r.values[name]
	if values == nil {
		return
	}
	xType := r.types[name]
	columnValues, ok := r.valuePosition[ns]
	if !ok {
		columnValues = map[string]map[interface{}][]int{}
		r.valuePosition[ns] = columnValues
	}
	if _, ok := columnValues[name]; !ok {
		columnValues[name] = map[interface{}][]int{}
	}
	for position, v := range *values {
		if v == nil {
			continue
		}
		val := xType.Deref(v)
		val = sqlxio.NormalizeKey(val)
		_, ok := columnValues[name][val]
		if !ok {
			columnValues[name][val] = make([]int, 0)
		}
		columnValues[name][val] = append(columnValues[name][val], position)
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
			values := r.values[link.Column]
			if values == nil || position >= len(*values) {
				valueSets = nil
				break
			}
			xType := r.types[link.Column]
			if xType == nil {
				valueSets = nil
				break
			}
			valueSets = append(valueSets, normalizeValues(xType.Deref((*values)[position])))
		}
		if valueSets == nil {
			continue
		}
		for _, row := range compositeRows(valueSets) {
			index[buildCompositeKey(row)] = append(index[buildCompositeKey(row)], position)
		}
	}
}
