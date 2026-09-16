package collector

import (
	"github.com/viant/xunsafe"
)

// ParentPlaceholders returns the deduplicated parent key values needed to
// parameterise the child SQL query. The three return values are:
//   - scalar values (single-column join)
//   - composite rows (multi-column join)
//   - column expressions for the SQL IN clause
func (r *Collector) ParentPlaceholders() ([]interface{}, [][]interface{}, []string) {
	if r.parent == nil || r.ReadAll() {
		return []interface{}{}, nil, nil
	}
	destPtr := xunsafe.AsPointer(r.parent.DestPtr())
	sliceLen := r.parent.slice.Len(destPtr)

	if r.relation.IsComposite() {
		result := make([][]interface{}, 0)
		unique := map[compositeKey]bool{}
		for i := 0; i < sliceLen; i++ {
			parent := r.parent.slice.ValuePointerAt(destPtr, i)
			valueSets := make([][]interface{}, 0, len(r.relation.On))
			for _, link := range r.relation.On {
				field := link.XField
				if field != nil {
					valueSets = append(valueSets, normalizeValues(field.Value(xunsafe.AsPointer(parent))))
					continue
				}
				value, ok := r.parent.sqlKeyAt(link.Column, i)
				if !ok {
					valueSets = nil
					break
				}
				valueSets = append(valueSets, normalizeValues(value))
			}
			for _, row := range compositeRows(valueSets) {
				key := buildCompositeKey(row)
				if unique[key] {
					continue
				}
				unique[key] = true
				result = append(result, row)
			}
		}
		return nil, result, r.relation.Of.On.InColumnExpression()
	}

	result := make([]interface{}, 0)
	unique := make(map[interface{}]bool)
outer:
	for i := 0; i < sliceLen; i++ {
		parent := r.parent.slice.ValuePointerAt(destPtr, i)
		for _, link := range r.relation.On {
			field := link.XField
			if field != nil {
				fieldValue := field.Value(xunsafe.AsPointer(parent))
				switch actual := fieldValue.(type) {
				case []*int64:
					for j := range actual {
						if _, ok := unique[int(*actual[j])]; ok {
							continue
						}
						unique[int(*actual[j])] = true
						result = append(result, int(*actual[j]))
					}
				case []int64:
					for j := range actual {
						if _, ok := unique[int(actual[j])]; ok {
							continue
						}
						unique[int(actual[j])] = true
						result = append(result, int(actual[j]))
					}
				case []int:
					for j := range actual {
						if _, ok := unique[actual[j]]; ok {
							continue
						}
						unique[actual[j]] = true
						result = append(result, actual[j])
					}
				case []string:
					for j := range actual {
						if _, ok := unique[actual[j]]; ok {
							continue
						}
						unique[actual[j]] = true
						result = append(result, actual[j])
					}
				default:
					if count := len(result); count > 0 {
						if result[count-1] == fieldValue {
							continue
						}
					}
					result = append(result, fieldValue)
				}
				continue outer
			}
			// Preserve SQL row order, as typed keys do, rather than map order.
			// Otherwise equivalent reads can produce different cache arguments.
			key, ok := r.parent.sqlKeyAt(link.Column, i)
			if !ok {
				continue outer
			}
			if _, ok := unique[key]; !ok {
				unique[key] = true
				result = append(result, key)
			}
			continue outer
		}
	}
	return result, nil, r.relation.Of.On.InColumnExpression()
}
