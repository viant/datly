package collector

import "github.com/viant/xunsafe"

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
				values := r.parent.values[link.Column]
				valueType := r.parent.types[link.Column]
				if values == nil || valueType == nil || i >= len(*values) {
					valueSets = nil
					break
				}
				valueSets = append(valueSets, normalizeValues(valueType.Deref((*values)[i])))
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
		for k, link := range r.relation.On {
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
			positions := r.parentValuesPositions(r.relation.On[k].Namespace, r.relation.On[k].Column)
			for key := range positions {
				if _, ok := unique[key]; ok {
					continue
				}
				unique[key] = true
				result = append(result, key)
			}
			continue outer
		}
	}
	return result, nil, r.relation.Of.On.InColumnExpression()
}
