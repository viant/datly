package collector

import (
	"unsafe"

	sqlxio "github.com/viant/sqlx/io"
)

// Collector value-index ownership:
//   - indexing concrete row values by relation
//   - composite value indexing
//   - value-to-position mapping within the collector

func (r *Collector) indexCompositeValueByRel(ptr unsafe.Pointer, rel *Relation, counter int) {
	signature := relationCompositeSignature(rel.On)
	index := r.compositeValuePosition[signature]
	if index == nil {
		index = map[compositeKey][]int{}
		r.compositeValuePosition[signature] = index
	}
	valueSets := make([][]interface{}, 0, len(rel.On))
	for _, link := range rel.On {
		if link == nil || link.XField == nil {
			return
		}
		valueSets = append(valueSets, normalizeValues(link.XField.Value(ptr)))
	}
	for _, row := range compositeRows(valueSets) {
		index[buildCompositeKey(row)] = append(index[buildCompositeKey(row)], counter)
	}
}

func (r *Collector) indexValueByRel(fieldValue interface{}, rel *Relation, counter int) {
	switch actual := fieldValue.(type) {
	case []int:
		for _, v := range actual {
			r.indexValueToPosition(rel, v, counter)
		}
	case []*int64:
		for _, v := range actual {
			if v == nil {
				continue
			}
			r.indexValueToPosition(rel, int(*v), counter)
		}
	case []int64:
		for _, v := range actual {
			r.indexValueToPosition(rel, int(v), counter)
		}
	case int32:
		r.indexValueToPosition(rel, int(actual), counter)
	case *int64:
		if actual == nil {
			return
		}
		r.indexValueToPosition(rel, int(*actual), counter)
	case []string:
		for _, v := range actual {
			r.indexValueToPosition(rel, v, counter)
		}
	default:
		r.indexValueToPosition(rel, sqlxio.NormalizeKey(fieldValue), counter)
	}
}

func (r *Collector) indexValueToPosition(rel *Relation, fieldValue interface{}, counter int) {
	for _, item := range rel.On {
		columnValues, ok := r.valuePosition[item.Namespace]
		if !ok {
			columnValues = map[string]map[interface{}][]int{}
			r.valuePosition[item.Namespace] = columnValues
		}
		if _, ok := columnValues[item.Column]; !ok {
			columnValues[item.Column] = map[interface{}][]int{}
		}
		_, ok = columnValues[item.Column][fieldValue]
		if !ok {
			columnValues[item.Column][fieldValue] = []int{counter}
		} else {
			columnValues[item.Column][fieldValue] = append(columnValues[item.Column][fieldValue], counter)
		}
	}
}
