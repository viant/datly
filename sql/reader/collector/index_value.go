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

func (r *Collector) indexValueByLink(fieldValue interface{}, link *Link, counter int) {
	switch actual := fieldValue.(type) {
	case []int:
		for _, v := range actual {
			r.indexValueToPosition(link, v, counter)
		}
	case []*int64:
		for _, v := range actual {
			if v == nil {
				continue
			}
			r.indexValueToPosition(link, int(*v), counter)
		}
	case []int64:
		for _, v := range actual {
			r.indexValueToPosition(link, int(v), counter)
		}
	case int32:
		r.indexValueToPosition(link, int(actual), counter)
	case *int64:
		if actual == nil {
			return
		}
		r.indexValueToPosition(link, int(*actual), counter)
	case []string:
		for _, v := range actual {
			r.indexValueToPosition(link, v, counter)
		}
	default:
		r.indexValueToPosition(link, sqlxio.NormalizeKey(fieldValue), counter)
	}
}

func (r *Collector) indexValueToPosition(link *Link, fieldValue interface{}, counter int) {
	key := relationIndexIdentity(link)
	positions := r.valuePosition[key]
	if positions == nil {
		positions = map[interface{}][]int{}
		r.valuePosition[key] = positions
	}
	positions[fieldValue] = append(positions[fieldValue], counter)
}
