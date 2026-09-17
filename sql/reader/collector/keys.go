package collector

import (
	"fmt"
	"strings"

	sqlxio "github.com/viant/sqlx/io"
)

type compositeKey string

// Indexes may be shared only when links read the same key source. A SQL alias
// alone is insufficient: separate hooks may give it different typed values.
type relationIndexKey struct {
	namespace string
	column    string
	source    KeySource
	field     string
}

func relationIndexIdentity(link *Link) relationIndexKey {
	key := relationIndexKey{namespace: link.Namespace, column: link.Column, source: KeySourceColumn}
	if link.XField != nil {
		key.source = link.KeySource
		if key.source == "" {
			key.source = KeySourceField
		}
		key.field = link.XField.Name
	}
	return key
}

func relationCompositeSignature(links Links) string {
	parts := make([]string, 0, len(links))
	for _, link := range links {
		if link == nil {
			continue
		}
		key := relationIndexIdentity(link)
		parts = append(parts, fmt.Sprintf("%q:%q:%q:%q", key.namespace, key.column, key.source, key.field))
	}
	return strings.Join(parts, "|")
}

func buildCompositeKey(values []interface{}) compositeKey {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = fmt.Sprintf("%#v", sqlxio.NormalizeKey(value))
	}
	return compositeKey(strings.Join(parts, "\x1f"))
}

func normalizeValues(value interface{}) []interface{} {
	switch actual := value.(type) {
	case []int:
		result := make([]interface{}, 0, len(actual))
		for _, item := range actual {
			result = append(result, sqlxio.NormalizeKey(item))
		}
		return result
	case []*int64:
		result := make([]interface{}, 0, len(actual))
		for _, item := range actual {
			if item == nil {
				continue
			}
			result = append(result, sqlxio.NormalizeKey(int(*item)))
		}
		return result
	case []int64:
		result := make([]interface{}, 0, len(actual))
		for _, item := range actual {
			result = append(result, sqlxio.NormalizeKey(int(item)))
		}
		return result
	case []string:
		result := make([]interface{}, 0, len(actual))
		for _, item := range actual {
			result = append(result, sqlxio.NormalizeKey(item))
		}
		return result
	default:
		return []interface{}{sqlxio.NormalizeKey(value)}
	}
}

func compositeRows(parts [][]interface{}) [][]interface{} {
	if len(parts) == 0 {
		return nil
	}
	result := make([][]interface{}, 1)
	for _, values := range parts {
		if len(values) == 0 {
			return nil
		}
		next := make([][]interface{}, 0, len(result)*len(values))
		for _, existing := range result {
			for _, value := range values {
				row := append(append([]interface{}{}, existing...), value)
				next = append(next, row)
			}
		}
		result = next
	}
	return result
}

func (r *Collector) parentValuesPositions(link *Link) map[interface{}][]int {
	parent := r.parent
	key := relationIndexIdentity(link)
	parent.indexMutex.Lock()
	defer parent.indexMutex.Unlock()
	if !parent.preparedValuePosition[key] {
		parent.indexPositions(link)
		parent.preparedValuePosition[key] = true
	}
	return parent.valuePosition[key]
}

func (r *Collector) parentCompositePositions(relation *Relation) map[compositeKey][]int {
	parent := r.parent
	signature := relationCompositeSignature(relation.On)
	parent.indexMutex.Lock()
	defer parent.indexMutex.Unlock()
	if !parent.preparedComposite[signature] {
		parent.indexCompositePositions(relation)
		parent.preparedComposite[signature] = true
	}
	return parent.compositeValuePosition[signature]
}

func (r *Collector) prepareRelationIndexes() {
	if r == nil || r.indexMutex == nil {
		return
	}
	r.indexMutex.Lock()
	defer r.indexMutex.Unlock()
	for _, relation := range r.view.Relations {
		if relation == nil {
			continue
		}
		if relation.IsComposite() {
			signature := relationCompositeSignature(relation.On)
			if !r.preparedComposite[signature] {
				for _, link := range relation.On {
					if link != nil && link.XField == nil {
						r.indexCompositePositions(relation)
						break
					}
				}
				r.preparedComposite[signature] = true
			}
			continue
		}
		for _, link := range relation.On {
			if link == nil {
				continue
			}
			key := relationIndexIdentity(link)
			if r.preparedValuePosition[key] {
				continue
			}
			if link.XField == nil {
				r.indexPositions(link)
			}
			r.preparedValuePosition[key] = true
		}
	}
}
