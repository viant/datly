package collector

import (
	"fmt"
	"github.com/viant/datly/spec"
	"reflect"
)

// ValidateToOne checks actual fetched keys using the collector's canonical key
// accessors. Opt-in projection graphs use it to reject ambiguous labels; legacy
// reader attachment behavior is unchanged.
func (r *Collector) ValidateToOne() error {
	for _, child := range r.relations {
		relation := child.relation
		if relation != nil && relation.Cardinality == spec.CardinalityOne {
			seen := map[compositeKey]bool{}
			rows := reflect.ValueOf(child.Dest())
			for i := 0; i < rows.Len(); i++ {
				row := rows.Index(i).Interface()
				keys := make([]any, 0, len(relation.Of.On))
				for _, link := range relation.Of.On {
					key, err := child.linkKeyAt(row, link, i)
					if err != nil {
						return err
					}
					keys = append(keys, key)
				}
				key := buildCompositeKey(keys)
				if seen[key] {
					return fmt.Errorf("to-one relation %s returned duplicate keys", relation.Name)
				}
				seen[key] = true
			}
		}
		if err := child.ValidateToOne(); err != nil {
			return err
		}
	}
	return nil
}
