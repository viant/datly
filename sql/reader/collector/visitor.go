package collector

import (
	"context"

	"github.com/viant/datly/spec"
)

// Visitor returns the compound VisitorFn to call once per scanned row.
func (r *Collector) Visitor(ctx context.Context) VisitorFn {
	relation := r.relation
	visitorRelations := Relations(r.view.Relations).PopulateWithVisitor()
	for _, rel := range visitorRelations {
		if rel.IsComposite() {
			signature := relationCompositeSignature(rel.On)
			if _, ok := r.compositeValuePosition[signature]; !ok {
				r.compositeValuePosition[signature] = map[compositeKey][]int{}
			}
			continue
		}
		for _, item := range rel.On {
			if _, ok := r.valuePosition[item.Namespace]; !ok {
				r.valuePosition[item.Namespace] = map[string]map[interface{}][]int{}
			}
			if _, ok := r.valuePosition[item.Namespace][item.Column]; !ok {
				r.valuePosition[item.Namespace][item.Column] = map[interface{}][]int{}
			}
		}
	}
	visitors := make([]VisitorFn, 1)
	visitors[0] = r.valueIndexer(ctx, visitorRelations)

	if relation != nil && !r.ReadAll() {
		switch relation.Cardinality {
		case spec.CardinalityOne:
			visitors = append(visitors, r.visitorOne(relation))
		case spec.CardinalityMany:
			visitors = append(visitors, r.visitorMany(relation))
		}
	}

	return func(value interface{}) error {
		if r.provenance {
			r.evidence(r.indexCounter)
		}
		for _, visitor := range visitors {
			if err := visitor(value); err != nil {
				return err
			}
		}
		return nil
	}
}
