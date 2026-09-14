package collector

import (
	"context"

	"github.com/viant/xunsafe"
)

func (r *Collector) valueIndexer(_ context.Context, visitorRelations []*Relation) func(value interface{}) error {
	distinctRelations := make([]*Relation, 0)
	presenceMap := map[string]map[string]bool{}
	compositePresence := map[string]bool{}

	for i := range visitorRelations {
		if visitorRelations[i].IsComposite() {
			signature := relationCompositeSignature(visitorRelations[i].On)
			if compositePresence[signature] {
				continue
			}
			distinctRelations = append(distinctRelations, visitorRelations[i])
			compositePresence[signature] = true
			continue
		}
		appended := false
		for _, item := range visitorRelations[i].On {
			if _, ok := presenceMap[item.Namespace]; !ok {
				presenceMap[item.Namespace] = map[string]bool{}
			}
			if _, ok := presenceMap[item.Namespace][item.Column]; ok {
				continue
			}
			if !appended {
				distinctRelations = append(distinctRelations, visitorRelations[i])
				appended = true
			}
			presenceMap[item.Namespace][item.Column] = true
		}
	}

	return func(value interface{}) error {
		ptr := xunsafe.AsPointer(value)
		for _, rel := range distinctRelations {
			if rel.IsComposite() {
				r.indexCompositeValueByRel(ptr, rel, r.indexCounter)
				continue
			}
			for _, link := range rel.On {
				if field := link.XField; field != nil {
					fieldValue := field.Value(ptr)
					r.indexValueByRel(fieldValue, rel, r.indexCounter)
				}
			}
		}
		r.indexCounter++
		return nil
	}
}
