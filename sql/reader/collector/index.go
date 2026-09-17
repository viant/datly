package collector

import (
	"context"

	"github.com/viant/xunsafe"
)

func (r *Collector) valueIndexer(_ context.Context, visitorRelations []*Relation) func(value interface{}) error {
	distinctRelations := make([]*Relation, 0)
	distinctLinks := make(Links, 0)
	presenceMap := map[relationIndexKey]bool{}
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
		for _, item := range visitorRelations[i].On {
			key := relationIndexIdentity(item)
			if presenceMap[key] {
				continue
			}
			distinctLinks = append(distinctLinks, item)
			presenceMap[key] = true
		}
	}

	return func(value interface{}) error {
		ptr := xunsafe.AsPointer(value)
		for _, rel := range distinctRelations {
			r.indexCompositeValueByRel(ptr, rel, r.indexCounter)
		}
		for _, link := range distinctLinks {
			if field := link.XField; field != nil {
				r.indexValueByLink(field.Value(ptr), link, r.indexCounter)
			}
		}
		r.indexCounter++
		return nil
	}
}
