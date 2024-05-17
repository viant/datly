package state

import (
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/tags"
)

func BuildPredicate(aTag *tags.Tag, param *Parameter) {
	if aTag == nil || len(aTag.Predicates) == 0 {
		return
	}
	for _, pTag := range aTag.Predicates {
		pTag.Init(param.Name)
		param.Predicates = append(param.Predicates, &extension.PredicateConfig{
			Group:  pTag.Group,
			Name:   pTag.Name,
			Args:   pTag.Arguments,
			Ensure: pTag.Ensure})
	}
}
