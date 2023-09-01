package state

import (
	"github.com/viant/datly/config"
	"github.com/viant/datly/view/state/predicate"
	"reflect"
)

func BuildPredicate(fieldTag reflect.StructTag, param *Parameter) {
	predicateTagLiteral, _ := fieldTag.Lookup(predicate.TagName)
	predicateTag := predicate.ParseTag(predicateTagLiteral, param.Name)
	if predicateTag.Predicate != "" {
		param.Predicates = append(param.Predicates, &config.PredicateConfig{
			Group: predicateTag.Group,
			Name:  predicateTag.Predicate,
			Args:  predicateTag.Args})
	}
}
