package config

import (
	"fmt"
	"github.com/viant/xdatly/predicate"
)

const PredicateEqual = "equal"
const PredicateNotEqual = "not_equal"

type (
	PredicateRegistry map[string]*predicate.Template
	PredicateConfig   struct {
		Parent  string
		Name    string
		Context int
		Args    []*predicate.NamedArgument
	}

	NamedArg struct {
		Position int
		Name     string
	}
)

func (r PredicateRegistry) Lookup(name string) (*predicate.Template, error) {
	result, ok := r[name]
	if !ok {
		return nil, fmt.Errorf("not found template %v", name)
	}

	return result, nil
}

func (r PredicateRegistry) Clone() PredicateRegistry {
	result := PredicateRegistry{}
	for key, template := range r {
		result[key] = template
	}

	return result
}

func NewEqualPredicate() *predicate.Template {
	return equalityCheckPredicate(PredicateEqual, true)
}

func equalityCheckPredicate(name string, equal bool) *predicate.Template {
	var negation string
	if !equal {
		negation = "!"
	}

	return &predicate.Template{
		Name:   name,
		Source: " ${Alias}.${ColumnName} " + negation + "= $criteria.AppendBinding($FilterValue)",
		Args: []*predicate.NamedArgument{
			{
				Name:     "Alias",
				Position: 0,
			},
			{
				Name:     "ColumnName",
				Position: 1,
			},
		},
	}
}

func NewNotEqualPredicate() *predicate.Template {
	return equalityCheckPredicate(PredicateNotEqual, false)
}
