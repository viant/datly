package config

import (
	"fmt"
	"github.com/viant/xdatly/predicate"
)

const PredicateEqual = "equal"
const PredicateNotEqual = "not_equal"
const PredicateIn = "in"
const PredicateNotIn = "not_in"

type (
	PredicateRegistry struct {
		parent   *PredicateRegistry
		registry map[string]*predicate.Template
	}
	PredicateConfig struct {
		Parent  string
		Name    string
		Context int
		Ensure  bool
		Args    []string
	}

	NamedArg struct {
		Position int
		Name     string
	}
)

func (r *PredicateRegistry) Lookup(name string) (*predicate.Template, error) {
	result, ok := r.registry[name]
	if ok {
		return result, nil
	}

	if r.parent != nil {
		return r.parent.Lookup(name)
	}

	return nil, fmt.Errorf("not found template %v", name)
}

func (r *PredicateRegistry) Scope() *PredicateRegistry {
	registry := NewPredicates()
	registry.parent = r
	return registry
}

func (r *PredicateRegistry) Add(template *predicate.Template) {
	r.registry[template.Name] = template
}

func NewPredicates() *PredicateRegistry {
	return &PredicateRegistry{
		parent:   nil,
		registry: map[string]*predicate.Template{},
	}
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
		Source: " ${Alias}.${ColumnName} " + negation + "=  $criteria.AppendBinding($FilterValue)",
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

func NewInPredicate() *predicate.Template {
	return newInPredicate(PredicateIn, true)
}

func NewNotInPredicate() *predicate.Template {
	return newInPredicate(PredicateNotIn, false)
}

func newInPredicate(name string, equal bool) *predicate.Template {
	column := `${Alias} + "." + ${ColumnName}`
	in := fmt.Sprintf(`$criteria.In(%v, $FilterValue)`, column)

	if !equal {
		in = fmt.Sprintf(`$criteria.NotIn(%v, $FilterValue)`, column)
	}

	return &predicate.Template{
		Name:   name,
		Source: " " + in,
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
