package config

import (
	"fmt"
	"github.com/viant/xdatly/predicate"
	"sync"
)

const PredicateEqual = "equal"
const PredicateNotEqual = "not_equal"
const PredicateIn = "in"
const PredicateMultiIn = "multi_in"
const PredicateNotIn = "not_in"
const PredicateMultiNotIn = "multi_not_in"
const PredicateLessOrEqual = "less_or_equal"

type (
	PredicateRegistry struct {
		sync.Mutex
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
	return binaryPredicate(PredicateEqual, "=")
}

func NewLessOrEqualPredicate() *predicate.Template {
	return binaryPredicate(PredicateLessOrEqual, "<=")
}

func NewNotEqualPredicate() *predicate.Template {
	return binaryPredicate(PredicateNotEqual, "!=")
}

func NewInPredicate() *predicate.Template {
	return newInPredicate(PredicateIn, true, false)
}

func NewMultiInPredicate() *predicate.Template {
	return newInPredicate(PredicateIn, true, true)
}

func NewMultiNotInPredicate() *predicate.Template {
	return newInPredicate(PredicateIn, false, true)
}

func NewNotInPredicate() *predicate.Template {
	return newInPredicate(PredicateNotIn, false, false)
}

func newInPredicate(name string, equal bool, multi bool) *predicate.Template {
	args := []*predicate.NamedArgument{
		{
			Name:     "Alias",
			Position: 0,
		},
	}

	column := `${Alias}`
	if !multi {
		column += `+ "." + ${ColumnName}`
		args = append(args, &predicate.NamedArgument{
			Name:     "ColumnName",
			Position: 1,
		})
	}

	in := fmt.Sprintf(`$criteria.In(%v, $FilterValue)`, column)

	if !equal {
		in = fmt.Sprintf(`$criteria.NotIn(%v, $FilterValue)`, column)
	}

	return &predicate.Template{
		Name:   name,
		Source: " " + in,
		Args:   args,
	}
}

func binaryPredicate(name, operator string) *predicate.Template {
	return &predicate.Template{
		Name:   name,
		Source: " ${Alias}.${ColumnName} " + operator + " $criteria.AppendBinding($FilterValue)",
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
