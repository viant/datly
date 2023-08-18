package config

import (
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/predicate"
	"github.com/viant/xreflect"
	"sync"
)

const (
	PredicateEqual       = "equal"
	PredicateNotEqual    = "not_equal"
	PredicateIn          = "in"
	PredicateMultiIn     = "multi_in"
	PredicateNotIn       = "not_in"
	PredicateMultiNotIn  = "multi_not_in"
	PredicateLessOrEqual = "less_or_equal"
	PredicateLike        = "like"
	PredicateNotLike     = "not_like"
	PredicateHandler     = "handler"
)

type (
	PredicateRegistry struct {
		sync.Mutex
		parent   *PredicateRegistry
		registry map[string]*Predicate
	}

	Predicate struct {
		Template *predicate.Template
		Handler  *PredicateHandlerFactory
	}

	PredicateHandlerFactory struct{}

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

func (r *PredicateRegistry) Lookup(name string) (*Predicate, error) {
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
	r.registry[template.Name] = &Predicate{
		Template: template,
	}
}

func NewPredicates() *PredicateRegistry {
	return &PredicateRegistry{
		parent:   nil,
		registry: map[string]*Predicate{},
	}
}

func NewEqualPredicate() *Predicate {
	return binaryPredicate(PredicateEqual, "=")
}

func NewLessOrEqualPredicate() *Predicate {
	return binaryPredicate(PredicateLessOrEqual, "<=")
}

func NewNotEqualPredicate() *Predicate {
	return binaryPredicate(PredicateNotEqual, "!=")
}

func NewInPredicate() *Predicate {
	return newInPredicate(PredicateIn, true, false)
}

func NewMultiInPredicate() *Predicate {
	return newInPredicate(PredicateIn, true, true)
}

func NewMultiNotInPredicate() *Predicate {
	return newInPredicate(PredicateIn, false, true)
}

func NewNotInPredicate() *Predicate {
	return newInPredicate(PredicateNotIn, false, false)
}

func newInPredicate(name string, equal bool, multi bool) *Predicate {
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

	return &Predicate{
		Template: &predicate.Template{
			Name:   name,
			Source: " " + in,
			Args:   args,
		},
	}
}

func NewLikePredicate() *Predicate {
	return newLikePredicate(PredicateLike, true)
}

func NewNotLikePredicate() *Predicate {
	return newLikePredicate(PredicateNotLike, false)
}

func newLikePredicate(name string, inclusive bool) *Predicate {
	args := []*predicate.NamedArgument{
		{
			Name:     "Alias",
			Position: 0,
		},
		{
			Name:     "ColumnName",
			Position: 1,
		},
	}
	column := `${Alias}` + `+ "." + ${ColumnName}`
	criteria := fmt.Sprintf(`$criteria.Like(%v, $FilterValue)`, column)
	if !inclusive {
		criteria = fmt.Sprintf(`$criteria.NotLike(%v, $FilterValue)`, column)
	}
	return &Predicate{
		Template: &predicate.Template{
			Name:   name,
			Source: " " + criteria,
			Args:   args,
		},
	}
}

func binaryPredicate(name, operator string) *Predicate {
	return &Predicate{
		Template: &predicate.Template{
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
		},
		Handler: nil,
	}
}

func (p *PredicateHandlerFactory) New(lookupType xreflect.LookupType, args ...string) (codec.PredicateHandler, error) {
	if len(args) < 1 {
		return nil, NotEnoughParametersError(args, PredicateHandler, 1)
	}

	predicateType := args[0]
	handlerType, err := types.LookupType(lookupType, predicateType)
	if err != nil {
		return nil, err
	}

	value := types.NewValue(handlerType)
	valueHandler, ok := value.(codec.PredicateHandler)
	if !ok {
		return nil, fmt.Errorf("%T doesn't implement PredicateHandler", value)
	}

	return valueHandler, nil
}
