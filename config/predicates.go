package config

import (
	"fmt"
	"github.com/viant/xdatly/handler/parameter"
)

type (
	PredicateRegistry map[string]parameter.PredicateFactory
	PredicateConfig   struct {
		Name    string
		Context int
		Args    []*NamedArg
	}

	PositionalPredicate interface {
		MapPositionalArgs(position int, value string, config *PredicateConfig) error
	}

	NamedArg struct {
		Position int
		Name     string
	}
)

type (
	ExistsPredicateFactory struct {
	}

	ExistsPredicate struct {
		config *PredicateConfig
	}
)

func (e *ExistsPredicate) Expand(value interface{}) (*parameter.Criteria, error) {
	return &parameter.Criteria{
		Query: "EXISTS (SELECT 1 FROM $Table t WHERE $Column = $FilterValue AND $JoinColumn = $ParentColumn)",
		Args:  []interface{}{value},
	}, nil
}

func (e ExistsPredicateFactory) NewPredicate(args []interface{}, options ...interface{}) (parameter.Predicate, error) {
	for _, option := range options {
		asConfig, ok := option.(*PredicateConfig)
		if ok {
			return &ExistsPredicate{
				config: asConfig,
			}, nil
		}
	}

	return nil, fmt.Errorf("not provided ExistsPredicate config")
}