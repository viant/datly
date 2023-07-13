package view

import (
	"github.com/viant/datly/config"
	"github.com/viant/xunsafe"
	"reflect"
)

type (
	PredicateRegistry map[string]*Predicate
	Predicate         struct {
		Parameters []*PredicateParameter
		config.PredicateConfig
	}

	PredicateParameter struct {
		Parameter *Parameter
		Setter    *xunsafe.Field
	}
)

type (
	PredicateInstance struct {
		Values     []*PredicateValue
		BindValues [][]interface{}
		Criteria   []string
	}

	PredicateValue struct {
		Name             string
		ExpandedCriteria string
		Value            interface{}
	}
)

func (p *Predicate) Type() reflect.Type {
	stringType := reflect.TypeOf("")
	var fields = []reflect.StructField{
		{
			Name: "FilterValue",
			Type: stringType,
		},
	}
	for _, arg := range p.Args {
		fields = append(fields, reflect.StructField{Name: arg, Type: stringType})
	}
	return reflect.StructOf(fields)
}

func (r PredicateRegistry) Lookup(name string) *Predicate {
	return r[name]
}
