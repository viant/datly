package view

import "reflect"

type ParameterPredicate struct {
	Name    string
	Context int
	Args    []string //actual instances
}

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

type (
	Predicate struct {
		Name     string
		Template string
		Args     []string //named parametr for tempalte substitution
	}

	PredicateRegistry map[string]*Predicate
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

func ExistsPredicate() *Predicate {
	return &Predicate{
		Name:     "exists",
		Template: ` EXISTS (SELECT 1 FROM $Table t WHERE $Column = $FilterValue AND $JoinColumn = $ParentColumn)`,
		Args: []string{
			"Column",
			"Table",
			"JoinColumn",
			"ParentColumn",
		},
	}
}

func ComputePredicate(registry PredicateRegistry, predicateParameter *Parameter, paramState interface{}) error {
	//TODO get PredicateInstance and populate based on the
	return nil
}
