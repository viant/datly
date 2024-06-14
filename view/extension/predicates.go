package extension

import (
	"fmt"
	"github.com/viant/datly/utils/types"
	codec2 "github.com/viant/datly/view/extension/codec"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/predicate"
	"github.com/viant/xreflect"
	"sync"
)

const (
	PredicateEqual          = "equal"
	PredicateNotEqual       = "not_equal"
	PredicateIn             = "in"
	PredicateMultiIn        = "multi_in"
	PredicateNotIn          = "not_in"
	PredicateMultiNotIn     = "multi_not_in"
	PredicateLessOrEqual    = "less_or_equal"
	PredicateLessThan       = "less_than"
	PredicateGreaterOrEqual = "greater_or_equal"
	PredicateGreaterThan    = "greater_than"

	PredicateLike        = "like"
	PredicateNotLike     = "not_like"
	PredicateHandler     = "handler"
	PredicateContains    = "contains"
	PredicateNotContains = "not_contains"
	PredicateIsNotNull   = "is_not_null"
	PredicateIsNull      = "is_null"
	PredicateExists      = "exists"
	PredicateNotExists   = "not_exists"

	PredicateCriteriaExists    = "exists_criteria"
	PredicateCriteriaNotExists = "not_exists_criteria"
	PredicateCriteriaIn        = "in_criteria"
	PredicateCriteriaNotIn     = "not_in_criteria"
	PredicateBetween           = "between"
	PredicateDuration          = "duration"
	PredicateWhenPresent       = "when_present"
	PredicateWhenNotPresent    = "when_not_present"
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
		Parent string
		Name   string   `yaml:"Name"`
		Group  int      `yaml:"Group"`
		Ensure bool     `yaml:"Ensure"`
		Args   []string `yaml:"Args"`
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

func newIsNullPredicate(name string, negated bool) *Predicate {
	args := []*predicate.NamedArgument{
		{
			Name:     "Alias",
			Position: 0,
		},
		{
			Name:     "Column",
			Position: 1,
		},
	}

	clause := `  ${Alias}.${Column} IS`

	if negated {
		clause = " NOT "
	}

	clause += " NULL "
	return &Predicate{
		Template: &predicate.Template{
			Name:   name,
			Source: " " + clause,
			Args:   args,
		},
	}
}

func NewBetweenPredicate() *Predicate {
	args := []*predicate.NamedArgument{
		{
			Name:     "Expression",
			Position: 0,
		},
		{
			Name:     "From",
			Position: 1,
		},
		{
			Name:     "To",
			Position: 2,
		},
	}
	clause := ` ${Expression} BETWEEN
      #if($FilterValue.Has.ValueMin)
          $criteria.AppendBinding($FilterValue.ValueMin)
      #else
          NULL
      #end
      AND
      #if($FilterValue.Has.ValueMax)
          $criteria.AppendBinding($FilterValue.ValueMax)
      #else
          NULL
      #end`
	return &Predicate{
		Template: &predicate.Template{
			Name:   PredicateBetween,
			Source: clause,
			Args:   args,
		},
	}
}

func NewDurationPredicate() *Predicate {
	args := []*predicate.NamedArgument{
		{
			Name:     "DayExpression",
			Position: 0,
		},
		{
			Name:     "CurrentDayExpression",
			Position: 1,
		},
		{
			Name:     "HourExpression",
			Position: 2,
		},
		{
			Name:     "CurrentHourExpression",
			Position: 3,
		},
		{
			Name:     "YesterdayDayExpression",
			Position: 4,
		}, {
			Name:     "WeekDayExpression",
			Position: 5,
		},
	}
	clause := `
#if($FilterValue == "hour")
	   ${DayExpression} = ${CurrentDayExpression}
	  AND ${HourExpression} = ${CurrentHourExpression}
#elseif($FilterValue == "day")
	 ${DayExpression} = ${CurrentDayExpression}
#elseif($FilterValue == "yesterday")
 	 ${DayExpression} = ${YesterdayDayExpression}
 #elseif($FilterValue == "week")
 	 ${DayExpression} BETWEEN ${WeekDayExpression}  AND ${CurrentDayExpression}
#end
`
	return &Predicate{
		Template: &predicate.Template{
			Name:   PredicateDuration,
			Source: clause,
			Args:   args,
		},
	}
}

func NewIsNullPredicate() *Predicate {
	return newIsNullPredicate(PredicateIsNull, false)
}

func NewIsNotNullPredicate() *Predicate {
	return newIsNullPredicate(PredicateIsNotNull, true)
}

func NewEqualPredicate() *Predicate {
	return binaryPredicate(PredicateEqual, "=")
}

func NewLessOrEqualPredicate() *Predicate {
	return binaryPredicate(PredicateLessOrEqual, "<=")
}
func NewLessThanPredicate() *Predicate {
	return binaryPredicate(PredicateLessThan, "<")
}

func NewGreaterOrEqualPredicate() *Predicate {
	return binaryPredicate(PredicateGreaterOrEqual, ">=")
}

func NewGreaterThanPredicate() *Predicate {
	return binaryPredicate(PredicateGreaterThan, ">")
}

func NewNotEqualPredicate() *Predicate {
	return binaryPredicate(PredicateNotEqual, "!=")
}

func NewInPredicate() *Predicate {
	return newInPredicate(PredicateIn, false, true, false)
}

func NewMultiInPredicate() *Predicate {
	return newInPredicate(PredicateIn, false, true, true)
}

func NewMultiNotInPredicate() *Predicate {
	return newInPredicate(PredicateIn, false, false, true)
}

func NewNotInPredicate() *Predicate {
	return newInPredicate(PredicateNotIn, false, false, false)
}

func newInPredicate(name string, withCriteria bool, equal bool, multi bool) *Predicate {
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
		if withCriteria {
			args = append(args, &predicate.NamedArgument{
				Name:     "LookupAlias",
				Position: 2,
			})

			args = append(args, &predicate.NamedArgument{
				Name:     "LookupTable",
				Position: 3,
			})

			args = append(args, &predicate.NamedArgument{
				Name:     "LookupColumn",
				Position: 4,
			})

			args = append(args, &predicate.NamedArgument{
				Name:     "FilterColumn",
				Position: 5,
			})

			args = append(args, &predicate.NamedArgument{
				Name:     "Criterion",
				Position: 6,
			})

		}
	}

	in := fmt.Sprintf(`$criteria.In(%v, $FilterValue)`, column)

	if !equal {
		in = fmt.Sprintf(`$criteria.NotIn(%v, $FilterValue)`, column)
	}

	if withCriteria {
		in = `${Alias}.${ColumnName} IN (SELECT ${LookupAlias}.${LookupColumn}  FROM ${LookupTable} ${LookupAlias} 
				WHERE ${Criterion} AND
                      $criteria.In(${LookupAlias} + "." + ${FilterColumn}, $FilterValue))  `
		if !equal {
			in = `${Alias}.${ColumnName} NOT IN (SELECT ${LookupAlias}.${LookupColumn}  FROM ${LookupTable} ${LookupAlias} 
				WHERE ${Criterion} AND
                      $criteria.In(${LookupAlias} + "." + ${FilterColumn}, $FilterValue))  `
		}
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

func NewContainsPredicate() *Predicate {
	return newContainsPredicate(PredicateContains, true)
}

func NewNotContainsPredicate() *Predicate {
	return newContainsPredicate(PredicateNotContains, false)
}

func newContainsPredicate(name string, inclusive bool) *Predicate {
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
	criteria := fmt.Sprintf(`$criteria.Contains(%v, $FilterValue)`, column)
	if !inclusive {
		criteria = fmt.Sprintf(`$criteria.NotContains(%v, $FilterValue)`, column)
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

func NewExistsPredicate() *Predicate {
	return newExistsPredicate(PredicateExists, false, false)
}

func NewNotExistsPredicate() *Predicate {
	return newExistsPredicate(PredicateNotExists, false, true)
}

func NewExistsCriteriaPredicate() *Predicate {
	return newExistsPredicate(PredicateCriteriaExists, true, false)
}

func NewNotExistsCriteriaPredicate() *Predicate {
	return newExistsPredicate(PredicateCriteriaNotExists, true, true)
}

func NewInCriteriaPredicate() *Predicate {
	return newInPredicate(PredicateCriteriaIn, true, true, false)
}

func NewNotInCriteriaPredicate() *Predicate {
	return newInPredicate(PredicateCriteriaNotIn, true, false, false)
}

func NewWhenPresent() *Predicate {
	return newWhenPredicate(PredicateWhenPresent)
}

func NewWhenNotPresent() *Predicate {
	return NewWhenNotPredicate(PredicateWhenNotPresent)
}

func newExistsPredicate(name string, withCriteria bool, negated bool) *Predicate {
	args := []*predicate.NamedArgument{
		{
			Name:     "Alias",
			Position: 0,
		},
		{
			Name:     "Column",
			Position: 1,
		},
		{
			Name:     "LookupAlias",
			Position: 2,
		},
		{
			Name:     "LookupTable",
			Position: 3,
		},
		{
			Name:     "LookupColumn",
			Position: 4,
		},
		{
			Name:     "FilterColumn",
			Position: 5,
		},
	}
	if withCriteria {
		args = append(args, &predicate.NamedArgument{
			Name:     "Criterion",
			Position: 6,
		})
	}

	clause := ` EXISTS (SELECT 1 FROM ${LookupTable} ${LookupAlias} 
				WHERE ${LookupAlias}.${LookupColumn} = ${Alias}.${Column} AND
                      $criteria.In(${LookupAlias} + "." + ${FilterColumn}, $FilterValue))  `

	if withCriteria {
		clause = ` EXISTS (SELECT 1 FROM ${LookupTable} ${LookupAlias} 
				WHERE ${LookupAlias}.${LookupColumn} = ${Alias}.${Column} AND ${Criterion} AND
                      $criteria.In(${LookupAlias} + "." + ${FilterColumn}, $FilterValue))  `
	}
	if negated {
		clause = " NOT " + clause
	}

	return &Predicate{
		Template: &predicate.Template{
			Name:   name,
			Source: " " + clause,
			Args:   args,
		},
	}
}

func NewPredicateHandler() *Predicate {
	return &Predicate{Handler: &PredicateHandlerFactory{}}
}

func (p *PredicateHandlerFactory) New(lookupType xreflect.LookupType, args ...string) (codec.PredicateHandler, error) {
	if len(args) < 1 {
		return nil, codec2.NotEnoughParametersError(args, PredicateHandler, 1)
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

func newWhenPredicate(name string) *Predicate {
	condition := `#if($HasFilterValue) ${Criterion} #end`
	return &Predicate{
		Template: &predicate.Template{
			Name:   name,
			Source: " " + condition,
			Args: []*predicate.NamedArgument{
				{Name: "Criterion", Position: 0},
			},
		},
	}
}

func NewWhenNotPredicate(name string) *Predicate {
	condition := `#if(!$HasFilterValue) ${Criterion} #end`
	return &Predicate{
		Template: &predicate.Template{
			Name:   name,
			Source: " " + condition,
			Args: []*predicate.NamedArgument{
				{Name: "Criterion", Position: 0},
			},
		},
	}
}
