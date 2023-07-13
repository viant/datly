package expand

import (
	"github.com/viant/datly/utils/types"
	"github.com/viant/xdatly/handler/parameter"
	"github.com/viant/xunsafe"
	"strings"
	"unsafe"
)

type (
	Predicate struct {
		dataUnit *DataUnit
		config   []*PredicateConfig
		state    interface{}
		has      interface{}
		statePtr unsafe.Pointer
		hasPtr   unsafe.Pointer
	}

	PredicateConfig struct {
		Context       int
		StateAccessor *types.Accessor
		HasAccessor   *types.Accessor
		Expander      func(interface{}) (*parameter.Criteria, error)
	}
)

func NewPredicate(state, has interface{}, config []*PredicateConfig, dataUnit *DataUnit) *Predicate {
	return &Predicate{
		dataUnit: dataUnit,
		config:   config,
		state:    state,
		statePtr: xunsafe.AsPointer(state),
		has:      has,
		hasPtr:   xunsafe.AsPointer(has),
	}
}

func (p *Predicate) ExpandWith(ctx int, operator string) (string, error) {
	return p.expand(ctx, operator)
}
func (p *Predicate) Expand(ctx int) (string, error) {
	return p.expand(ctx, "AND")
}

func (p *Predicate) expand(ctx int, operator string) (string, error) {
	result := &strings.Builder{}
	var accArgs []interface{}
	for _, predicateConfig := range p.config {
		if predicateConfig.Context != ctx {
			continue
		}

		if p.hasPtr != nil {
			value, err := predicateConfig.HasAccessor.Value(p.hasPtr)
			if err != nil {
				return "", err
			}

			asBool, ok := value.(bool)
			if !asBool && ok {
				continue
			}
		}

		value, err := predicateConfig.StateAccessor.Value(p.hasPtr)
		if err != nil {
			return "", err
		}

		criteria, err := predicateConfig.Expander(value)
		if err != nil {
			return "", err
		}

		if result.Len() != 0 {
			result.WriteString(" ")
			result.WriteString(operator)
			result.WriteString(" ")
		}

		result.WriteString(criteria.Query)
		accArgs = append(accArgs, criteria.Args...)
	}

	p.dataUnit.Add(0, accArgs)
	return result.String(), nil
}
