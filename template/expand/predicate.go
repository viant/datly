package expand

import (
	dConfig "github.com/viant/datly/config"
	"github.com/viant/datly/utils/types"
	"github.com/viant/xdatly/handler/parameter"
	"strings"
	"unsafe"
)

type (
	Predicate struct {
		dataUnit *DataUnit
		config   []*PredicateConfig
		state    interface{}
		statePtr unsafe.Pointer
		has      interface{}
		hasPtr   unsafe.Pointer
	}

	PredicateConfig struct {
		Config        *dConfig.PredicateConfig
		StateAccessor *types.Accessor
		HasAccessor   *types.Accessor
		Expander      func(interface{}) (*parameter.Criteria, error)
	}
)

func (p *Predicate) Expand(ctx int) (string, error) {
	result := &strings.Builder{}
	var accArgs []interface{}
	for _, predicateConfig := range p.config {
		if predicateConfig.Config.Context != ctx {
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
			result.WriteString(" AND ")
		}

		result.WriteString(criteria.Query)
		accArgs = append(accArgs, criteria.Args...)
	}

	p.dataUnit.Add(0, accArgs)
	return result.String(), nil
}
