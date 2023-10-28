package translator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

func (s *Service) updateExplicitInputType(resource *Resource, viewlet *Viewlet, parameters state.Parameters) error {
	if 1 == 1 {
		return nil
	}
	res := view.NewResourcelet(&resource.Resource, &viewlet.View.View)
	res.Parameters = parameters
	res.Resource.Init(context.Background())

	predicates := 0
	for _, param := range parameters {
		err := param.Init(context.Background(), res)
		fmt.Printf("%v %v %v\n", param.Name, err, param.Schema.Name)
		predicates += len(param.Predicates)
		switch param.In.Kind {
		case state.KindRepeated:

		case state.KindObject:
			resource.AddParameterType(param)
		}

	}
	if predicates > 0 {
		output := resource.OutputState.ViewParameters()
		filter := output.LookupByLocation(state.KindOutput, "filter")
		if filter != nil {
			if filter.Schema == nil {
				filter.Schema = &state.Schema{}
			}
			filter.Schema.SetType(parameters.PredicateStructType(resource.Rule.Doc.Filter))
			resource.AddParameterType(filter)
		}
	}
	return nil
}
