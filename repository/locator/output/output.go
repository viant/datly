package output

import (
	"context"
	"github.com/viant/datly/service/reader"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/xdatly/handler/response"
	"strings"
)

type outputLocator struct {
	Output     *reader.Output
	Status     *response.Status
	Parameters state.Parameters
}

func (l *outputLocator) Names() []string {
	return nil
}

func (l *outputLocator) Value(ctx context.Context, name string) (interface{}, bool, error) {
	switch strings.ToLower(name) {
	case "data":
		if l.Output == nil {
			return nil, false, nil
		}
		return l.Output.Data, true, nil
	case "summary", "meta":
		if l.Output == nil {
			return nil, false, nil
		}
		return l.Output.ViewMeta, true, nil
	case "status":
		if l.Status == nil {
			return nil, false, nil
		}
		return l.Status, true, nil
	case "sql":
		if l.Output == nil {
			return nil, false, nil
		}
		SQL := l.Output.Metrics.SQL()
		return SQL, true, nil
	case "filter":
		parameter := l.Parameters.LookupByLocation(state.KindOutput, "filter")
		if parameter == nil || l.Output == nil {
			return nil, false, nil
		}
		filterState, err := l.buildFilter(parameter)
		if err != nil {
			return nil, false, err
		}
		return filterState.State(), true, nil
	}
	return nil, false, nil
}

// newOutputLocator returns output locator
func newOutputLocator(opts ...locator.Option) (kind.Locator, error) {
	options := locator.NewOptions(opts)
	ret := &outputLocator{Parameters: options.OutputParameters}
	for _, candidate := range options.Custom {
		if output, ok := candidate.(*reader.Output); ok {
			ret.Output = output
		}
		if status, ok := candidate.(*response.Status); ok {
			ret.Status = status
		}
	}
	return ret, nil
}

func init() {
	locator.Register(state.KindOutput, newOutputLocator)
}
