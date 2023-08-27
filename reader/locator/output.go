package locator

import (
	"context"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/xdatly/handler/response"
	"strings"
)

type outputLocator struct {
	Output *reader.Output
	Status *response.Status
}

func (v *outputLocator) Names() []string {
	return nil
}

func (v *outputLocator) Value(ctx context.Context, name string) (interface{}, bool, error) {

	switch strings.ToLower(name) {
	case "data":
		if v.Output == nil {
			return nil, false, nil
		}
		return v.Output.Data, true, nil
	case "summary", "meta":
		if v.Output == nil {
			return nil, false, nil
		}
		return v.Output.ViewMeta, true, nil
	case "status":
		if v.Status == nil {
			return nil, false, nil
		}
		return v.Status, true, nil
	}
	return nil, false, nil
}

// newOutputLocator returns output locator
func newOutputLocator(opts ...locator.Option) (kind.Locator, error) {
	options := locator.NewOptions(opts)
	ret := &outputLocator{}
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
