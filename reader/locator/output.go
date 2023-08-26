package locator

import (
	"context"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/datly/view/state/kind/locator"
)

type outputLocator struct {
	Output *reader.Output
}

func (v *outputLocator) Names() []string {
	return nil
}

func (v *outputLocator) Value(ctx context.Context, name string) (interface{}, bool, error) {
	switch name {

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
	}
	return ret, nil
}

func init() {
	locator.Register(state.KindOutput, newOutputLocator)
}
