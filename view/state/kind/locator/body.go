package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/structology"
)

type Body struct {
	body  []byte
	state *structology.State
	names []string
}

func (v *Body) Names() []string {
	return v.names
}

func (v *Body) Value(ctx context.Context, name string) (interface{}, bool, error) {
	if name == "" {
		return v.state.State(), true, nil
	}
	sel, err := v.state.Selector(name)
	if err != nil {
		return nil, false, err
	}
	if !sel.Has(v.state.Pointer()) {
		return nil, false, nil
	}
	return sel.Value(v.state.Pointer()), true, nil
}

// NewBody returns body locator
func NewBody(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.BodyType == nil {
		return nil, fmt.Errorf("body type was empty")
	}
	if options.Request == nil {
		return nil, fmt.Errorf("request was empty")
	}
	if options.Request.Body == nil {
		return nil, fmt.Errorf("request.body was empty")
	}
	data, err := readRequestBody(options.Request)
	if err != nil {
		return nil, err
	}
	var ret = &Body{body: data}
	bodyType := structology.NewStateType(options.BodyType)
	ret.state = bodyType.NewState()
	unmarshal := options.UnmarshalFunc()
	err = unmarshal(data, ret.state.State())
	return ret, err
}
