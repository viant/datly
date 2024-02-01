package locator

import (
	"context"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
)

type Form struct {
	Form *state.Form
}

func (r *Form) Names() []string {
	return nil
}

func (r *Form) Value(ctx context.Context, name string) (interface{}, bool, error) {
	if len(r.Form.Values) == 0 {
		return nil, false, nil
	}
	value, ok := r.Form.Values[name]
	if !ok {
		return nil, false, nil
	}
	if len(value) > 1 {
		return value, true, nil
	}
	return r.Form.Get(name), true, nil
}

// NewForm returns body locator
func NewForm(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	var ret = &Form{Form: options.Form}
	return ret, nil
}
