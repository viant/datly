package locator

import (
	"context"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/xdatly/handler/state"
	"net/http"
)

type Form struct {
	form    *state.Form
	request *http.Request
}

func (r *Form) Names() []string {
	return nil
}

func (r *Form) Value(ctx context.Context, name string) (interface{}, bool, error) {
	if r.form != nil && len(r.form.Values) == 0 && r.request == nil {
		return nil, false, nil
	}
	values, ok := r.form.Lookup(name)
	if !ok {
		if r.request == nil {
			return nil, false, nil
		}
		r.form.Mutex().Lock()
		defer r.form.Mutex().Unlock()
		value := r.request.FormValue(name)
		if value == "" {
			if r.request.Form == nil {
				return nil, false, nil
			}
			_, ok := r.request.Form[name]
			return nil, ok, nil
		}
		return value, true, nil
	}
	if len(values) > 1 {
		return values, true, nil
	}
	return r.form.Get(name), true, nil
}

// NewForm returns body locator
func NewForm(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	var ret = &Form{form: options.Form, request: options.request}
	return ret, nil
}
