package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/structology"
	"reflect"
	"sync"
)

type Body struct {
	bodyType  reflect.Type
	body      []byte
	unmarshal Unmarshal
	request   *structology.State
	err       error
	sync.Once
}

func (r *Body) Names() []string {
	return nil
}

func (r *Body) Value(ctx context.Context, name string) (interface{}, bool, error) {
	var err error
	r.Once.Do(func() {
		r.err = r.ensureRequest()
	})
	if r.err != nil {
		return nil, false, r.err
	}

	if name == "" {
		return r.request.State(), true, nil
	}
	sel, err := r.request.Selector(name)
	if err != nil {
		return nil, false, err
	}
	if !sel.Has(r.request.Pointer()) {
		return nil, false, nil
	}
	return sel.Value(r.request.Pointer()), true, nil
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
	if options.Unmarshal == nil {
		return nil, fmt.Errorf("unmarshal was empty")
	}
	data, err := readRequestBody(options.Request)
	if err != nil {
		return nil, err
	}
	var ret = &Body{body: data, bodyType: options.BodyType, unmarshal: options.UnmarshalFunc()}
	return ret, err
}

func (r *Body) ensureRequest() (err error) {
	bodyType := structology.NewStateType(r.bodyType)
	r.request = bodyType.NewState()
	dest := r.request.StatePtr()
	if err = r.unmarshal(r.body, dest); err == nil {
		r.request.Sync()
	}
	return err
}
