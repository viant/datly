package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/structology"
	"net/http"
	"reflect"
	"sync"
)

type Body struct {
	bodyType     reflect.Type
	mux          sync.Mutex
	body         []byte
	unmarshal    Unmarshal
	requestState *structology.State
	request      *http.Request
	err          error
	sync.Once
}

func (r *Body) Names() []string {
	return nil
}

func (r *Body) Value(ctx context.Context, name string) (interface{}, bool, error) {
	var err error
	r.Once.Do(func() {
		r.body, r.err = readRequestBody(r.request)
		if len(r.body) > 0 {
			r.err = r.ensureRequest()
		}
	})
	if len(r.body) == 0 {
		return nil, false, nil
	}
	if r.err != nil {
		return nil, false, r.err
	}
	if r.bodyType.Kind() == reflect.Map {
		aMapPtr := reflect.New(r.bodyType)
		aMap := reflect.MakeMap(r.bodyType)
		aMapPtr.Elem().Set(aMap)
		ret := aMapPtr.Interface()
		err := r.unmarshal(r.body, ret)
		return ret, err == nil, err
	}
	if name == "" {
		return r.requestState.State(), true, nil
	}
	sel, err := r.requestState.Selector(name)
	if err != nil {
		return nil, false, err
	}
	if !sel.Has(r.requestState.Pointer()) {
		return nil, false, nil
	}
	return sel.Value(r.requestState.Pointer()), true, nil
}

// NewBody returns body locator
func NewBody(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.BodyType == nil {
		return nil, fmt.Errorf("body type was empty")
	}
	if options.request == nil {
		return nil, fmt.Errorf("request was empty")
	}
	if options.Unmarshal == nil {
		return nil, fmt.Errorf("unmarshal was empty")
	}
	request, err := options.GetRequest()
	if err != nil {
		return nil, err
	}
	var ret = &Body{request: request, bodyType: options.BodyType, unmarshal: options.UnmarshalFunc()}
	return ret, nil
}

func (r *Body) ensureRequest() (err error) {
	if r.bodyType == nil {
		return nil
	}
	rType := r.bodyType
	if rType.Kind() == reflect.Map {
		return nil
	}
	bodyType := structology.NewStateType(r.bodyType)
	r.requestState = bodyType.NewState()
	dest := r.requestState.StatePtr()
	if err = r.unmarshal(r.body, dest); err == nil {
		r.requestState.Sync()
	}
	return err
}
