package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/structology"
	hstate "github.com/viant/xdatly/handler/state"
	"net/http"
	"reflect"
	"sync"
)

type Body struct {
	bodyType     reflect.Type
	form         *hstate.Form
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
		var request *http.Request
		request, r.err = shared.CloneHTTPRequest(r.request)
		r.body, r.err = readRequestBody(request)
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
		return r.decodeBodyMap(ctx)
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

func (r *Body) decodeBodyMap(ctx context.Context) (interface{}, bool, error) {
	aMapPtr := reflect.New(r.bodyType)
	aMap := reflect.MakeMap(r.bodyType)
	aMapPtr.Elem().Set(aMap)
	ret := aMapPtr.Interface()
	err := r.unmarshal(r.body, ret)
	if err == nil {
		r.updateQueryString(ctx, ret)
	}
	return ret, err == nil, err
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
	var ret = &Body{request: options.request, bodyType: options.BodyType, unmarshal: options.UnmarshalFunc(), form: options.Form}
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

func (r *Body) updateQueryString(ctx context.Context, body interface{}) {
	var queryParams map[string]string
	switch actual := body.(type) {
	case *map[string]interface{}:
		queryParams = make(map[string]string)

		for key, value := range *actual {
			switch actual := value.(type) {
			case string:
				queryParams[key] = actual
			default:
				queryParams[key] = fmt.Sprintf("%v", value)
			}
		}
	case *map[string]string:
		queryParams = *actual
	}
	if len(queryParams) > 0 {
		for k, v := range queryParams {
			r.form.Set(k, v)
		}
	}

	req := r.request
	q := req.URL.Query()
	for key, value := range queryParams {
		q.Set(key, value)
	}

	// Encode the query string and assign it back to the request's URL
	req.URL.RawQuery = q.Encode()
}
