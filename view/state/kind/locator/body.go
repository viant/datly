package locator

import (
	"context"
	"fmt"
	"mime"
	"mime/multipart"
	"net/http"
	"reflect"
	"sync"

	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/structology"
	hstate "github.com/viant/xdatly/handler/state"
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
	isMultipart bool
}

const maxMultipartMemory = 32 << 20 // 32 MiB

func (r *Body) Names() []string {
	return nil
}

func (r *Body) Value(ctx context.Context, rType reflect.Type, name string) (interface{}, bool, error) {
	var err error
	r.initOnce()
	var requestState *structology.State

	// Multipart handling
	if r.isMultipart {
		return r.handleMultipartValue(rType, name)
	}

	if len(r.body) > 0 {
		if r.requestState != nil && r.requestState.Type().Type() == rType {
			requestState = r.requestState
		}
		if name == "" {
			requestState, r.err = r.ensureRequest(rType)
		} else {
			requestState, r.err = r.ensureRequest(r.bodyType)
		}
		if r.err == nil {
			r.requestState = requestState
		}
	}

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
		return requestState.State(), true, nil
	}
	sel, err := requestState.Selector(name)
	if err != nil {
		return nil, false, err
	}
	if !sel.Has(requestState.Pointer()) {
		return nil, false, nil
	}
	return sel.Value(requestState.Pointer()), true, nil
}

// initOnce initializes body locator state based on content type (multipart vs non-multipart)
func (r *Body) initOnce() {
	r.Once.Do(func() {
		// Multipart branch
		if r.request != nil {
			ct := r.request.Header.Get("Content-Type")
			if shared.IsMultipartContentType(ct) {
				r.isMultipart = true
				if mediaType, _, err := mime.ParseMediaType(ct); err == nil && shared.IsFormData(mediaType) {
					r.err = r.request.ParseMultipartForm(maxMultipartMemory)
					if r.err == nil {
						r.seedFormFromMultipart()
					}
				}
				return
			}
		}
		// Non-multipart: clone and read body safely
		var request *http.Request
		request, r.err = shared.CloneHTTPRequest(r.request)
		r.body, r.err = readRequestBody(request)
	})
}

// handleMultipartValue returns value for multipart/form-data content
func (r *Body) handleMultipartValue(rType reflect.Type, name string) (interface{}, bool, error) {
	if r.err != nil {
		return nil, false, r.err
	}
	if r.request == nil || r.request.MultipartForm == nil {
		return nil, false, nil
	}
	if name == "" {
		return nil, false, nil
	}
	// File destinations
	if rType != nil {
		// []*multipart.FileHeader
		if rType.Kind() == reflect.Slice && rType.Elem() == reflect.TypeOf((*multipart.FileHeader)(nil)) {
			files := r.request.MultipartForm.File[name]
			if len(files) == 0 {
				return nil, false, nil
			}
			return files, true, nil
		}
		// *multipart.FileHeader
		if rType == reflect.TypeOf((*multipart.FileHeader)(nil)) {
			files := r.request.MultipartForm.File[name]
			if len(files) == 0 {
				return nil, false, nil
			}
			return files[0], true, nil
		}
	}
	// Textual parts
	if r.request.MultipartForm.Value != nil {
		if vs, ok := r.request.MultipartForm.Value[name]; ok && len(vs) > 0 {
			if rType != nil && rType.Kind() == reflect.Slice && rType.Elem().Kind() == reflect.String {
				return vs, true, nil
			}
			return vs[0], true, nil
		}
	}
	return nil, false, nil
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
	if options.request == nil {
		return nil, fmt.Errorf("request was empty")
	}
	if options.Unmarshal == nil {
		return nil, fmt.Errorf("unmarshal was empty")
	}
	// Allow missing BodyType only for multipart/* requests; otherwise keep existing requirement.
	if options.BodyType == nil {
		ct := ""
		if options.request != nil && options.request.Header != nil {
			ct = options.request.Header.Get("Content-Type")
		}
		isMultipart := false
		if ct != "" {
			isMultipart = shared.IsMultipartContentType(ct)
		}
		if !isMultipart {
			return nil, fmt.Errorf("body type was empty")
		}
	}
	var ret = &Body{request: options.request, bodyType: options.BodyType, unmarshal: options.UnmarshalFunc(), form: options.Form}
	return ret, nil
}

func (r *Body) ensureRequest(rType reflect.Type) (*structology.State, error) {
	if rType == nil {
		return nil, nil
	}
	if rType.Kind() == reflect.Map {
		return nil, nil
	}
	bodyType := structology.NewStateType(rType)
	requestState := bodyType.NewState()
	dest := requestState.StatePtr()
	err := r.unmarshal(r.body, dest)
	if err == nil {
		requestState.Sync()
	}
	return requestState, err
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

// isMultipartRequest checks content type for multipart/form-data
// removed: local isMultipartRequest; use shared.IsMultipartContentType instead

// seedFormFromMultipart copies textual multipart values into shared form to avoid re-parsing later
func (r *Body) seedFormFromMultipart() {
	if r.request == nil || r.request.MultipartForm == nil || r.form == nil {
		return
	}
	for k, vs := range r.request.MultipartForm.Value {
		if len(vs) == 0 {
			continue
		}
		r.form.Set(k, vs...)
	}
}
