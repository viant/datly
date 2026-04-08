package locator

import (
	"context"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"reflect"
	"sync"

	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/xdatly/handler/state"
)

type Form struct {
	form    *state.Form
	request *http.Request
	once    sync.Once
}

func (r *Form) Names() []string {
	return nil
}

func (r *Form) Value(ctx context.Context, rType reflect.Type, name string) (interface{}, bool, error) {
	if r.form != nil && len(r.form.Values) == 0 && r.request == nil {
		return nil, false, nil
	}

	// Support file uploads when parameters are declared with kind=form
	// and types *multipart.FileHeader or []*multipart.FileHeader. This
	// aligns multipart file fields with form semantics instead of body.
	if r.request != nil && shared.IsMultipartContentType(r.request.Header.Get("Content-Type")) && rType != nil {
		// Parse/seed multipart values only once
		r.once.Do(func() { r.seedFormFromMultipart() })
		if r.request.MultipartForm != nil {
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
	}

	values, ok := r.form.Lookup(name)
	if !ok {
		if r.request == nil {
			return nil, false, nil
		}
		// If multipart, seed from multipart and avoid FormValue fallback
		if shared.IsMultipartContentType(r.request.Header.Get("Content-Type")) {
			r.once.Do(func() { r.seedFormFromMultipart() })
			if values, ok = r.form.Lookup(name); ok {
				if len(values) > 1 {
					return values, true, nil
				}
				return r.form.Get(name), true, nil
			}
			return nil, false, nil
		}
		// Non-multipart: use standard FormValue fallback
		r.form.Mutex().Lock()
		defer r.form.Mutex().Unlock()
		value := r.request.FormValue(name)
		if value == "" {
			if r.request.Form == nil {
				return nil, false, nil
			}
			_, ok := r.request.Form[name]
			return "", ok, nil
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

// seedFormFromMultipart parses multipart/form-data and copies values into shared maps.
// Mutex is required because multiple Form locators (one per parameter) can call this
// concurrently on the same request. Uses form.Values directly instead of form.Set to
// avoid deadlock (form.Set locks the same mutex).
func (r *Form) seedFormFromMultipart() {
	if r.request == nil || r.form == nil {
		return
	}
	if r.request.MultipartForm == nil && len(r.form.Values) == 0 {
		ct := r.request.Header.Get("Content-Type")
		if ct != "" {
			if mediaType, _, err := mime.ParseMediaType(ct); err == nil && shared.IsFormData(mediaType) {
				const maxMultipartMemory = 32 << 20 // 32 MiB
				_ = r.request.ParseMultipartForm(maxMultipartMemory)
			}
		}
	}
	if r.request.MultipartForm == nil {
		return
	}
	// BUG FIX (concurrent map writes):
	mu := r.form.Mutex()
	mu.Lock()
	defer mu.Unlock()
	if r.request.Form == nil {
		r.request.Form = url.Values{}
	}
	for k, vs := range r.request.MultipartForm.Value {
		if len(vs) == 0 {
			continue
		}
		r.form.Values[k] = vs
		r.request.Form[k] = vs
	}
}
