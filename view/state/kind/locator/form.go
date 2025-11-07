package locator

import (
	"context"
	"mime"
	"net/http"
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

func (r *Form) Value(ctx context.Context, _ reflect.Type, name string) (interface{}, bool, error) {
	if r.form != nil && len(r.form.Values) == 0 && r.request == nil {
		return nil, false, nil
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

// seedFormFromMultipart parses multipart/form-data (if needed) and copies textual values to the shared form
func (r *Form) seedFormFromMultipart() {
	if r.request == nil || r.form == nil {
		return
	}
	if r.request.MultipartForm == nil {
		// Only ParseMultipartForm for form-data; other multipart types aren't supported by ParseMultipartForm
		ct := r.request.Header.Get("Content-Type")
		if ct != "" {
			if mediaType, _, err := mime.ParseMediaType(ct); err == nil && shared.IsFormData(mediaType) {
				// Use the same default memory threshold as Body locator
				const maxMultipartMemory = 32 << 20 // 32 MiB
				_ = r.request.ParseMultipartForm(maxMultipartMemory)
			}
		}
	}
	if r.request.MultipartForm == nil {
		return
	}
	r.form.Mutex().Lock()
	defer r.form.Mutex().Unlock()
	for k, vs := range r.request.MultipartForm.Value {
		if len(vs) == 0 {
			continue
		}
		r.form.Set(k, vs...)
	}
}
