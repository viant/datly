package contract

import (
	"context"
	hstate "github.com/viant/xdatly/handler/state"
	"net/http"
)

type (
	//Options represents dispatcher options
	Options struct {
		Constants      map[string]interface{}
		PathParameters map[string]string
		Form           *hstate.Form
		Request        *http.Request
	}
	//Option represents a dispatcher option
	Option func(o *Options)
)

// NewOptions creates a new options
func NewOptions(opts ...Option) *Options {
	var o = &Options{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// Dispatcher represents a dispatcher
type Dispatcher interface {
	//Dispatch dispatches request
	Dispatch(ctx context.Context, path *Path, options ...Option) (interface{}, error)
}

// WithConstants adds constants
func WithConstants(constants map[string]interface{}) Option {
	return func(o *Options) {
		o.Constants = constants
	}
}

// WithPathParameters adds path parameters
func WithPathParameters(pathParameters map[string]string) Option {
	return func(o *Options) {
		o.PathParameters = pathParameters
	}
}

// WithForm adds form
func WithForm(form *hstate.Form) Option {
	return func(o *Options) {
		o.Form = form
	}
}

// WithRequest adds request
func WithRequest(request *http.Request) Option {
	return func(o *Options) {
		o.Request = request
	}
}
