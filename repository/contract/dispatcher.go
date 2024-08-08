package contract

import (
	"context"
	hstate "github.com/viant/xdatly/handler/state"
	"net/http"
)

type (
	//Path represents a path
	Options struct {
		Constants map[string]interface{}
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
	Dispatch(ctx context.Context, path *Path, Request *http.Request, form *hstate.Form, options ...Option) (interface{}, error)
}

// WithConstants adds constants
func WithConstants(constants map[string]interface{}) Option {
	return func(o *Options) {
		o.Constants = constants
	}
}
