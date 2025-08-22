package contract

import (
	"context"
	"github.com/viant/xdatly/handler/logger"
	hstate "github.com/viant/xdatly/handler/state"
	"net/http"
	"net/url"
)

type (
	//Options represents dispatcher options
	Options struct {
		Constants      map[string]interface{}
		PathParameters map[string]string
		Query          url.Values
		Header         http.Header
		Form           *hstate.Form
		Request        *http.Request
		Logger         logger.Logger
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

// WithPath adds path parameters
func WithPath(pathParameters map[string]string) Option {
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

// WithQuery adds query parameters
func WithQuery(query url.Values) Option {
	return func(o *Options) {
		o.Query = query
	}
}

// WithHeader adds header
func WithHeader(header http.Header) Option {
	return func(o *Options) {
		o.Header = header
	}
}

// WithRequest adds request
func WithRequest(request *http.Request) Option {
	return func(o *Options) {
		o.Request = request
	}
}

// WithLogger adds path parameters
func WithLogger(loger logger.Logger) Option {
	return func(o *Options) {
		o.Logger = loger
	}
}
