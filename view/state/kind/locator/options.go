package locator

import (
	"context"
	"github.com/viant/datly/router/marshal/common"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/structology"
	"net/http"
	"reflect"
)

// Options represents locator options
type (
	Options struct {
		Request          *http.Request
		Parent           *KindLocator
		URIPattern       string
		BodyType         reflect.Type
		Unmarshal        Unmarshal
		IOConfig         common.IOConfig
		Custom           []interface{}
		ParameterLookup  ParameterLookup
		ReadInto         ReadInto
		Parameters       state.NamedParameters
		OutputParameters state.Parameters
		Views            view.NamedViews
		State            *structology.State
	}

	ParameterLookup func(ctx context.Context, parameter *state.Parameter) (interface{}, bool, error)
	ReadInto        func(ctx context.Context, dest interface{}, aView *view.View) error
)

func (u Options) UnmarshalFunc() Unmarshal {
	if u.Unmarshal != nil {
		return u.Unmarshal
	}
	var jsonUnmarshaller = json.New(u.IOConfig)
	u.Unmarshal = func(data []byte, dest any) error {
		return jsonUnmarshaller.Unmarshal(data, dest)
	}
	return u.Unmarshal
}

func NewOptions(opts []Option) *Options {
	ret := &Options{}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

// Option represents locator option
type Option func(o *Options)

// WithRequest create http request option
func WithRequest(request *http.Request) Option {
	return func(o *Options) {
		o.Request = request
	}
}

// WithCustomOption creates custom options
func WithCustomOption(options ...interface{}) Option {
	return func(o *Options) {
		o.Custom = options
	}
}

// WithURIPattern create Path pattern request
func WithURIPattern(URI string) Option {
	return func(o *Options) {
		o.URIPattern = URI
	}
}

// WithBodyType create Body Type option
func WithBodyType(rType reflect.Type) Option {
	return func(o *Options) {
		o.BodyType = rType
	}
}

// WithUnmarshal creates with unmarshal options
func WithUnmarshal(fn func([]byte, interface{}) error) Option {
	return func(o *Options) {
		o.Unmarshal = fn
	}
}

// WithParent creates with parent options
func WithParent(locators *KindLocator) Option {
	return func(o *Options) {
		o.Parent = locators
	}
}

// WithParameterLookup creates with parameter options
func WithParameterLookup(lookupFn ParameterLookup) Option {
	return func(o *Options) {
		o.ParameterLookup = lookupFn
	}
}

func WithIOConfig(config common.IOConfig) Option {
	return func(o *Options) {
		o.IOConfig = config
	}
}

// WithParameters creates with parameter options
func WithParameters(parameters state.NamedParameters) Option {
	return func(o *Options) {
		if len(o.Parameters) == 0 {
			o.Parameters = make(state.NamedParameters)
		}
		for k, v := range parameters {
			o.Parameters[k] = v
		}
	}
}

func WithReadInto(fn ReadInto) Option {
	return func(o *Options) {
		o.ReadInto = fn
	}
}

// WithViews returns with views options
func WithViews(views view.NamedViews) Option {
	return func(o *Options) {
		o.Views = views
	}
}

// WithState returns with satte options
func WithState(state *structology.State) Option {
	return func(o *Options) {
		o.State = state
	}
}

func WithOutputParameters(parameters state.Parameters) Option {
	return func(o *Options) {
		o.OutputParameters = parameters
	}
}

// WithStatus returns with status options
