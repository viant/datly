package locator

import (
	"context"
	"github.com/viant/datly/router/marshal/common"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/toolbox/format"
	"net/http"
	"reflect"
)

// Options represents locator options
type (
	Options struct {
		Request         *http.Request
		Parent          *KindLocator
		URIPattern      string
		BodyType        reflect.Type
		Unmarshal       Unmarshal
		IOConfig        common.IOConfig
		Custom          []interface{}
		ParameterLookup ParameterLookup
		Parameters      state.NamedParameters
		FormatCase      format.Case
		DateFormat      string
	}

	ParameterLookup func(ctx context.Context, parameter *state.Parameter) (interface{}, bool, error)
	ReadViewData    func(ctx context.Context, dest interface{}, aView *view.View, selectors *view.States) error
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
func WithUnmarshal(fn Unmarshal) Option {
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

func WithFormatCase(formatCase format.Case) Option {
	return func(o *Options) {
		o.FormatCase = formatCase
	}
}

func DateFormat(dateFormat string) Option {
	return func(o *Options) {
		o.DateFormat = dateFormat
	}
}

// WithParameters creates with parameter options
func WithParameters(parameters state.NamedParameters) Option {
	return func(o *Options) {
		o.Parameters = parameters
	}
}
