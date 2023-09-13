package locator

import (
	"context"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/datly/gateway/router/marshal/json"
	"github.com/viant/datly/repository/component"
	"github.com/viant/datly/service/reader"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/structology"
	"net/http"
	"reflect"
)

// Options represents locator options
type (
	Options struct {
		request          *http.Request
		Parent           *KindLocator
		URIPattern       string
		BodyType         reflect.Type
		Unmarshal        Unmarshal
		IOConfig         config.IOConfig
		Custom           []interface{}
		ParameterLookup  ParameterLookup
		ReadInto         ReadInto
		InputParameters  state.NamedParameters
		OutputParameters state.NamedParameters
		Views            view.NamedViews
		View             *view.View
		Metrics          reader.Metrics
		State            *structology.State
		Dispatcher       component.Dispatcher
	}

	ParameterLookup func(ctx context.Context, parameter *state.Parameter) (interface{}, bool, error)
	ReadInto        func(ctx context.Context, dest interface{}, aView *view.View) error
)

func (u Options) GetRequest() (*http.Request, error) {
	return shared.CloneHTTPRequest(u.request)
}

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
		o.request = request
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

func WithIOConfig(config config.IOConfig) Option {
	return func(o *Options) {
		o.IOConfig = config
	}
}

// WithInputParameters creates with parameter options
func WithInputParameters(parameters state.NamedParameters) Option {
	return func(o *Options) {
		if len(o.InputParameters) == 0 {
			o.InputParameters = make(state.NamedParameters)
		}
		for k, v := range parameters {
			o.InputParameters[k] = v
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
		o.OutputParameters = parameters.Index()
	}
}

// WithDispatcher returns options to set dispatcher
func WithDispatcher(dispatcher component.Dispatcher) Option {
	return func(o *Options) {
		o.Dispatcher = dispatcher
	}
}

// WithDispatched returns options to set dispatcher
func WithView(aView *view.View) Option {
	return func(o *Options) {
		o.View = aView
	}
}

// WithMetrics return metrics option
func WithMetrics(metrics reader.Metrics) Option {
	return func(o *Options) {
		o.Metrics = metrics
	}
}
