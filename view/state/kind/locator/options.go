package locator

import (
	"context"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/datly/gateway/router/marshal/json"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/structology"
	"github.com/viant/xdatly/handler/response"
	hstate "github.com/viant/xdatly/handler/state"
	"net/http"
	"net/url"
	"reflect"
)

// Options represents locator options
type (
	Options struct {
		request           *http.Request
		fromError         error
		form              *hstate.Form
		Parent            *KindLocator
		URIPattern        string
		BodyType          reflect.Type
		Unmarshal         Unmarshal
		IOConfig          *config.IOConfig
		Custom            []interface{}
		ParameterLookup   ParameterLookup
		ReadInto          ReadInto
		InputParameters   state.NamedParameters
		OutputParameters  state.NamedParameters
		Views             view.NamedViews
		Metrics           response.Metrics
		State             *structology.State
		Dispatcher        contract.Dispatcher
		View              *view.View
		Resource          *view.Resource
		Types             []*state.Type
		Constants         map[string]interface{}
		resourceConstants map[string]interface{}
	}

	ParameterLookup func(ctx context.Context, parameter *state.Parameter) (interface{}, bool, error)
	ReadInto        func(ctx context.Context, dest interface{}, aView *view.View) error
)

func (o Options) LookupParameters(name string) *state.Parameter {
	if len(o.InputParameters) > 0 {
		if ret, ok := o.InputParameters[name]; ok {
			return ret
		}
	}
	if len(o.OutputParameters) > 0 {
		if ret, ok := o.OutputParameters[name]; ok {
			return ret
		}
	}
	return nil
}

func (o *Options) GetRequest() (*http.Request, error) {
	return shared.CloneHTTPRequest(o.request)
}

func (o *Options) GetForm() *hstate.Form {
	return o.form
}

func (o *Options) UnmarshalFunc() Unmarshal {
	if o.Unmarshal != nil {
		return o.Unmarshal
	}
	var jsonUnmarshaller = json.New(o.IOConfig)
	o.Unmarshal = func(data []byte, dest any) error {
		return jsonUnmarshaller.Unmarshal(data, dest)
	}
	return o.Unmarshal
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

var defaultURL, _ = url.Parse("http://localhost:8080/")

// WithRequest create http requestState option
func WithRequest(request *http.Request) Option {
	return func(o *Options) {
		ensureValueRequest(request)
		o.request = request
	}
}

func ensureValueRequest(request *http.Request) {
	if request.URL == nil {
		request.URL = defaultURL
	}
	if request.Header == nil {
		request.Header = make(http.Header)
	}
}

// WithCustom creates custom options
func WithCustom(options ...interface{}) Option {
	return func(o *Options) {
		o.Custom = options
	}
}

// WithURIPattern create Path pattern requestState
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

func WithIOConfig(config *config.IOConfig) Option {
	return func(o *Options) {
		o.IOConfig = config
	}
}

// WithInputParameters creates with parameter options
func WithInputParameters(parameters state.NamedParameters) Option {
	return func(o *Options) {
		if len(o.resourceConstants) == 0 {
			o.resourceConstants = make(map[string]interface{})
		}
		if len(o.InputParameters) == 0 {
			o.InputParameters = make(state.NamedParameters)
		}
		for k, v := range parameters {
			o.InputParameters[k] = v
			if v.In.Kind == state.KindConst {
				o.resourceConstants[v.In.Name] = v.Value
			}
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
func WithDispatcher(dispatcher contract.Dispatcher) Option {
	return func(o *Options) {
		o.Dispatcher = dispatcher
	}
}

// WithView returns options to set view
func WithView(aView *view.View) Option {
	return func(o *Options) {
		o.View = aView
	}
}

// WithForm return metrics option
func WithForm(form *hstate.Form) Option {
	return func(o *Options) {
		if o.form == nil {
			o.form = form
		} else if form != nil {
			o.form.SetValues(form.Values)
		}
	}
}

// WithMetrics return metrics option
func WithMetrics(metrics response.Metrics) Option {
	return func(o *Options) {
		o.Metrics = metrics
	}
}

// WithResource return resource option
func WithResource(resource *view.Resource) Option {
	return func(o *Options) {
		o.Resource = resource
	}
}

// WithConstants return Constants option
func WithConstants(constants map[string]interface{}) Option {
	return func(o *Options) {
		o.Constants = constants
	}
}

// WithTypes return types option
func WithTypes(types ...*state.Type) Option {
	return func(o *Options) {
		o.Types = types
	}
}
