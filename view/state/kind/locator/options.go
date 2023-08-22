package locator

import (
	"github.com/viant/datly/router/marshal/common"
	"github.com/viant/datly/router/marshal/json"
	"net/http"
	"reflect"
)

// Options represents locator options
type Options struct {
	Request          *http.Request
	Locators         *Locators
	URIPattern       string
	BodyType         reflect.Type
	Unmarshal        Unmarshal
	CustomValidation bool
	IOConfig         common.IOConfig
	Custom           []interface{}
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
func WithParent(locators *Locators) Option {
	return func(o *Options) {
		o.Locators = locators
	}
}
