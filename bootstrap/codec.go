package bootstrap

import (
	"fmt"
	"strings"

	structqlcodec "github.com/viant/datly/runtime/handler/codec/structql"
	xcodec "github.com/viant/xdatly/codec"
)

type codecFactory struct {
	builtins map[string]xcodec.Factory
	fallback xcodec.Factory
}

func newCodecFactory(fallback xcodec.Factory) xcodec.Factory {
	return &codecFactory{
		builtins: map[string]xcodec.Factory{structqlcodec.Name: structqlcodec.Factory{}},
		fallback: fallback,
	}
}

func (f *codecFactory) New(config *xcodec.Config, options ...xcodec.Option) (xcodec.Instance, error) {
	if config == nil {
		return nil, fmt.Errorf("codec config is required")
	}
	name := strings.ToLower(strings.TrimSpace(config.Body))
	if factory := f.builtins[name]; factory != nil {
		copy := *config
		copy.Body = name
		return factory.New(&copy, options...)
	}
	if f.fallback != nil {
		return f.fallback.New(config, options...)
	}
	return nil, fmt.Errorf("codec %q is not registered", config.Body)
}
