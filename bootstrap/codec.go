package bootstrap

import (
	"fmt"
	"reflect"
	"strings"

	structqlcodec "github.com/viant/datly/runtime/handler/codec/structql"
	xcodec "github.com/viant/xdatly/codec"
)

type codecFactory struct {
	builtins map[string]xcodec.Factory
	fallback xcodec.Factory
	lookup   func(string) (reflect.Type, error)
}

func newCodecFactory(fallback xcodec.Factory) *codecFactory {
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
	lookup := f.lookup
	if lookup == nil {
		lookup = xcodec.NewOptions(options).LookupType
	}
	if lookup != nil {
		typeOf, err := lookup(strings.TrimSpace(config.Body))
		if err != nil {
			return nil, fmt.Errorf("resolve codec type %q: %w", config.Body, err)
		}
		if typeOf != nil {
			for typeOf.Kind() == reflect.Pointer {
				typeOf = typeOf.Elem()
			}
			pointer := reflect.PointerTo(typeOf)
			if pointer.Implements(reflect.TypeFor[xcodec.Factory]()) {
				return reflect.New(typeOf).Interface().(xcodec.Factory).New(config, options...)
			}
			if pointer.Implements(reflect.TypeFor[xcodec.Instance]()) {
				if len(config.Args) != 0 {
					return nil, fmt.Errorf("codec instance %s does not accept codec arguments", typeOf)
				}
				return reflect.New(typeOf).Interface().(xcodec.Instance), nil
			}
			return nil, fmt.Errorf("codec type %s implements neither codec.Factory nor codec.Instance", typeOf)
		}
	}
	if f.fallback != nil {
		return f.fallback.New(config, options...)
	}
	return nil, fmt.Errorf("codec %q is not registered", config.Body)
}
