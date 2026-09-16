package standalone

import (
	"fmt"
	"reflect"
	"strings"

	xcodec "github.com/viant/xdatly/codec"
)

func normalizeCodecs(input map[string]xcodec.Factory) (map[string]xcodec.Factory, error) {
	result := make(map[string]xcodec.Factory, len(input))
	for name, factory := range input {
		key := strings.ToLower(strings.TrimSpace(name))
		switch key {
		case "", "jwtclaim", "jwtclaims", "structql":
			return nil, fmt.Errorf("application codec name %q is empty or reserved", name)
		}
		if _, ok := result[key]; ok {
			return nil, fmt.Errorf("duplicate application codec %q", name)
		}
		if factory == nil {
			return nil, fmt.Errorf("application codec %q has a nil factory", name)
		}
		value := reflect.ValueOf(factory)
		switch value.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
			if value.IsNil() {
				return nil, fmt.Errorf("application codec %q has a nil factory", name)
			}
		}
		result[key] = factory
	}
	return result, nil
}

type applicationCodecs struct {
	factories map[string]xcodec.Factory
	fallback  xcodec.Factory
}

func (c *applicationCodecs) New(config *xcodec.Config, options ...xcodec.Option) (xcodec.Instance, error) {
	if config == nil {
		return nil, fmt.Errorf("codec config is required")
	}
	if factory := c.factories[strings.ToLower(strings.TrimSpace(config.Body))]; factory != nil {
		copy := *config
		copy.Args = append([]string(nil), config.Args...)
		return factory.New(&copy, options...)
	}
	if c.fallback != nil {
		return c.fallback.New(config, options...)
	}
	return nil, fmt.Errorf("codec %q is not registered", config.Body)
}
