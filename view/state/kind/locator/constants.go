package locator

import (
	"context"
	"github.com/viant/datly/view/state/kind"
	"sync"
)

type Constants struct {
	constants         map[string]interface{}
	resourceConstants map[string]interface{}
	sync.Once
}

func (r *Constants) Names() []string {
	return nil
}

func (r *Constants) Value(ctx context.Context, name string) (interface{}, bool, error) {
	if len(r.constants) > 0 {
		if value, ok := r.constants[name]; ok {
			return value, true, nil
		}
	}
	if len(r.resourceConstants) > 0 {
		if value, ok := r.resourceConstants[name]; ok {
			return value, true, nil
		}
	}
	return nil, false, nil
}

// NewConstants returns body locator
func NewConstants(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	var ret = &Constants{constants: options.constants, resourceConstants: options.resourceConstants}
	return ret, nil
}
