package locator

import (
	"context"
	"github.com/viant/datly/view/state/kind"
	"reflect"
)

type Transient struct{}

func (v *Transient) Names() []string {
	return nil
}

func (v *Transient) Value(ctx context.Context, _ reflect.Type, name string) (interface{}, bool, error) {
	if name == "" {
		return nil, false, nil
	}
	return nil, true, nil
}

// NewTransient returns Transient locator
func NewTransient(_ ...Option) (kind.Locator, error) {
	ret := &Transient{}
	return ret, nil
}
