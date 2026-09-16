package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/xdatly/handler/exec"
	"reflect"
)

type Context struct {
}

func (v *Context) Names() []string {
	return nil
}

func (v *Context) Value(ctx context.Context, _ reflect.Type, name string) (interface{}, bool, error) {

	rawValue := ctx.Value(exec.ContextKey)
	if rawValue == nil {
		return nil, false, nil
	}
	execContext, ok := rawValue.(*exec.Context)
	if !ok {
		return nil, false, fmt.Errorf("invalid exec context, expected: %T, but had: %T", execContext, rawValue)
	}
	value, has := execContext.Value(name)
	return value, has, nil
}

// NewContext returns env locator
func NewContext(_ ...Option) (kind.Locator, error) {
	ret := &Context{}
	return ret, nil
}
