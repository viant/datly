package custom

import (
	"fmt"
	xshape "github.com/viant/x/shape"

	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
)

// Factory is the typed linking bridge registered as a compiled export in
// viant/x. It creates one custom adapter per registration build.
func Factory[I, O any](create func() xhandler.Contract[I, O]) func() (rhandler.TypedHandler, error) {
	return func() (rhandler.TypedHandler, error) {
		if create == nil {
			return nil, fmt.Errorf("custom contract factory is required")
		}
		contract := create()
		if (xshape.Runtime{}).IsNil(contract) {
			return nil, fmt.Errorf("custom contract factory returned nil")
		}
		return New(contract), nil
	}
}
