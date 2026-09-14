package mutation

import (
	"fmt"
	xshape "github.com/viant/x/shape"

	rhandler "github.com/viant/datly/runtime/handler"
	policy "github.com/viant/xdatly/handler/mutation"
)

// Factory links a public Definition factory without reflective policy calls.
// The definition is created once per build; Capture still creates each Program.
func Factory[I, O any](create func() policy.Definition[I, O]) func() (rhandler.TypedHandler, error) {
	return func() (rhandler.TypedHandler, error) {
		if create == nil {
			return nil, fmt.Errorf("mutation definition factory is required")
		}
		definition := create()
		if (xshape.Runtime{}).IsNil(definition) {
			return nil, fmt.Errorf("mutation definition factory returned nil")
		}
		return New(definition), nil
	}
}
