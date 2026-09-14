package bootstrap

import (
	"fmt"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/runtime/registry"
)

// Registration combines artifact-owned metadata/named handler selection with
// explicit application capabilities. It never resolves factories again.
func (a *Artifact) Registration(runtime registry.RegisteredComponent) (*registry.RegisteredComponent, error) {
	if a == nil || a.Component == nil || a.Input == nil {
		return nil, fmt.Errorf("compiled component artifact is incomplete")
	}
	if runtime.Component != nil || runtime.Input != nil || runtime.Output != nil || runtime.OutputType != nil {
		return nil, fmt.Errorf("registration metadata is owned by the compiled artifact")
	}
	if a.Handler != nil {
		if runtime.Handler != nil {
			return nil, fmt.Errorf("runtime component %s has both explicit and named factory handlers", a.Component.Key.String())
		}
		runtime.Handler = a.Handler
	}
	if runtime.Handler == nil && runtime.Reader == nil {
		return nil, fmt.Errorf("runtime component %s requires a handler or reader", a.Component.Key.String())
	}
	runtime.Component = a.Component.Clone()
	runtime.Documentation = a.Documentation
	runtime.Input = a.Input
	runtime.Output = a.Output
	runtime.OutputType = a.outputType
	runtime.Providers = append([]locator.Provider(nil), runtime.Providers...)
	return &runtime, nil
}
