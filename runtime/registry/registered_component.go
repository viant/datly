package registry

import (
	documentation "github.com/viant/datly/documentation"
	"reflect"

	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
)

// RegisteredComponent is the minimal runtime registration carrier still needed
// by the thin root runtime facade while richer registration behavior is moved
// out of root.
type RegisteredComponent struct {
	Documentation *documentation.Snapshot
	Component     *spec.Component
	Input         *InputContract
	OutputType    reflect.Type
	Output        *OutputContract
	Reader        dexec.Reader
	Handler       rhandler.Handler
	Capabilities  rhandler.InvocationCapabilities
	Providers     []locator.Provider
	DataSource    dexec.DataSource
}
