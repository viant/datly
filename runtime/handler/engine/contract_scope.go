package engine

import (
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/provider/values"
	dexec "github.com/viant/datly/exec"
)

// ContractScope composes metadata binding with the same provider precedence and
// protected-kind checks as execution. It does not bind/initialize business input
// or start a data/transaction owner. Dependencies use the runtime's native provider.
func ContractScope(root *bindly.Injector, component []locator.Provider, protocol dexec.ProviderScope, constants, dependencies locator.Provider, targets ...dexec.ComponentTarget) (*bindly.Injector, error) {
	var providers []locator.Provider
	if protocol != nil {
		providers = protocol.Providers()
	}
	var owned []locator.Provider
	if len(targets) > 0 {
		target := targets[0]
		owned = append(owned, values.New("contract", map[string]any{"method": target.Route.Method, "path": target.Route.Path}))
	}
	if dependencies != nil {
		owned = append(owned, dependencies)
	}
	composed, err := (providerComposer{}).compose(providerComposition{component: component, protocol: providers, runtime: owned, constants: constants})
	if err != nil {
		return nil, err
	}
	return root.ForScope(withDefaultGenerator(root, composed)...)
}
