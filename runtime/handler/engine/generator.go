package engine

import (
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
)

func withDefaultGenerator(root *bindly.Injector, providers []locator.Provider) []locator.Provider {
	for _, provider := range providers {
		if provider.Kind() == "generator" {
			return providers
		}
	}
	if root.HasProvider("generator") {
		return providers
	}
	return append(providers, handlerprovider.Generator())
}
