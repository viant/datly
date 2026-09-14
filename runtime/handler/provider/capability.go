package provider

import (
	"github.com/viant/bindly/locator"
	rhandler "github.com/viant/datly/runtime/handler"
)

// Capabilities adapts present invocation capabilities to canonical Bindly
// providers. Absent values are omitted so parent-scope defaults remain visible.
func Capabilities(capabilities rhandler.InvocationCapabilities) []locator.Provider {
	result := make([]locator.Provider, 0, 5)
	if hasValue(capabilities.Differ) {
		result = append(result, Static(rhandler.DifferCapabilityKey, capabilities.Differ))
	}
	if hasValue(capabilities.Logger) {
		result = append(result, Static(rhandler.LoggerCapabilityKey, capabilities.Logger))
	}
	if hasValue(capabilities.Validator) {
		result = append(result, Static(rhandler.ValidatorCapabilityKey, capabilities.Validator))
	}
	if hasValue(capabilities.MessageBus) {
		result = append(result, Static(rhandler.MessageBusCapabilityKey, capabilities.MessageBus))
	}
	if hasValue(capabilities.Connector) {
		result = append(result, Static(rhandler.ConnectorCapabilityKey, capabilities.Connector))
	}
	return result
}
