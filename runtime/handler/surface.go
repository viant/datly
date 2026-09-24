package handler

import xhandler "github.com/viant/xdatly/handler"

const (
	// DifferCapabilityKey exposes typed comparisons through scoped DI.
	DifferCapabilityKey = xhandler.DifferKey
	// LoggerCapabilityKey exposes the invocation logger through the narrow binder
	// lookup surface for internal runtime-owned DI.
	LoggerCapabilityKey = xhandler.LoggerKey
	// ValidatorCapabilityKey exposes the invocation validator through the narrow
	// binder lookup surface for internal runtime-owned DI.
	ValidatorCapabilityKey = xhandler.ValidatorKey
	// MessageBusCapabilityKey exposes the invocation message bus through the
	// narrow binder lookup surface for internal runtime-owned DI.
	MessageBusCapabilityKey = xhandler.MessageBusKey
	// ConnectorCapabilityKey identifies the explicitly granted named DB provider.
	ConnectorCapabilityKey = xhandler.ConnectorKey
	// RemoteMapperCapabilityKey selects the runtime-owned mapper through native DI.
	RemoteMapperCapabilityKey xhandler.ValueKey = "remote_mapper"
)

type ValidationService = xhandler.Validator

type MessageBusService = xhandler.MessageBus

// InvocationCapabilities groups DI-bindable runtime capabilities that handlers
// may need, but which should not expand the thin public session facade.
type InvocationCapabilities = xhandler.Capabilities
