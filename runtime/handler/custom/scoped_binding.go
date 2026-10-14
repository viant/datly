package custom

import (
	"fmt"
	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
	"reflect"
)

// ScopedInputBinding trusts only the private typed adapter, never a raw
// runtime handler with a coincidentally named application method.
func ScopedInputBinding(handler rhandler.Handler) (bool, error) {
	if policy, ok := handler.(interface{ scopedInputBinding() (bool, error) }); ok {
		return policy.scopedInputBinding()
	}
	return false, nil
}
func (h *contractHandler[I, O]) scopedInputBinding() (bool, error) { return h.scoped, h.policyErr }

func scopedPolicy[I, O any](contract xhandler.Contract[I, O]) (enabled bool, err error) {
	defer func() {
		if recover() != nil {
			enabled = false
			err = fmt.Errorf("scoped input binding policy panicked")
		}
	}()
	policy, ok := contract.(xhandler.ScopedInputBinder)
	if !ok || !policy.BindsInputByScope() {
		return false, nil
	}
	reject := func() (bool, error) {
		return true, fmt.Errorf("scoped input binding is incompatible with capture, initialization, read metadata or mutation/composition lifecycle")
	}
	if _, ok := contract.(xhandler.InputCapturer[I]); ok {
		return reject()
	}
	if consumer, ok := contract.(xhandler.ReadMetadataConsumer); ok && consumer.RequiresReadMetadata() {
		return reject()
	}
	if _, ok := contract.(rhandler.OutcomeFinalizer); ok {
		return reject()
	}
	if _, ok := contract.(rhandler.MutationRecoverer); ok {
		return reject()
	}
	if _, ok := contract.(rhandler.TransactionRetryer); ok {
		return reject()
	}
	if _, ok := contract.(rhandler.ScopedMutationRecoverer); ok {
		return reject()
	}
	if _, ok := contract.(rhandler.CapturedExecutionGuard); ok {
		return reject()
	}
	if _, ok := contract.(rhandler.PreBindingTransaction); ok {
		return reject()
	}
	input := reflect.TypeFor[*I]()
	for _, kind := range []reflect.Type{reflect.TypeFor[xhandler.Initializer](), reflect.TypeFor[xhandler.InvokerInitializer](), reflect.TypeFor[xmcp.Initializer]()} {
		if input.Implements(kind) {
			return reject()
		}
	}
	output := reflect.TypeFor[*O]()
	for _, kind := range []reflect.Type{reflect.TypeFor[xhandler.InvokerFinalizer](), reflect.TypeFor[xhandler.InvokerErrorFinalizer](), reflect.TypeFor[xhandler.InjectorFinalizer]()} {
		if output.Implements(kind) {
			return reject()
		}
	}
	return true, nil
}
