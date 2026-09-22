package exec

import (
	"context"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
)

const ComponentInvokerKey xhandler.ValueKey = "component_invoker"

// ProviderScope supplies invocation-local providers without coupling protocol
// adapters to the concrete runtime engine.
type ProviderScope interface {
	Providers() []locator.Provider
}

// ComponentTarget identifies one exact component route.
type ComponentTarget struct {
	Component spec.Key
	Route     spec.RouteRef
}

func (t ComponentTarget) String() string {
	return t.Component.String() + " " + t.Route.String()
}

// ComponentRequest invokes an exact target. Input is reserved for a trusted
// caller that already owns the canonical typed component input.
type ComponentRequest struct {
	Target ComponentTarget
	Input  any
	// Replay is a native binding seed, never pre-bound component input.
	Replay *bindly.ReplayBinding
	// BindingOutput supplies the calling output to explicitly declared output bindings.
	BindingOutput any
	Providers     []locator.Provider
	// Completion observes canonical managed-transaction evidence for this root.
	// It is trusted service metadata and never client-bindable.
	Completion func(xhandler.Outcome)
	// PrepareQuery requests a bound root query through the same input lifecycle.
	// It is a trusted component capability, never a transport request option.
	PrepareQuery bool
	// DryRun plans an ordinary reader root without opening a row reader.
	// Input initialization, providers and SQL templates still run.
	DryRun bool
	// ReaderOptions apply after canonical input binding, to the target reader's
	// result graph. Dependency binding retains its own invocation context policy.
	ReaderOptions *ReaderOptions
	// WarmupOmitInput names trusted warmup-owned inputs that are intentionally
	// absent from canonical transport binding, such as an indexed warmup key.
	WarmupOmitInput []string
	Warmup          *ReaderWarmupRequest
}

type ComponentInvoker interface {
	InvokeComponent(context.Context, ComponentRequest) (any, error)
}
