package exec

import (
	"context"

	xhandler "github.com/viant/xdatly/handler"
)

// InvocationKey is the reserved binder key for runtime-owned invocation
// metadata. Protocol and component providers cannot override it.
const InvocationKey xhandler.ValueKey = "invocation"

// InvocationPurpose identifies why the runtime is executing a component.
type InvocationPurpose uint8

const (
	InvocationPurposeRequest InvocationPurpose = iota
	InvocationPurposeCacheWarmup
)

// WarmupPhase distinguishes validation from the operation that fills caches.
type WarmupPhase uint8

const (
	WarmupPhaseNone WarmupPhase = iota
	WarmupPhasePrepare
	WarmupPhaseFill
)

// InvocationInfo is immutable runtime-owned execution metadata. Its fields are
// intentionally private so a bound predicate can inspect, but not modify, the
// authorization capability selected by the runtime.
type InvocationInfo struct {
	purpose                   InvocationPurpose
	warmupPhase               WarmupPhase
	mayBypassRowAuthorization bool
}

func (i *InvocationInfo) Purpose() InvocationPurpose {
	if i == nil {
		return InvocationPurposeRequest
	}
	return i.purpose
}

func (i *InvocationInfo) IsCacheWarmup() bool {
	return i != nil && i.purpose == InvocationPurposeCacheWarmup
}

func (i *InvocationInfo) WarmupPhase() WarmupPhase {
	if i == nil {
		return WarmupPhaseNone
	}
	return i.warmupPhase
}

// MayBypassRowAuthorization reports the explicit runtime capability; callers
// should not infer authorization policy from IsCacheWarmup alone.
func (i *InvocationInfo) MayBypassRowAuthorization() bool {
	return i != nil && i.mayBypassRowAuthorization
}

type invocationInfoKey struct{}

var requestInvocation = &InvocationInfo{purpose: InvocationPurposeRequest}

// WithCacheWarmup marks a trusted server-owned warmup phase. The public API is
// intended for runtime implementations; transport values never reach it.
func WithCacheWarmup(ctx context.Context, phase WarmupPhase) context.Context {
	mayBypass := phase == WarmupPhasePrepare || phase == WarmupPhaseFill
	return context.WithValue(ctx, invocationInfoKey{}, &InvocationInfo{
		purpose:                   InvocationPurposeCacheWarmup,
		warmupPhase:               phase,
		mayBypassRowAuthorization: mayBypass,
	})
}

// InvocationFromContext returns the same descriptor exposed by InvocationKey.
func InvocationFromContext(ctx context.Context) *InvocationInfo {
	if ctx != nil {
		if result, ok := ctx.Value(invocationInfoKey{}).(*InvocationInfo); ok && result != nil {
			return result
		}
	}
	return requestInvocation
}

func IsCacheWarmup(ctx context.Context) bool {
	return InvocationFromContext(ctx).IsCacheWarmup()
}
