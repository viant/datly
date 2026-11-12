package exec

import (
	"context"
	"github.com/viant/x/shape"
	"reflect"
	"sync"
)

// EffectiveContract describes caller-effective wire type identities. Runtime
// handlers retain their original linked application types and binding plans.
type EffectiveContract struct {
	Dynamic bool
	Input   reflect.Type
	Output  reflect.Type
	// Representations are immutable opt-in metadata over the linked types.
	InputRepresentation  *shape.Representation
	OutputRepresentation *shape.Representation
	// BindingState is an invocation-owned, opaque native binding handoff. It is
	// never wire metadata and must not be retained in a generation-level cache.
	BindingState any `json:"-" mcp:"-"`
}

// ContractResolver resolves metadata in the caller's native provider scope.
// Implementations must not execute the target component's business handler.
type ContractResolver interface {
	ResolveContract(context.Context, ComponentTarget, ProviderScope) (*EffectiveContract, error)
}

type contractCaptureKey struct{}
type contractCapture struct {
	mu     sync.RWMutex
	values map[ComponentTarget]EffectiveContract
}

// CaptureContracts installs transport-owned contract evidence for one request.
// It retains effective metadata and its operation-local binding handoff. Runtime
// completion releases resolved values; policy callbacks are never retained.
func CaptureContracts(ctx context.Context) context.Context {
	return context.WithValue(ctx, contractCaptureKey{}, &contractCapture{values: map[ComponentTarget]EffectiveContract{}})
}
func PublishContract(ctx context.Context, target ComponentTarget, value *EffectiveContract) {
	if capture, ok := ctx.Value(contractCaptureKey{}).(*contractCapture); ok && value != nil {
		capture.mu.Lock()
		capture.values[target] = *value
		capture.mu.Unlock()
	}
}
func SelectedContract(ctx context.Context, target ComponentTarget) (EffectiveContract, bool) {
	capture, ok := ctx.Value(contractCaptureKey{}).(*contractCapture)
	if !ok {
		return EffectiveContract{}, false
	}
	capture.mu.RLock()
	defer capture.mu.RUnlock()
	value, ok := capture.values[target]
	return value, ok
}
