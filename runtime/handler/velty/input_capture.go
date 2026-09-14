package velty

import (
	"context"
	"fmt"
	"reflect"
)

// WithInputCapture returns a configured copy sharing only the immutable compiled
// program. The callback must capture detached invocation-local state without
// mutating the input and must be safe for concurrent calls. A nil callback
// disables capture on the returned copy, leaving the receiver unchanged.
func (h *Handler[I, O]) WithInputCapture(capture func(context.Context, *I) (any, error)) *Handler[I, O] {
	if h == nil {
		return nil
	}
	result := *h
	result.captureInput = capture
	return &result
}

// CaptureInput implements the engine's opt-in pre-initialization adapter.
func (h *Handler[I, O]) CaptureInput(ctx context.Context, value any) (any, error) {
	if h == nil || h.program == nil {
		return nil, fmt.Errorf("velty handler is not initialized")
	}
	if h.captureInput == nil {
		return nil, nil
	}
	input, ok := value.(*I)
	if !ok || input == nil {
		return nil, fmt.Errorf("capture input must be *%s, got %T", reflect.TypeFor[I](), value)
	}
	return h.captureInput(ctx, input)
}
