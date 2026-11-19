package requesttrace

import "context"

type contextKey struct{}

// Ensure stores the root request trace ID on the context if it is not already set.
func Ensure(ctx context.Context, traceID string) context.Context {
	if ctx == nil || traceID == "" {
		return ctx
	}
	if Current(ctx) != "" {
		return ctx
	}
	return context.WithValue(ctx, contextKey{}, traceID)
}

// Current returns the root request trace ID stored on the context.
func Current(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	value := ctx.Value(contextKey{})
	if value == nil {
		return ""
	}
	traceID, _ := value.(string)
	return traceID
}
