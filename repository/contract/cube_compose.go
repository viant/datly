package contract

import (
	"context"
	"time"
)

const CubeComposeAlignmentElapsed = "elapsed"

// CubeComposeFrameContext describes one internally prepared composition frame.
// Custom predicates may use it to align time windows while the Datly
// composition engine remains independent of business-specific calendars.
type CubeComposeFrameContext struct {
	Frame     int
	Alignment string
	Snapshot  time.Time
}

type cubeComposeFrameContextKey struct{}

// WithCubeComposeFrameContext attaches composition metadata to an internal
// frame preparation context.
func WithCubeComposeFrameContext(ctx context.Context, frame CubeComposeFrameContext) context.Context {
	return context.WithValue(ctx, cubeComposeFrameContextKey{}, frame)
}

// CubeComposeFrameContextFrom returns composition metadata when query
// preparation is running as part of a CubeCompose request.
func CubeComposeFrameContextFrom(ctx context.Context) (CubeComposeFrameContext, bool) {
	if ctx == nil {
		return CubeComposeFrameContext{}, false
	}
	frame, ok := ctx.Value(cubeComposeFrameContextKey{}).(CubeComposeFrameContext)
	return frame, ok
}
