package cubecompose

import (
	"context"
	"time"
)

// FrameContext lets period-aware source predicates interpret elapsed alignment.
// All frames in a request share Snapshot; Datly does not infer business time rules.
type FrameContext struct {
	Frame     int
	Alignment string
	Snapshot  time.Time
}
type frameContextKey struct{}

func WithFrameContext(ctx context.Context, frame FrameContext) context.Context {
	return context.WithValue(ctx, frameContextKey{}, frame)
}
func FrameContextFrom(ctx context.Context) (FrameContext, bool) {
	frame, ok := ctx.Value(frameContextKey{}).(FrameContext)
	return frame, ok
}
