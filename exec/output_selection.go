package exec

import (
	"context"
	"reflect"
	"sync"
)

// OutputFieldFilter is an invocation's user projection, independent of loaded
// columns and per-row read evidence. Paths follow the native JSON encoder.
type OutputFieldFilter interface{ ExcludePath([]string, string) bool }

type outputSelection struct {
	mu     sync.RWMutex
	filter OutputFieldFilter
	typeOf reflect.Type
	sealed bool
	active bool
	root   bool
}
type outputSelectionKey struct{}
type outputSelectionFrameKey struct{}

// CaptureOutputSelection installs a transport-owned result slot. Typed handler
// results remain unchanged; explicit response and custom marshal owners can ignore it.
func CaptureOutputSelection(ctx context.Context) context.Context {
	return context.WithValue(ctx, outputSelectionKey{}, &outputSelection{})
}

// ScopeOutputSelection shadows parent state at every canonical component entry.
// Only the outer frame can publish to the transport, after its handler returns.
func ScopeOutputSelection(ctx context.Context) (context.Context, func(any, error)) {
	parent, _ := ctx.Value(outputSelectionFrameKey{}).(*outputSelection)
	target, _ := ctx.Value(outputSelectionKey{}).(*outputSelection)
	if parent == nil && target != nil {
		target.mu.Lock()
		target.filter, target.typeOf = nil, nil
		target.mu.Unlock()
	}
	frame := &outputSelection{root: parent == nil && target != nil}
	ctx = context.WithValue(ctx, outputSelectionFrameKey{}, frame)
	return ctx, func(result any, err error) {
		frame.mu.Lock()
		defer frame.mu.Unlock()
		frame.sealed = true
		if parent != nil || target == nil || err != nil || reflect.TypeOf(result) != frame.typeOf {
			return
		}
		target.mu.Lock()
		defer target.mu.Unlock()
		target.filter, target.typeOf = frame.filter, frame.typeOf
	}
}

// PublishOutputSelection records a completed typed reader result in this frame.
// A sealed frame ignores reads initiated by output finalizers.
func PublishOutputSelection(ctx context.Context, result any, filter OutputFieldFilter) {
	frame, _ := ctx.Value(outputSelectionFrameKey{}).(*outputSelection)
	if frame == nil {
		return
	}
	frame.mu.Lock()
	defer frame.mu.Unlock()
	if frame.active && !frame.sealed && frame.root {
		frame.filter, frame.typeOf = filter, reflect.TypeOf(result)
	}
}

func SelectedOutputFields(ctx context.Context, value any) OutputFieldFilter {
	target, _ := ctx.Value(outputSelectionKey{}).(*outputSelection)
	if target == nil {
		return nil
	}
	target.mu.RLock()
	defer target.mu.RUnlock()
	if reflect.TypeOf(value) != target.typeOf {
		return nil
	}
	return target.filter
}

// WantsOutputSelection lets ordinary typed reader calls avoid wire compilation.
func WantsOutputSelection(ctx context.Context) bool {
	frame, _ := ctx.Value(outputSelectionFrameKey{}).(*outputSelection)
	if frame == nil {
		return false
	}
	frame.mu.RLock()
	defer frame.mu.RUnlock()
	return frame.root && frame.active && !frame.sealed
}

// BeginOutputSelection excludes input preparation reads from output selection.
func BeginOutputSelection(ctx context.Context) {
	frame, _ := ctx.Value(outputSelectionFrameKey{}).(*outputSelection)
	if frame == nil {
		return
	}
	frame.mu.Lock()
	defer frame.mu.Unlock()
	frame.active = true
}
