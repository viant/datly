package exec

import (
	"context"
	"reflect"
	"sync"

	xhandler "github.com/viant/xdatly/handler"
)

// OutputFieldFilter is an invocation's user projection, independent of loaded
// columns and per-row read evidence. Paths follow the native JSON encoder.
type OutputFieldFilter = xhandler.OutputFieldFilter

type outputSelection struct {
	mu        sync.RWMutex
	filter    OutputFieldFilter
	typeOf    reflect.Type
	sealed    bool
	active    bool
	root      bool
	lifecycle bool
	published bool
	valid     bool
}
type outputSelectionKey struct{}
type outputSelectionFrameKey struct{}

// CaptureOutputSelection installs a transport-owned result slot. Typed handler
// results remain unchanged; explicit response and custom marshal owners can ignore it.
func CaptureOutputSelection(ctx context.Context) context.Context {
	return context.WithValue(ctx, outputSelectionKey{}, &outputSelection{})
}

// CaptureChildOutputSelection isolates one explicitly delegated output read.
// The caller must publish SelectedOutputFields(childContext, childResult) for
// its own result, and is responsible for preserving the filter's field paths.
// Ordinary child/input/sibling reads never inherit this publication authority.
func CaptureChildOutputSelection(ctx context.Context) context.Context {
	if !WantsOutputSelection(ctx) {
		return ctx
	}
	ctx = context.WithValue(ctx, outputSelectionFrameKey{}, (*outputSelection)(nil))
	return CaptureOutputSelection(ctx)
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
		frame.valid = frame.published && err == nil && reflect.TypeOf(result) == frame.typeOf
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
	if frame.active && !frame.sealed && (frame.root || frame.lifecycle) {
		frame.filter, frame.typeOf = filter, reflect.TypeOf(result)
		frame.published = true
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
	return (frame.root || frame.lifecycle) && frame.active && !frame.sealed
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

// EnableLifecycleOutputSelection retains this invocation's successful reader
// mask independently of transport publication. Input reads are still excluded.
func EnableLifecycleOutputSelection(ctx context.Context) {
	frame, _ := ctx.Value(outputSelectionFrameKey{}).(*outputSelection)
	if frame == nil {
		return
	}
	frame.mu.Lock()
	defer frame.mu.Unlock()
	frame.lifecycle = true
}

// CurrentOutputSelection returns only the sealed mask for this exact output
// type. It never reads the transport target or an enclosing invocation's frame.
func CurrentOutputSelection(ctx context.Context, output any) (OutputFieldFilter, bool) {
	frame, _ := ctx.Value(outputSelectionFrameKey{}).(*outputSelection)
	if frame == nil {
		return nil, false
	}
	frame.mu.RLock()
	defer frame.mu.RUnlock()
	if !frame.sealed || !frame.valid || reflect.TypeOf(output) != frame.typeOf {
		return nil, false
	}
	return frame.filter, true
}

// SealOutputSelection freezes this invocation's reader evidence without
// publishing it to the transport. Response binding defers that publication
// until the final response has been classified.
func SealOutputSelection(ctx context.Context, result any, err error) {
	frame, _ := ctx.Value(outputSelectionFrameKey{}).(*outputSelection)
	if frame == nil {
		return
	}
	frame.mu.Lock()
	defer frame.mu.Unlock()
	frame.sealed = true
	frame.valid = frame.published && err == nil && reflect.TypeOf(result) == frame.typeOf
}
