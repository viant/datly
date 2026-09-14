package http

import (
	"context"
	"sync"
	"time"
)

// WarmupLifetime owns admission and completion across any handlers sharing it.
// It stores no handlers, generations or requests. Application.Manager supplies
// one instance to all its staged HTTP generations, including pinned generations.
type WarmupLifetime struct {
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	stopped bool
	active  sync.WaitGroup
	done    chan struct{}
}

func NewWarmupLifetime(parent context.Context) *WarmupLifetime {
	ctx, cancel := context.WithCancel(parent)
	return &WarmupLifetime{ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

func (l *WarmupLifetime) begin(timeout time.Duration) (context.Context, context.CancelFunc, bool) {
	if l == nil || l.ctx == nil {
		return nil, nil, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped || l.ctx.Err() != nil {
		return nil, nil, false
	}
	l.active.Add(1)
	ctx, cancel := context.WithTimeout(l.ctx, timeout)
	return ctx, func() { cancel(); l.active.Done() }, true
}

// Shutdown rejects new accepted operations, cancels authorization, preparation
// and execution, then joins all previously accepted work. Repeated calls can wait
// again after an earlier caller deadline. Callbacks must honor operation context.
func (l *WarmupLifetime) Shutdown(ctx context.Context) error {
	if l == nil || l.ctx == nil {
		return nil
	}
	l.mu.Lock()
	if !l.stopped {
		l.stopped = true
		l.cancel()
		go func() { l.active.Wait(); close(l.done) }()
	}
	l.mu.Unlock()
	select {
	case <-l.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *WarmupLifetime) available() bool {
	if l == nil || l.ctx == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.stopped && l.ctx.Err() == nil
}
