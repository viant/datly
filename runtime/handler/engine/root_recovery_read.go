package engine

import (
	"context"
	"errors"
	"sync"
)

// Recovery reads are issued only after the existing root recovery admission.
// They do not revive the completed journal, transaction or mutation guard.
var ErrRootRecoveryReadOnly = errors.New("root mutation recovery admits only fresh canonical reads")

type rootRecoveryReadKey struct{}
type rootRecoveryDetachedKey struct{}
type rootRecoveryRead struct {
	mu        sync.Mutex
	origin    *dataScope
	active    bool
	violation error
	reads     map[*rootRecoveryReadExecution]struct{}
}

type rootRecoveryReadExecution struct{ cancel context.CancelCauseFunc }

func recoveryReadPermit(ctx context.Context) *rootRecoveryRead {
	p, _ := ctx.Value(rootRecoveryReadKey{}).(*rootRecoveryRead)
	return p
}
func rootScope(s *dataScope) *dataScope {
	if s != nil && s.root != nil {
		return s.root
	}
	return s
}
func beginRootRecoveryReads(ctx context.Context, origin *dataScope) (context.Context, func() error) {
	p := &rootRecoveryRead{origin: rootScope(origin), active: true, reads: make(map[*rootRecoveryReadExecution]struct{})}
	return context.WithValue(ctx, rootRecoveryReadKey{}, p), func() error {
		p.mu.Lock()
		p.active = false
		var pending []*rootRecoveryReadExecution
		for read := range p.reads {
			pending = append(pending, read)
		}
		if len(pending) > 0 {
			p.violation = errors.Join(p.violation, ErrRootRecoveryReadOnly)
		}
		failure := p.violation
		p.mu.Unlock()
		for _, read := range pending {
			read.cancel(ErrRootRecoveryReadOnly)
		}
		return failure
	}
}
func (p *rootRecoveryRead) open() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.active }
func (p *rootRecoveryRead) reject() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.violation = errors.Join(p.violation, ErrRootRecoveryReadOnly)
	return ErrRootRecoveryReadOnly
}
func detachRecoveryRead(call context.Context, p *rootRecoveryRead) context.Context {
	call = context.WithValue(call, rootRecoveryReadKey{}, p)
	call = context.WithValue(call, rootRecoveryDetachedKey{}, p)
	// Keep caller cancellation, values and provider scope; replace only the
	// finished native data/relationship. The reader opens a new ordinary unit.
	call = context.WithValue(call, dataScopeContextKey{}, (*dataScope)(nil))
	return context.WithValue(call, componentScopeKey{}, componentScope{})
}

// IsRootRecoveryRead reports a trusted detached recovery read to the dispatcher.
// No transport input can construct its private, hook-bounded permit.
func IsRootRecoveryRead(ctx context.Context) bool {
	p := recoveryReadPermit(ctx)
	return p != nil && ctx.Value(rootRecoveryDetachedKey{}) == p
}
func checkRootRecoveryRead(ctx context.Context, canonicalReader bool) error {
	p := recoveryReadPermit(ctx)
	if p == nil || !IsRootRecoveryRead(ctx) {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	failure := p.violation
	active := p.active
	p.mu.Unlock()
	if failure != nil {
		return failure
	}
	if !active || !canonicalReader {
		return p.reject()
	}
	return nil
}

// AdmitRootRecoveryRead bounds a detached canonical read to its recovery hook.
// Closing the hook cancels pending reads; completion cannot outlive that hook.
func AdmitRootRecoveryRead(ctx context.Context) (context.Context, func() error, error) {
	if !IsRootRecoveryRead(ctx) {
		return ctx, func() error { return nil }, nil
	}
	if err := checkRootRecoveryRead(ctx, true); err != nil {
		return nil, nil, err
	}
	p := recoveryReadPermit(ctx)
	p.mu.Lock()
	if !p.active || p.violation != nil {
		p.violation = errors.Join(p.violation, ErrRootRecoveryReadOnly)
		p.mu.Unlock()
		return nil, nil, ErrRootRecoveryReadOnly
	}
	readCtx, cancel := context.WithCancelCause(ctx)
	read := &rootRecoveryReadExecution{cancel: cancel}
	p.reads[read] = struct{}{}
	p.mu.Unlock()
	return readCtx, func() error {
		p.mu.Lock()
		delete(p.reads, read)
		if !p.active {
			p.violation = errors.Join(p.violation, ErrRootRecoveryReadOnly)
		}
		failure := errors.Join(p.violation, context.Cause(readCtx))
		p.mu.Unlock()
		cancel(nil)
		return failure
	}, nil
}
