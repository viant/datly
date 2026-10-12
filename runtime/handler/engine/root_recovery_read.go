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
}

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
	p := &rootRecoveryRead{origin: rootScope(origin), active: true}
	return context.WithValue(ctx, rootRecoveryReadKey{}, p), func() error { p.mu.Lock(); defer p.mu.Unlock(); p.active = false; return p.violation }
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
