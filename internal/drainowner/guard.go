package drainowner

import (
	"context"
	"errors"
	"sync"
)

var ErrGuardBinding = errors.New("execution guard requires its exact native registration")
var ErrGuardScope = errors.New("execution guard purpose is missing, foreign or expired")

// GuardBinding identifies one registration. It grants no mutation or drain
// authority. Copies do not identify the original registration.
type GuardBinding struct{ cell *guardBindingCell }
type guardBindingCell struct {
	mu    sync.Mutex
	self  *GuardBinding
	owner *State
}
type guardScope struct {
	mu       sync.Mutex
	binding  *guardBindingCell
	owner    *State
	terminal bool
	active   bool
}
type guardContextKey struct{}

func NewGuardBinding() *GuardBinding {
	b := &GuardBinding{cell: &guardBindingCell{}}
	b.cell.self = b
	return b
}

// BindExecutionGuard is called only by native registration, after its ordinary
// registration preconditions pass. A binding cannot be registered twice.
func BindExecutionGuard(receiver any, binding *GuardBinding) error {
	state, err := exactState(receiver)
	if err != nil {
		return err
	}
	if binding == nil || binding.cell == nil {
		return ErrGuardBinding
	}
	cell := binding.cell
	cell.mu.Lock()
	defer cell.mu.Unlock()
	if cell.self != binding || cell.owner != nil {
		return ErrGuardBinding
	}
	cell.owner = state
	return nil
}

// WithGuardPurpose encloses exactly one native callback invocation. Native DML
// derives terminal from its completion boundary, never from component input.
// The returned close function must be deferred before invoking the callback.
func WithGuardPurpose(ctx context.Context, receiver any, binding *GuardBinding, terminal bool) (context.Context, func(), error) {
	state, err := exactState(receiver)
	if err != nil {
		return nil, nil, err
	}
	if ctx == nil || binding == nil || binding.cell == nil {
		return nil, nil, ErrGuardBinding
	}
	cell := binding.cell
	cell.mu.Lock()
	valid := cell.self == binding && cell.owner == state
	cell.mu.Unlock()
	if !valid {
		return nil, nil, ErrGuardBinding
	}
	scope := &guardScope{binding: cell, owner: state, terminal: terminal, active: true}
	close := func() { scope.mu.Lock(); scope.active = false; scope.mu.Unlock() }
	return context.WithValue(ctx, guardContextKey{}, scope), close, nil
}

// GuardPurpose verifies evidence for the exact expected registration. A child
// context may preserve the evidence only during the callback's dynamic scope.
func GuardPurpose(ctx context.Context, binding *GuardBinding) (bool, error) {
	if ctx == nil || binding == nil || binding.cell == nil {
		return false, ErrGuardScope
	}
	cell := binding.cell
	cell.mu.Lock()
	owner, valid := cell.owner, cell.self == binding
	cell.mu.Unlock()
	if !valid || owner == nil {
		return false, ErrGuardScope
	}
	scope, ok := ctx.Value(guardContextKey{}).(*guardScope)
	if !ok || scope == nil {
		return false, ErrGuardScope
	}
	scope.mu.Lock()
	defer scope.mu.Unlock()
	if !scope.active || scope.binding != cell || scope.owner != owner {
		return false, ErrGuardScope
	}
	return scope.terminal, nil
}

// GuardRegistered reports only exact native registration, never completion.
func GuardRegistered(binding *GuardBinding) bool {
	if binding == nil || binding.cell == nil {
		return false
	}
	cell := binding.cell
	cell.mu.Lock()
	defer cell.mu.Unlock()
	return cell.self == binding && cell.owner != nil
}
