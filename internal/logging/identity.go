// Package logging holds internal compatibility logging evidence. Identity
// observation is diagnostic only and must never be used for authorization.
package logging

import (
	"context"
	"sync"
)

// Identity is a copied, scalar subset of successfully verified claims.
// It deliberately contains no credentials, claims pointers or subject binding.
type Identity struct {
	UserID   int
	Username string
	Email    string
	Scope    string
}

type identityKey struct{}
type identityObservation struct {
	mu       sync.Mutex
	identity Identity
	verified bool
	conflict bool
}

// WithIdentityObservation opts a request into diagnostic identity observation.
// Nested owners share existing evidence; they cannot reset a conflict.
func WithIdentityObservation(ctx context.Context) context.Context {
	if _, ok := ctx.Value(identityKey{}).(*identityObservation); ok {
		return ctx
	}
	return context.WithValue(ctx, identityKey{}, &identityObservation{})
}

// ObserveIdentity records only successful verifier evidence. Callers must first
// complete signature verification and every configured claim-policy check.
// Conflicting evidence permanently suppresses attribution for this request.
func ObserveIdentity(ctx context.Context, identity Identity) {
	state, _ := ctx.Value(identityKey{}).(*identityObservation)
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.conflict {
		return
	}
	if state.verified {
		if state.identity != identity {
			state.identity = Identity{}
			state.verified = false
			state.conflict = true
		}
		return
	}
	state.identity, state.verified = identity, true
}

// IdentitySnapshot returns detached evidence for the outer logging owner.
// A conflict must produce at most one fixed diagnostic at request completion,
// without claim values. No diagnostic callback executes under the state lock.
func IdentitySnapshot(ctx context.Context) (identity Identity, verified, conflict bool) {
	state, _ := ctx.Value(identityKey{}).(*identityObservation)
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.identity, state.verified, state.conflict
}
