package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/viant/bindly/locator"
	"github.com/viant/structology"
)

// ErrWriteEligibilityMutation reports an attempted managed mutation while a
// writer is evaluating its read-only eligibility hook.
var ErrWriteEligibilityMutation = errors.New("managed mutation is forbidden during WriteEligible")

type mutationGuard struct {
	mu        sync.Mutex
	depth     int
	violation error
}

func (s *dataScope) mutationGuard() *mutationGuard {
	if s == nil {
		return nil
	}
	if s.root != nil {
		s = s.root
	}
	return &s.writeEligibility
}

func (g *mutationGuard) check(operation string) error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.depth == 0 {
		return nil
	}
	err := fmt.Errorf("%w: %s", ErrWriteEligibilityMutation, operation)
	if g.violation == nil {
		g.violation = err
	}
	return err
}

// BeginWriteEligibility enters the current invocation's managed read-only
// boundary. The caller must defer finish, which unwinds once and returns any
// attempted violation even if the hook discarded the capability's error.
// This protects managed mutation APIs; it does not sandbox arbitrary Go or SQL.
func BeginWriteEligibility(ctx context.Context) (finish func() error, err error) {
	scope, _ := ctx.Value(dataScopeContextKey{}).(*dataScope)
	guard := scope.mutationGuard()
	if guard == nil {
		return nil, errors.New("WriteEligible requires an invocation-owned data scope")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	guard.mu.Lock()
	guard.depth++
	guard.mu.Unlock()
	var once sync.Once
	var violation error
	return func() error {
		once.Do(func() {
			guard.mu.Lock()
			guard.depth--
			violation = guard.violation
			guard.mu.Unlock()
		})
		return violation
	}, nil
}

// RetainMutationAuthority captures an invocation's data scope for a managed
// component invoker. Restoring it prevents a replacement call context from
// dropping the owning guard and keeps reader dependencies in that invocation.
func RetainMutationAuthority(ctx context.Context) func(context.Context) context.Context {
	scope, _ := ctx.Value(dataScopeContextKey{}).(*dataScope)
	return func(call context.Context) context.Context {
		if scope == nil || !scope.mutationGuard().active() {
			return call
		}
		return withDataScope(call, scope)
	}
}

// CheckComponentMutation admits canonical readers while rejecting writers and
// unknown custom effects before child binding or data-unit creation.
func CheckComponentMutation(ctx context.Context, canonicalReader bool) error {
	if canonicalReader {
		return nil
	}
	scope, _ := ctx.Value(dataScopeContextKey{}).(*dataScope)
	return scope.mutationGuard().check("component invocation")
}

// Retain at provider installation as well as value binding: the first lookup
// itself may use a replaced context. These are framework component providers.
func retainProviderMutationAuthority(ctx context.Context, provider locator.Provider, capture bool) locator.Provider {
	authority := RetainMutationAuthority(ctx)
	if capture {
		// Value lookup captures authority but does not execute a component. Keep
		// dispatch restoration conditional so ordinary replaced-context calls
		// retain their previous ownership behavior outside WriteEligible.
		scope, _ := ctx.Value(dataScopeContextKey{}).(*dataScope)
		authority = func(call context.Context) context.Context {
			if scope == nil {
				return call
			}
			return withDataScope(call, scope)
		}
	}
	return mutationAuthorityProvider{Provider: provider, authority: authority}
}

type mutationAuthorityProvider struct {
	locator.Provider
	authority func(context.Context) context.Context
}

func (p mutationAuthorityProvider) DefaultCacheable() bool {
	if policy, ok := p.Provider.(locator.CachePolicy); ok {
		return policy.DefaultCacheable()
	}
	return false
}
func (p mutationAuthorityProvider) Locate(state *structology.State) locator.Locator {
	return mutationAuthorityLocator{Locator: p.Provider.Locate(state), authority: p.authority}
}

type mutationAuthorityLocator struct {
	locator.Locator
	authority func(context.Context) context.Context
}

func (l mutationAuthorityLocator) Value(ctx context.Context, target reflect.Type, name string) (any, bool, error) {
	return l.Locator.Value(l.authority(ctx), target, name)
}

func (g *mutationGuard) active() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.depth != 0
}
