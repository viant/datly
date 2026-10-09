package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/structology"
)

// ErrWriteEligibilityMutation reports an attempted managed mutation while a
// writer is evaluating its read-only eligibility hook.
var ErrWriteEligibilityMutation = errors.New("managed mutation is forbidden during WriteEligible")

type mutationGuard struct {
	mu                       sync.Mutex
	depth                    int
	reconciliationDepth      int
	reconciliationReplayVeto bool
	completionClosed         bool
	protectedCompletion      bool
	protectedIssuer          *drainowner.Invocation
	guardedExecution         bool
	streamingQueryUsed       bool
	guardedFailure           error
	violation                error
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
	defer g.unlockAndPublishProtectedFailure()
	if drainowner.BindingGroupOpen(g.protectedIssuer) {
		err := fmt.Errorf("%w: %s", drainowner.ErrBindingGroup, operation)
		g.guardedFailure = errors.Join(g.guardedFailure, err)
		return err
	}
	if g.completionClosed {
		err := fmt.Errorf("invocation mutation admission is closed: %s", operation)
		if g.protectedCompletion {
			g.guardedFailure = errors.Join(g.guardedFailure, err)
		}
		return err
	}
	if g.guardedFailure != nil {
		return g.guardedFailure
	}
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
// component invoker. Buffered callers retain their exact journal ownership
// throughout the call lifetime. Restoring it prevents a replacement call context from
// dropping the owning guard and keeps reader dependencies in that invocation.
func RetainMutationAuthority(ctx context.Context) func(context.Context) context.Context {
	scope, _ := ctx.Value(dataScopeContextKey{}).(*dataScope)
	return func(call context.Context) context.Context {
		if scope == nil || (!scope.requireBufferedOwner && scope.protectedAdmissionFailure() == nil && !scope.bufferedLifetimeClosed() && !scope.mutationGuard().active()) {
			return call
		}
		return withDataScope(call, scope)
	}
}

// CheckComponentMutation admits canonical readers while rejecting writers and
// unknown custom effects before child binding or data-unit creation.
func CheckComponentMutation(ctx context.Context, canonicalReader bool) error {
	scope, _ := ctx.Value(dataScopeContextKey{}).(*dataScope)
	if drainowner.BindingGroupContext(ctx) {
		if !canonicalReader || scope == nil {
			return drainowner.ErrBindingGroup
		}
		root := scope
		if root.root != nil {
			root = root.root
		}
		root.mu.Lock()
		issuer := root.nativeInvocation
		failure := root.guardedFailure
		root.mu.Unlock()
		if failure != nil {
			return failure
		}
		return drainowner.BindingGroupAdmission(ctx, issuer)
	}
	if failure := scope.protectedAdmissionFailure(); failure != nil {
		return failure
	}
	if componentFromContext(ctx).relation == ComponentBufferedImperative && (scope == nil || !scope.requireBufferedOwner) {
		return fmt.Errorf("buffered component invocation requires retained canonical caller ownership")
	}
	if scope != nil && scope.bufferedLifetimeClosed() {
		return scope.rejectProtected(fmt.Errorf("buffered component invocation has completed"))
	}
	if canonicalReader {
		guard := scope.mutationGuard()
		if guard != nil && guard.reconciliationActive() {
			return guard.check("component invocation")
		}
		if guard == nil {
			return nil
		}
		guard.mu.Lock()
		closed := guard.completionClosed
		guard.mu.Unlock()
		if closed {
			return guard.check("component invocation")
		}
		return nil
	}
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
	return g.depth != 0 || g.completionClosed || g.guardedExecution || drainowner.BindingGroupOpen(g.protectedIssuer)
}

func (g *mutationGuard) closeCompletion(protected ...bool) {
	g.mu.Lock()
	g.completionClosed = true
	if len(protected) == 1 && protected[0] {
		g.protectedCompletion = true
	}
	g.mu.Unlock()
}

// Proxy admission linearizes invocation-wide opt-in before driver dispatch.
func (g *mutationGuard) admitStreamingQuery() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.unlockAndPublishProtectedFailure()
	if drainowner.BindingGroupOpen(g.protectedIssuer) {
		err := fmt.Errorf("%w: streaming SQL", drainowner.ErrBindingGroup)
		g.guardedFailure = errors.Join(g.guardedFailure, err)
		return err
	}
	if g.completionClosed {
		err := errors.New("invocation mutation admission is closed")
		if g.protectedCompletion {
			g.guardedFailure = errors.Join(g.guardedFailure, err)
		}
		return err
	}
	var reconciliationErr error
	if g.reconciliationDepth != 0 {
		reconciliationErr = fmt.Errorf("%w: SQL query during ReconcileInput", ErrWriteEligibilityMutation)
		if g.violation == nil {
			g.violation = reconciliationErr
		}
	}
	if g.guardedExecution {
		if g.guardedFailure == nil {
			g.guardedFailure = errors.New("managed streaming SQL is unsupported in a captured writer invocation")
		}
		return g.guardedFailure
	}
	if reconciliationErr != nil {
		return reconciliationErr
	}
	g.streamingQueryUsed = true
	return nil
}
func (g *mutationGuard) enableCapturedExecution() error {
	g.mu.Lock()
	defer g.unlockAndPublishProtectedFailure()
	g.guardedExecution = true
	if g.streamingQueryUsed && g.guardedFailure == nil {
		g.guardedFailure = errors.New("managed streaming SQL predates a captured writer invocation")
	}
	return g.guardedFailure
}
func (g *mutationGuard) capturedExecutionFailure() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.guardedFailure
}

// Called with g.mu held; publish only after releasing the proxy lock.
func (g *mutationGuard) unlockAndPublishProtectedFailure() {
	issuer, failure := g.protectedIssuer, g.guardedFailure
	g.mu.Unlock()
	if issuer != nil && failure != nil {
		drainowner.FailProtected(issuer, failure)
	}
}
func (g *mutationGuard) bindProtectedIssuer(issuer *drainowner.Invocation) error {
	if !drainowner.ActivitiesEnrolled(issuer) {
		return nil
	}
	g.mu.Lock()
	if g.protectedIssuer != nil && g.protectedIssuer != issuer {
		g.mu.Unlock()
		return errors.New("proxy guard already belongs to another invocation")
	}
	g.protectedIssuer = issuer
	g.unlockAndPublishProtectedFailure()
	return nil
}

// BeginReconciliation reuses native guarded capabilities and additionally
// denies reader invocations and SQL reads throughout the finite callback.
func BeginReconciliation(ctx context.Context) (func() error, error) {
	finish, err := BeginWriteEligibility(ctx)
	if err != nil {
		return nil, err
	}
	scope, _ := ctx.Value(dataScopeContextKey{}).(*dataScope)
	guard := scope.mutationGuard()
	guard.mu.Lock()
	guard.reconciliationDepth++
	guard.mu.Unlock()
	var once sync.Once
	var result error
	return func() error {
		once.Do(func() { guard.mu.Lock(); guard.reconciliationDepth--; guard.mu.Unlock(); result = finish() })
		return result
	}, nil
}
func (g *mutationGuard) reconciliationActive() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reconciliationDepth != 0
}

// VetoReconciliationReplay applies to the complete owning invocation, including
// a composing parent's recovery, from the first admitted allocation boundary.
func VetoReconciliationReplay(ctx context.Context) error {
	scope, _ := ctx.Value(dataScopeContextKey{}).(*dataScope)
	guard := scope.mutationGuard()
	if guard == nil {
		return errors.New("finite_reconciliation requires an invocation-owned data scope")
	}
	guard.mu.Lock()
	guard.reconciliationReplayVeto = true
	guard.mu.Unlock()
	return ctx.Err()
}
func (g *mutationGuard) reconciliationVetoedReplay() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reconciliationReplayVeto
}
