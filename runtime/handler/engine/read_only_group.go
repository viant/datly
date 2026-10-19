package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/internal/drainowner"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	xhandler "github.com/viant/xdatly/handler"
	xresponse "github.com/viant/xdatly/response"
)

// ErrReadOnlyDependency is a sticky managed-effect/admission failure, not a
// missing optional binding. This policy does not sandbox arbitrary Go code.
var ErrReadOnlyDependency = errors.New("read-only dependency group violation")

type readOnlyDependencyKey struct{}
type readOnlyDependency struct {
	group    *readOnlyGroup
	expected string
	anchor   context.Context
}
type readOnlyGroup struct {
	mu       sync.Mutex
	guard    *mutationGuard
	open     bool
	members  map[string]string
	entered  map[string]bool
	observed map[string]bool
}

func readOnlyFrom(ctx context.Context) *readOnlyDependency {
	permit, _ := ctx.Value(readOnlyDependencyKey{}).(*readOnlyDependency)
	if permit == nil {
		if scope, _ := ctx.Value(dataScopeContextKey{}).(*dataScope); scope != nil {
			permit = scope.readOnlyDependency
		}
	}
	return permit
}

// ReadOnlyDependency reports private invocation authority, never a public tag
// or HTTP-method inference. Runtime uses it before resolving child capabilities.
func ReadOnlyDependency(ctx context.Context) bool { return readOnlyFrom(ctx) != nil }

func readOnlyProviders(ctx context.Context, providers []locator.Provider) []locator.Provider {
	if !ReadOnlyDependency(ctx) {
		return providers
	}
	// These expose unchecked external effects/raw DB handles rather than the
	// guarded Datly data surface. Native readers retain their internal connector.
	for _, kind := range []string{string(xhandler.MessageBusKey), string(xhandler.ConnectorKey)} {
		blocked := handlerprovider.Named(kind, func(context.Context, reflect.Type, string) (any, bool, error) {
			return nil, false, RejectReadOnlyDependency(ctx, "unchecked "+kind+" capability")
		})
		found := false
		for i, provider := range providers {
			if provider.Kind() == kind {
				providers[i], found = blocked, true
				break
			}
		}
		if !found {
			providers = append(providers, blocked)
		}
	}
	return providers
}

func RejectReadOnlyDependency(ctx context.Context, reason string) error {
	err := fmt.Errorf("%w: %s", ErrReadOnlyDependency, reason)
	if permit := readOnlyFrom(ctx); permit != nil {
		permit.group.guard.rejectReadOnly(err)
	}
	return err
}

func criticalReadOnlyFailure(cause error) bool {
	if cause == nil {
		return false
	}
	status := xresponse.ErrorStatusCode(cause, 0)
	return errors.Is(cause, ErrReadOnlyDependency) || drainowner.IsTerminalBindingFailure(cause) || status == 401 || status == 403
}

// RecordReadOnlyFailure preserves terminal failures rejected before child activity
// admission, even when a handwritten hook catches the immediate invocation error.
func RecordReadOnlyFailure(ctx context.Context, cause error) {
	if permit := readOnlyFrom(ctx); permit != nil && criticalReadOnlyFailure(cause) {
		permit.group.guard.rejectReadOnly(cause)
	}
}

// AdmitReadOnlyDependency consumes the exact initial binding target; descendants
// may call other canonical readers, under the same read-only owner and lifetime.
func AdmitReadOnlyDependency(ctx context.Context, route string, canonicalReader bool) (context.Context, error) {
	permit := readOnlyFrom(ctx)
	if permit == nil {
		return ctx, nil
	}
	permit.group.mu.Lock()
	open := permit.group.open
	permit.group.mu.Unlock()
	scope, _ := ctx.Value(dataScopeContextKey{}).(*dataScope)
	if !open || scope.mutationGuard() != permit.group.guard || !canonicalReader || permit.expected != "" && permit.expected != route {
		return nil, RejectReadOnlyDependency(ctx, "child must be a canonical reader in the current binding lifetime")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	copy := *permit
	copy.expected = ""
	return context.WithValue(ctx, readOnlyDependencyKey{}, &copy), nil
}

func (g *mutationGuard) rejectReadOnly(err error) {
	g.mu.Lock()
	defer g.unlockAndPublishProtectedFailure()
	g.readOnlyFailure = errors.Join(g.readOnlyFailure, err)
	g.guardedFailure = errors.Join(g.guardedFailure, err)
}

func (c resolutionGroupController) openReadOnly(plan bindly.ResolutionGroupPlan) (bindly.ResolutionGroupRun, error) {
	group := &readOnlyGroup{guard: c.scope.mutationGuard(), open: true, members: map[string]string{}, entered: map[string]bool{}, observed: map[string]bool{}}
	barriers := map[string]bool{}
	for _, path := range plan.After() {
		barriers[path] = true
	}
	for _, field := range c.request.Input.Fields() {
		binding := field.Binding()
		if binding.Required != nil && *binding.Required && (field.VerifiesJWT() || binding.ResolutionGroup == nil && (binding.ErrorCode == 401 || binding.ErrorCode == 403)) && !barriers[field.Path()] {
			return nil, fmt.Errorf("read-only group %s must declare authorization input %s as a barrier", plan.Name(), field.Path())
		}
	}
	for _, member := range plan.Members() {
		found := false
		for _, field := range c.request.Input.Fields() {
			if field.Path() != member.Path {
				continue
			}
			target, ok := field.Dependency()
			if !ok || field.Binding().ResolutionGroup == nil || field.Binding().ResolutionGroup.Mode != bindly.ResolutionGroupDependencies {
				return nil, fmt.Errorf("read-only group member %s requires a canonical component binding", member.Path)
			}
			group.members[member.Path], found = target.String(), true
			break
		}
		if !found {
			return nil, fmt.Errorf("read-only group member %s is not canonical", member.Path)
		}
	}
	guard := group.guard
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if guard.completionClosed || guard.guardedFailure != nil || drainowner.BindingGroupOpen(guard.protectedIssuer) {
		return nil, errors.Join(ErrReadOnlyDependency, guard.guardedFailure)
	}
	guard.readOnlyGroups++
	return group, nil
}

func (g *readOnlyGroup) Enter(ctx context.Context, path string) (context.Context, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	target, exists := g.members[path]
	if !g.open || !exists || g.entered[path] {
		return nil, ErrReadOnlyDependency
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.entered[path] = true
	return context.WithValue(ctx, readOnlyDependencyKey{}, &readOnlyDependency{group: g, expected: target, anchor: ctx}), nil
}

func (g *readOnlyGroup) Observe(_ context.Context, outcome bindly.ResolutionOutcome) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.members[outcome.Path()]; !g.open || !exists || g.observed[outcome.Path()] {
		return ErrReadOnlyDependency
	}
	g.observed[outcome.Path()] = true
	// Business/provider errors remain binding errors, sorted by Bindly. A caught
	// managed violation must still cancel peers and fail this group.
	g.guard.mu.Lock()
	failure := g.guard.readOnlyFailure
	g.guard.mu.Unlock()
	if failure == nil && criticalReadOnlyFailure(outcome.Cause()) {
		return outcome.Cause()
	}
	return failure
}

func (g *readOnlyGroup) Close(context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.open {
		return ErrReadOnlyDependency
	}
	g.open = false
	g.guard.mu.Lock()
	defer g.guard.mu.Unlock()
	g.guard.readOnlyGroups--
	if len(g.observed) != len(g.members) {
		return errors.Join(ErrReadOnlyDependency, g.guard.readOnlyFailure)
	}
	return g.guard.readOnlyFailure
}
