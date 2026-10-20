package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	hp "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/sql/dml"
)

func TestReadOnlyDependencyGroupNestedBindingConcurrencyOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type parent struct{ A, B int }
	type child struct{ Nested int }
	bindings := []bindly.BindingSpec{}
	for _, name := range []string{"A", "B"} {
		bindings = append(bindings, bindly.BindingSpec{Path: name, Location: bindstate.Location{Kind: "component", In: "GET:/" + name}, ResolutionGroup: &bindly.ResolutionGroupSpec{Name: "readers", Mode: bindly.ResolutionGroupDependencies}})
	}
	rootInput := testRouteInput(t, reflect.TypeFor[parent](), bindings...)
	childInput := testRouteInput(t, reflect.TypeFor[child](), bindly.BindingSpec{Path: "Nested", Location: bindstate.Location{Kind: "component", In: "GET:/leaf"}})
	leafInput := testRouteInput(t, reflect.TypeFor[struct{}]())
	root := neutralDataScope(ctx)
	require.NoError(t, root.enrollBufferedScope(ctx))
	ctx = withDataScope(ctx, root)
	e := New()
	var provider locator.Provider
	provider = drainowner.SealBindingGroupProvider(hp.Named("component", func(ctx context.Context, _ reflect.Type, name string) (any, bool, error) {
		ctx, err := AdmitReadOnlyDependency(ctx, name, true)
		if err != nil {
			return nil, false, err
		}
		input := childInput
		handler := rh.HandlerFunc(func(_ context.Context, invocation rh.Invocation) (any, error) {
			return invocation.Input.(*child).Nested + 1, nil
		})
		if name == "GET:/leaf" {
			input = leafInput
			handler = func(context.Context, rh.Invocation) (any, error) { return 1, nil }
		}
		result, err := e.Execute(ctx, Request{CanonicalReader: true, BufferedComponentCalls: true, Input: input, Components: provider, Handler: handler})
		return result, err == nil, err
	}))
	injector, err := bindly.NewInjector(bindly.WithProviders(provider))
	require.NoError(t, err)
	input := &parent{}
	err = injector.Bind(ctx, input, bindly.WithPlan(rootInput.Plan()), bindly.WithSource(input), bindly.WithResolutionGroupConcurrency(1), bindly.WithResolutionGroupController(resolutionGroupController{scope: root, request: Request{BufferedComponentCalls: true, Input: rootInput, Components: provider}}))
	require.NoError(t, err)
	require.Equal(t, &parent{A: 2, B: 2}, input)
	require.NoError(t, completeDataScope(ctx, root, true, nil))
}

func TestReadOnlyDependencyRetainedCapabilityCannotWriteAfterGroup(t *testing.T) {
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE audit(id INTEGER)"))
	root, _ := invocationDataScope(t.Context(), dml.Source{DB: db.DB})
	require.NoError(t, root.enrollBufferedScope(t.Context()))
	data, err := root.resolve(t.Context())
	require.NoError(t, err)
	root.writeEligibility.readOnlyGroups = 1
	child := &dataScope{root: root, parent: root, readOnlyDependency: &readOnlyDependency{}}
	capability := newHandlerData(data, child.capabilityGuard())
	root.writeEligibility.readOnlyGroups = 0
	err = capability.Execute("INSERT INTO audit VALUES(1)")
	require.ErrorIs(t, err, ErrReadOnlyDependency)
	require.ErrorIs(t, drainowner.ProtectedFailure(root.nativeInvocation), ErrReadOnlyDependency)
	require.Error(t, completeDataScope(t.Context(), root, true, err))
	var count int
	require.NoError(t, db.DB.QueryRow("SELECT count(*) FROM audit").Scan(&count))
	require.Zero(t, count)
}

func TestReadOnlyDependencyGroupDoesNotSuppressAdmissionFailure(t *testing.T) {
	ctx := t.Context()
	root := neutralDataScope(ctx)
	group := &readOnlyGroup{guard: root.mutationGuard(), open: true}
	ctx = context.WithValue(withDataScope(ctx, root), readOnlyDependencyKey{}, &readOnlyDependency{group: group, anchor: ctx})
	_, err := AdmitReadOnlyDependency(ctx, "POST:/writer", false)
	require.ErrorIs(t, err, ErrReadOnlyDependency)
	require.ErrorIs(t, root.mutationGuard().readOnlyFailure, ErrReadOnlyDependency)
	if !errors.Is(CheckComponentMutation(ctx, false), ErrReadOnlyDependency) {
		t.Fatal(fmt.Sprint(err))
	}
}

func TestReadOnlyDependencyAuthorizationBarrier(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			type input struct{ Auth, Child int }
			required := true
			group := &bindly.ResolutionGroupSpec{Name: "readers", Mode: bindly.ResolutionGroupDependencies}
			if after {
				group.After = []string{"Auth"}
			}
			contract := testRouteInput(t, reflect.TypeFor[input](),
				bindly.BindingSpec{Path: "Auth", Location: bindstate.Location{Kind: "header", In: "Authorization"}, Required: &required, ErrorCode: 401},
				bindly.BindingSpec{Path: "Child", Location: bindstate.Location{Kind: "component", In: "GET:/child"}, ResolutionGroup: group},
			)
			root := neutralDataScope(t.Context())
			require.NoError(t, root.enrollBufferedScope(t.Context()))
			var calls atomic.Int32
			component := drainowner.SealBindingGroupProvider(hp.Named("component", func(context.Context, reflect.Type, string) (any, bool, error) { calls.Add(1); return 1, true, nil }))
			denied := errors.New("authorization denied")
			injector, err := bindly.NewInjector(bindly.WithProviders(component, hp.Named("header", func(context.Context, reflect.Type, string) (any, bool, error) { return nil, false, denied })))
			require.NoError(t, err)
			value := &input{}
			err = injector.Bind(t.Context(), value, bindly.WithSource(value), bindly.WithPlan(contract.Plan()), bindly.WithResolutionGroupController(resolutionGroupController{scope: root, request: Request{BufferedComponentCalls: true, Input: contract, Components: component}}))
			if after {
				require.ErrorIs(t, err, denied)
			} else {
				require.ErrorContains(t, err, "authorization input Auth as a barrier")
			}
			require.Zero(t, calls.Load())
		})
	}
}
