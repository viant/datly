package registry

import (
	"context"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"github.com/viant/structology"
	"reflect"
	"sync"
	"testing"
)

type scopePlanInput struct {
	Lazy string               `bind:"kind=scope_plan,scope=lazy"`
	Has  *struct{ Lazy bool } `setMarker:"true"`
}
type scopePlanProvider struct{ calls int }

func (*scopePlanProvider) Kind() string                                { return "scope_plan" }
func (*scopePlanProvider) Priority() int                               { return 0 }
func (p *scopePlanProvider) Locate(*structology.State) locator.Locator { return p }
func (p *scopePlanProvider) Value(context.Context, reflect.Type, string) (any, bool, error) {
	p.calls++
	return "value", true, nil
}
func TestInputScopeEmptyPlanDoesNotInferTagsAndCacheIsImmutable(t *testing.T) {
	injector, _ := bindly.NewInjector()
	required := true
	binding := bindly.BindingSpec{Path: "Lazy", Scope: "lazy", Location: state.Location{Kind: "scope_plan"}, Required: &required}
	plan, err := injector.CompilePlan(reflect.TypeFor[scopePlanInput](), binding)
	if err != nil {
		t.Fatal(err)
	}
	projection, _ := plan.Projection()
	ref := spec.RouteRef{Method: "GET", Path: "/scope"}
	owner, err := NewInputContract(reflect.TypeFor[scopePlanInput](), projection, RouteInput{Route: ref, Plan: plan, Bindings: []bindly.BindingSpec{binding}})
	if err != nil {
		t.Fatal(err)
	}
	route, _ := owner.ForRoute(ref)
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := route.PrepareScopePlans(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	empty, ok := route.ScopePlan("")
	if !ok {
		t.Fatal("empty missing")
	}
	lazy, ok := route.ScopePlan("lazy")
	if !ok {
		t.Fatal("lazy missing")
	}
	again, _ := route.ScopePlan("lazy")
	if lazy != again || route.Plan() != plan {
		t.Fatal("changed canonical plan")
	}
	if _, ok := route.ScopePlan(" lazy"); ok {
		t.Fatal("trimmed scope")
	}
	fields := route.Fields()
	*fields[0].Binding().Required = false
	p := &scopePlanProvider{}
	scope, _ := injector.ForScope(p)
	target := &scopePlanInput{}
	if err := scope.Bind(t.Context(), target, bindly.WithPlan(empty), bindly.WithSource(target)); err != nil {
		t.Fatal(err)
	}
	if target.Has != nil || target.Lazy != "" || p.calls != 0 {
		t.Fatalf("empty scope inferred tags %+v calls%d", target, p.calls)
	}
	if err := scope.Bind(t.Context(), target, bindly.WithPlan(lazy), bindly.WithSource(target)); err != nil {
		t.Fatal(err)
	}
	if target.Lazy != "value" || target.Has == nil || !target.Has.Lazy || p.calls != 1 {
		t.Fatal(target, p.calls)
	}
}
