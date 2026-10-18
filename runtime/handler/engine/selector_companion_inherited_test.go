package engine

import (
	"context"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/selectorcompanion"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/compiler"
	provider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	"reflect"
	"testing"
)

func TestSelectorCompanionInheritedRejectsBeforeOwnerResolution(t *testing.T) {
	for _, mode := range []string{"noncanonical bound input", "replay", "child source", "priority interleaving"} {
		t.Run(mode, func(t *testing.T) {
			source := &companionSource{}
			childSource := &companionSource{}
			entries, lookups := 0, 0
			route := companionRoute(t)
			_, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: source, Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				parent := ctx.Value(dataScopeContextKey{}).(*dataScope)
				request := Request{Input: route, CanonicalReader: true, BufferedComponentCalls: true, Providers: []locator.Provider{provider.Named("query", func(context.Context, reflect.Type, string) (any, bool, error) { lookups++; return "1", true, nil })}, Handler: rh.HandlerFunc(func(ctx context.Context, _ rh.Invocation) (any, error) {
					entries++
					child := ctx.Value(dataScopeContextKey{}).(*dataScope)
					if child.parent != parent || child.root != parent.root {
						t.Error("owner identity changed")
					}
					return nil, nil
				})}
				switch mode {
				case "missing composed query":
					request.Providers = nil
				case "noncanonical bound input":
					request.BoundInput = &fixture.Input{}
					request.CanonicalReader = false
				case "resolved input":
					request.ResolvedInput = &fixture.Input{}
					request.ResolvedPaths = []string{"ID"}
				case "replay":
					request.Replay = &bindly.ReplayBinding{}
				case "priority interleaving":
					request.Input = interleavedCompanionRoute(t)
				case "child source":
					request.DataSource = childSource
				}
				return New().Execute(PrepareComponent(ctx, ComponentBufferedImperative, ""), request)
			})})
			if err == nil || entries != 0 || lookups != 0 || source.opens != 0 || childSource.opens != 0 {
				t.Fatalf("err=%v handler=%d lookup=%d opens=%d/%d", err, entries, lookups, source.opens, childSource.opens)
			}
		})
	}
}

func interleavedCompanionRoute(t *testing.T) *registry.RouteInputContract {
	t.Helper()
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[fixture.Input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	p := compiled.SelectorPlan
	selected, err := selectorcompanion.Select(c, reflect.TypeFor[fixture.Input]())
	if err != nil {
		t.Fatal(err)
	}
	reordered := append([]bindly.BindingSpec(nil), compiled.Bindings...)
	reordered[0].Priority = 1
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	public, err := injector.CompilePlan(reflect.TypeFor[fixture.Input](), reordered...)
	if err != nil {
		t.Fatal(err)
	}
	scratch, err := injector.CompilePlan(p.Type(), p.Bindings()...)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := selectorcompanion.NewBindingPlan(c, selected, reflect.TypeFor[fixture.Input](), public, scratch, reordered, p.Bindings())
	if err != nil {
		t.Fatal(err)
	}
	projection, err := public.Projection()
	if err != nil {
		t.Fatal(err)
	}
	ref := spec.RouteRef{Method: "GET", Path: "/selector-records"}
	contract, err := registry.NewInputContract(reflect.TypeFor[fixture.Input](), projection, registry.RouteInput{Route: ref, Plan: public, Bindings: reordered, Selectors: plan})
	if err != nil {
		t.Fatal(err)
	}
	route, ok := contract.ForRoute(ref)
	if !ok {
		t.Fatal("route missing")
	}
	return route
}
func TestSelectorCompanionInheritedIdentityAndCanonicalPreparation(t *testing.T) {
	route := companionRoute(t)
	query := provider.Named("query", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
		switch name {
		case "id":
			return "9", true, nil
		case "fields":
			return []string{"id"}, true, nil
		case "page":
			return "1", true, nil
		}
		return nil, false, nil
	})
	entries := 0
	db := sqlite.New(t)
	source := &dml.Source{DB: db.DB}
	var parent *dataScope
	_, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: source, Handler: rh.HandlerFunc(func(ctx context.Context, _ rh.Invocation) (any, error) {
		parent = ctx.Value(dataScopeContextKey{}).(*dataScope)
		return New().Execute(PrepareComponent(ctx, ComponentBufferedImperative, ""), Request{Input: route, CanonicalReader: true, BufferedComponentCalls: true, Providers: []locator.Provider{query}, Handler: rh.HandlerFunc(func(ctx context.Context, invocation rh.Invocation) (any, error) {
			entries++
			child := ctx.Value(dataScopeContextKey{}).(*dataScope)
			if child.parent != parent || child.root != parent.root || child.source != nil {
				t.Fatal("borrowed owner identity changed")
			}
			input := invocation.Input.(*fixture.Input)
			if input.ID != 9 || input.Has == nil || !input.Has.ID {
				t.Fatal("canonical public binding missing")
			}
			value, err := route.SelectorPlan().Value(invocation.Binder, input)
			if err != nil {
				t.Fatal(err)
			}
			if value.Elem().FieldByName("Page").Int() != 1 {
				t.Fatal("companion was not bound in shared binding phase")
			}
			raw, ok, err := invocation.Binder.Lookup(ctx, dexec.ReaderInputPreparerKey)
			if err != nil || !ok {
				t.Fatal("existing preparer missing")
			}
			prepared, err := raw.(dexec.ReaderInputPreparer)(ctx, "ID")
			if err != nil {
				t.Fatal(err)
			}
			clone, ok := prepared.Input.(*fixture.Input)
			if !ok || clone == input || clone.ID != 0 || input.ID != 9 || !input.Has.ID {
				t.Fatal("canonical Without/original preservation failed")
			}
			carried, err := route.SelectorPlan().Value(prepared.Binder, clone)
			if err != nil {
				t.Fatal(err)
			}
			if carried.Pointer() == value.Pointer() || carried.Elem().FieldByName("Page").Int() != 1 {
				t.Fatal("prepared companion was not detached")
			}
			fields := carried.Elem().FieldByName("Fields")
			fields.Index(0).SetString("changed")
			if value.Elem().FieldByName("Fields").Index(0).String() != "id" {
				t.Fatal("prepared selector mutation reached original")
			}
			return nil, nil
		})})
	})})
	if err != nil || entries != 1 {
		t.Fatalf("engine invocation failed: %v entries=%d", err, entries)
	}
}
