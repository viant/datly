package engine

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	compiler "github.com/viant/datly/runtime/handler/compiler"
	provider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
)

func companionRoute(t *testing.T) *registry.RouteInputContract {
	t.Helper()
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	component, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := compiler.New(compiler.Input{Component: component, InputType: reflect.TypeFor[fixture.Input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/selector-records"})
	if !ok {
		t.Fatal("route missing")
	}
	return route
}

type companionSource struct{ opens int }

func (s *companionSource) Open(context.Context) (xhandler.Data, error) {
	s.opens++
	return nil, fmt.Errorf("unexpected source open")
}
func TestSelectorCompanionEngineRejectsUnsupportedModesBeforeBinding(t *testing.T) {
	route := companionRoute(t)
	for _, tc := range []struct {
		name  string
		alter func(*Request, *companionSource)
	}{
		{"not native reader", func(r *Request, _ *companionSource) { r.CanonicalReader = false }},
		{"noncanonical bound input", func(r *Request, _ *companionSource) { r.BoundInput = &fixture.Input{}; r.CanonicalReader = false }},

		{"replay", func(r *Request, _ *companionSource) { r.Replay = &bindly.ReplayBinding{} }},
		{"buffered data", func(r *Request, s *companionSource) { r.BufferedComponentCalls = true; r.DataSource = s }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entries, lookups := 0, 0
			source := &companionSource{}
			query := provider.Named("query", func(context.Context, reflect.Type, string) (any, bool, error) { lookups++; return "1", true, nil })
			request := Request{Input: route, CanonicalReader: true, Providers: []locator.Provider{query}, Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { entries++; return nil, nil })}
			tc.alter(&request, source)
			if _, err := New().Execute(t.Context(), request); err == nil || !strings.Contains(err.Error(), "selector companion") {
				t.Fatalf("unsupported mode admitted: %v", err)
			}
			if entries != 0 || lookups != 0 || source.opens != 0 {
				t.Fatalf("unsupported mode touched providers/handler/data: %d/%d/%d", lookups, entries, source.opens)
			}
		})
	}
}
func TestSelectorCompanionEngineBindsBeforeHandlerAndCarriesCanonicalPreparation(t *testing.T) {
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
	_, err := New().Execute(t.Context(), Request{Input: route, CanonicalReader: true, Providers: []locator.Provider{query}, Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
		entries++
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
	if err != nil || entries != 1 {
		t.Fatalf("engine invocation failed: %v entries=%d", err, entries)
	}
}

func TestSelectorCompanionOptionalPreparationPreservesExactRoute(t *testing.T) {
	route := companionRoute(t)
	for _, name := range []string{"ID", "id"} {
		got, err := route.WithOptionalBindings(name)
		if err != nil || got != route || got.Plan() != route.Plan() {
			t.Fatalf("optional no-op changed canonical contract: %v", err)
		}
	}
	if _, err := route.WithOptionalBindings("missing"); err == nil {
		t.Fatal("unknown alias admitted")
	}
}

func TestSelectorCompanionAbsentOptionalQueryDoesNotInheritSideValues(t *testing.T) {
	route := companionRoute(t)
	sideReads := 0
	parent := provider.Named("query", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
		if name == "fields" || name == "page" {
			sideReads++
			return []string{"label"}, true, nil
		}
		return "7", name == "id", nil
	})
	injector, err := bindly.NewInjector(bindly.WithProviders(parent))
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	_, err = New().Execute(t.Context(), Request{Input: route, CanonicalReader: true, Injector: injector, Handler: rhandler.HandlerFunc(func(_ context.Context, inv rhandler.Invocation) (any, error) {
		calls++
		side, err := route.SelectorPlan().Value(inv.Binder, inv.Input)
		if err != nil {
			return nil, err
		}
		if side.Elem().FieldByName("Fields").Len() != 0 || side.Elem().FieldByName("Page").Int() != 0 {
			t.Fatal("inherited query acquired side authority")
		}
		return nil, nil
	})})
	if err != nil || calls != 1 || sideReads != 0 {
		t.Fatalf("err=%v calls=%d sideReads=%d", err, calls, sideReads)
	}
}
