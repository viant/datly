package selectorcompanion_test

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	"github.com/viant/datly/internal/selectorcompanion"
	"github.com/viant/datly/internal/selectorcompanion/model"
	compiler "github.com/viant/datly/runtime/handler/compiler"
	provider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	"github.com/viant/x"
	xhandler "github.com/viant/xdatly/handler"
)

func runtimeComponent(t *testing.T) *spec.Component {
	t.Helper()
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func runtimePlan(t *testing.T) (*selectorcompanion.BindingPlan, *spec.Component) {
	t.Helper()
	c := runtimeComponent(t)
	result, err := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[fixture.Input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectorPlan == nil {
		t.Fatal("checked plan absent")
	}
	return result.SelectorPlan, c
}
func TestSelectorCompanionCanonicalExclusionFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*spec.Component)
	}{
		{"different source", func(c *spec.Component) { c.Parameters[len(c.Parameters)-1].Source.Name = "other" }},
		{"different type", func(c *spec.Component) { c.Parameters[len(c.Parameters)-1].TypeExpr = "int64" }},
		{"requiredness", func(c *spec.Component) { value := true; c.Parameters[len(c.Parameters)-1].Required = &value }},
		{"predicate", func(c *spec.Component) {
			c.Parameters[len(c.Parameters)-1].Predicates = []*spec.Predicate{{Name: "extra"}}
		}},
		{"nontrailing", func(c *spec.Component) {
			c.Parameters[0], c.Parameters[len(c.Parameters)-1] = c.Parameters[len(c.Parameters)-1], c.Parameters[0]
		}},
		{"missing nonselector", func(c *spec.Component) {
			c.Parameters = append(c.Parameters, &spec.Parameter{Name: "Absent", TypeExpr: "string", Source: spec.BindSource{Kind: "query", Name: "missing"}})
		}},
		{"overlapping source", func(c *spec.Component) { c.Parameters[0].Source.Name = "fields" }},
		{"foreign kind", func(c *spec.Component) { c.Key.Kind = spec.KindView }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := runtimeComponent(t)
			tc.change(c)
			if _, err := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[fixture.Input]()}).Compile(); err == nil {
				t.Fatal("unsupported exclusion accepted")
			}
		})
	}
}

type priorityProvider struct {
	locator.Provider
	priority int
}

func (p priorityProvider) Priority() int { return p.priority }
func TestSelectorCompanionComposedProviderAndPlanAuthority(t *testing.T) {
	p, c := runtimePlan(t)
	public := &fixture.Input{}
	source := provider.Named("query", func(_ context.Context, _ reflect.Type, key string) (any, bool, error) {
		switch key {
		case "id":
			return "7", true, nil
		case "fields":
			return []string{"id"}, true, nil
		case "page":
			return "2", true, nil
		}
		return nil, false, nil
	})
	if err := p.ValidateProviders([]locator.Provider{source}); err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateProviders(nil); err != nil {
		t.Fatal("absent optional query source rejected", err)
	}
	selected, err := selectorcompanion.Select(c, reflect.TypeFor[fixture.Input]())
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[fixture.Input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	reordered := append([]bindly.BindingSpec(nil), compiled.Bindings...)
	reordered[0].Priority = 1
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	publicPlan, err := injector.CompilePlan(reflect.TypeFor[fixture.Input](), reordered...)
	if err != nil {
		t.Fatal(err)
	}
	scratchPlan, err := injector.CompilePlan(p.Type(), p.Bindings()...)
	if err != nil {
		t.Fatal(err)
	}
	interleaved, err := selectorcompanion.NewBindingPlan(c, selected, reflect.TypeFor[fixture.Input](), publicPlan, scratchPlan, reordered, p.Bindings())
	if err != nil {
		t.Fatal(err)
	}
	if err := interleaved.ValidateProviders([]locator.Provider{source}); err == nil {
		t.Fatal("effective priority interleaving admitted")
	}
	if err := p.ValidateProviders([]locator.Provider{priorityProvider{Provider: source, priority: 20}}); err != nil {
		t.Fatal("equal effective priorities should preserve stable canonical order")
	}

	for _, binding := range p.Bindings() {
		parameter, ok := binding.Extension.(*spec.Parameter)
		if !ok {
			t.Fatal("canonical pointer lost")
		}
		found := false
		for _, actual := range c.Parameters {
			found = found || parameter == actual
		}
		if !found {
			t.Fatal("noncanonical Parameter pointer")
		}
	}
	root, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	scope, err := root.ForScope(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := scope.Bind(t.Context(), public, bindly.WithPlan(p.PublicPlan()), bindly.WithSource(public)); err != nil {
		t.Fatal(err)
	}
	marker := public.Has
	value, err := p.Bind(t.Context(), scope, public)
	if err != nil {
		t.Fatal(err)
	}
	if public.ID != 7 || public.Has != marker || !public.Has.ID {
		t.Fatal("canonical public state changed")
	}
	delegated := &delegatingBinder{failure: errors.New("unchanged binder failure")}
	carried, err := p.Carry(delegated, public, value)
	if err != nil {
		t.Fatal(err)
	}
	if err := carried.Bind(t.Context(), public); err != delegated.failure {
		t.Fatal("Bind delegation changed")
	}
	got, found, err := carried.Lookup(t.Context(), "test")
	if got != public || !found || err != delegated.failure {
		t.Fatal("Lookup delegation changed")
	}
	actual, err := p.Value(carried, public)
	if err != nil || actual.Interface() != value {
		t.Fatal("fresh state lost")
	}
	another, err := p.Bind(t.Context(), scope, public)
	if err != nil || another == value {
		t.Fatal("invocation scratch reused")
	}
	if _, err = p.Value(carried, &fixture.Input{}); err == nil {
		t.Fatal("foreign canonical pointer admitted")
	}
	other, _ := runtimePlan(t)
	if _, err = other.Value(carried, public); err == nil {
		t.Fatal("foreign plan admitted")
	}
	if _, err = p.Value(delegated, public); err == nil {
		t.Fatal("unissued carrier admitted")
	}
	bad := provider.Named("query", func(_ context.Context, _ reflect.Type, key string) (any, bool, error) {
		return "bad", key == "page", nil
	})
	badScope, _ := root.ForScope(bad)
	if _, err = p.Bind(t.Context(), badScope, public); err == nil {
		t.Fatal("malformed native binding accepted")
	} else {
		var binding *bindly.BindingError
		var conversion *strconv.NumError
		if !errors.As(err, &binding) || binding.Name != "Page" || binding.Path != "Page" || binding.Kind != "query" || binding.In != "page" || binding.Cause == nil || !errors.As(binding.Cause, &conversion) || conversion.Num != "bad" {
			t.Fatalf("native binding provenance changed: %v", err)
		}
	}
}

type delegatingBinder struct {
	input   any
	failure error
}

func (b *delegatingBinder) Bind(_ context.Context, target any) error {
	b.input = target
	return b.failure
}
func (b *delegatingBinder) Lookup(context.Context, xhandler.ValueKey) (any, bool, error) {
	return b.input, true, b.failure
}

func TestSelectorCompanionMalformedReflectedAssociationFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		holder reflect.Type
		field  string
	}{
		{"scalar holder", reflect.TypeFor[int](), "ID"},
		{"scalar component field", reflect.TypeFor[fixture.Input](), "ID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := runtimeComponent(t)
			descriptor, err := component.SelectorCompanion.Companion()
			if err != nil {
				t.Fatal(err)
			}
			component.SelectorCompanion, err = model.New(x.NewType(tc.holder), tc.field, descriptor)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = selectorcompanion.Select(component, reflect.TypeFor[fixture.Input]()); err == nil {
				t.Fatal("malformed reflected association accepted")
			}
		})
	}
}
