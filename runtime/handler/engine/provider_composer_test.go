package engine

import (
	"context"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
)

func TestProviderComposerPreservesPerNameFallback(t *testing.T) {
	protocol := testharness.Request{}.WithQuery(url.Values{"name": {"request"}})
	component := handlerprovider.Named("query", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
		if name == "tenant" {
			return "component", true, nil
		}
		return nil, false, nil
	})
	providers, err := (providerComposer{}).compose(providerComposition{
		component: []locator.Provider{component}, protocol: protocol.Providers(),
	})
	if err != nil {
		t.Fatal(err)
	}
	injector, err := bindly.NewInjector(bindly.WithProviders(providers...))
	if err != nil {
		t.Fatal(err)
	}
	type input struct{ Name, Tenant string }
	plan, err := injector.CompilePlan(reflect.TypeOf(input{}),
		bindly.BindingSpec{Path: "Name", Location: bindstate.Location{Kind: "query", In: "name"}},
		bindly.BindingSpec{Path: "Tenant", Location: bindstate.Location{Kind: "query", In: "tenant"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	actual := &input{}
	if err = injector.Bind(context.Background(), actual, bindly.WithPlan(plan)); err != nil {
		t.Fatal(err)
	}
	if actual.Name != "request" || actual.Tenant != "component" {
		t.Fatalf("input = %+v", actual)
	}
}

func TestProviderComposerRejectsDuplicateAndProtectedAuthorities(t *testing.T) {
	query := handlerprovider.Named("query", func(context.Context, reflect.Type, string) (any, bool, error) { return nil, false, nil })
	validator := handlerprovider.Static(rhandler.ValidatorCapabilityKey, struct{}{})
	tests := []struct {
		name  string
		input providerComposition
		want  string
	}{
		{name: "component cannot override differ", input: providerComposition{component: []locator.Provider{handlerprovider.Static(rhandler.DifferCapabilityKey, struct{}{})}}, want: "protected runtime kind"},
		{name: "duplicate component kind", input: providerComposition{component: []locator.Provider{query, query}}, want: "duplicated"},
		{name: "component protected kind", input: providerComposition{component: []locator.Provider{validator}}, want: "protected runtime kind"},
		{name: "protocol protected kind", input: providerComposition{protocol: []locator.Provider{validator}}, want: "protected runtime kind"},
		{name: "child protected kind", input: providerComposition{child: []locator.Provider{validator}}, want: "protected runtime kind"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := (providerComposer{}).compose(testCase.input)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("compose() error = %v", err)
			}
		})
	}
}

func TestComposeScopePreservesChildAuthorityForInheritance(t *testing.T) {
	parent := engineProviderScope{
		handlerprovider.Named("query", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
			switch name {
			case "name":
				return "parent", true, nil
			case "tenant":
				return "acme", true, nil
			default:
				return nil, false, nil
			}
		}),
	}
	child := handlerprovider.Named("query", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
		if name == "name" {
			return "child", true, nil
		}
		return nil, false, nil
	})
	scope, err := ComposeScope(parent, child)
	if err != nil {
		t.Fatal(err)
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := injector.ForScope(scope.Providers()...)
	if err != nil {
		t.Fatal(err)
	}
	type input struct{ Name, Tenant string }
	plan, err := scoped.CompilePlan(reflect.TypeOf(input{}),
		bindly.BindingSpec{Path: "Name", Location: bindstate.Location{Kind: "query", In: "name"}},
		bindly.BindingSpec{Path: "Tenant", Location: bindstate.Location{Kind: "query", In: "tenant"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	actual := &input{}
	if err = scoped.Bind(context.Background(), actual, bindly.WithPlan(plan)); err != nil {
		t.Fatal(err)
	}
	if actual.Name != "child" || actual.Tenant != "acme" {
		t.Fatalf("input = %+v", actual)
	}
}
