package engine

import (
	"context"
	"net/url"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	numericfixture "github.com/viant/datly/bootstrap/testdata/companionnumeric"
	fixture "github.com/viant/datly/bootstrap/testdata/companionpage"
	"github.com/viant/datly/runtime/handler/compiler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
)

type queryCompositionBinder struct{ xhandler.Binder }

func TestDeferredSelectorRetainsInheritedQueryThroughChildFieldsOverride(t *testing.T) {
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
	plan := compiled.SelectorPlan
	for _, tc := range []struct {
		name, page, fallback string
		want                 int
	}{
		{"inherited", "2", "7", 2}, {"malformed primary alias", "bad", "3", 3}, {"negative primary", "-1", "3", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope := requestprovider.NewValues(requestprovider.WithQuery(url.Values{"id": {"7"}, "page": {tc.page}, "_page": {tc.fallback}}))
			defer scope.Close()
			numericReads := 0
			child := handlerprovider.Named("query", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
				if name == "fields" {
					return []string{"id"}, true, nil
				}
				if name == "page" || name == "_page" {
					numericReads++
				}
				return nil, false, nil
			})
			providers, err := (providerComposer{}).compose(providerComposition{protocol: scope.Providers(), child: []locator.Provider{child}})
			if err != nil {
				t.Fatal(err)
			}
			injector, err := bindly.NewInjector(bindly.WithProviders(providers...))
			if err != nil {
				t.Fatal(err)
			}
			input := &fixture.Input{}
			if err = injector.Bind(t.Context(), input, bindly.WithPlan(plan.PublicPlan()), bindly.WithSource(input)); err != nil {
				t.Fatal(err)
			}
			side, err := plan.Bind(t.Context(), injector, input)
			if err != nil {
				t.Fatal(err)
			}
			binder, err := plan.CarryPage(&queryCompositionBinder{}, input, side, providers)
			if err != nil {
				t.Fatal(err)
			}
			if numericReads != 0 {
				t.Fatalf("numeric source read before reader validation: %d", numericReads)
			}
			page, err := plan.ResolvePage(t.Context(), binder, input)
			if err != nil || page != tc.want {
				t.Fatalf("Page=%d err=%v want %d", page, err, tc.want)
			}
		})
	}
}

// No Page declaration means no deferredPage plan can mask an incorrect
// eager numeric binding plan. Observe the winning child source directly.
func TestComposedChildNumericWithoutPageDefersLookup(t *testing.T) {
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[numericfixture.Component]().PkgPath()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	component, err := found.Components[0].Resolve(reflect.TypeFor[numericfixture.Input](), reflect.TypeFor[numericfixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := compiler.New(compiler.Input{Component: component, InputType: reflect.TypeFor[numericfixture.Input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	plan := compiled.SelectorPlan
	for _, bad := range []string{"bad", "1,2"} {
		t.Run(bad, func(t *testing.T) {
			scope := requestprovider.NewValues(requestprovider.WithQuery(url.Values{"limit": {"1"}}))
			defer scope.Close()
			reads := 0
			child := handlerprovider.Named("query", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
				if name == "limit" {
					reads++
					return bad, true, nil
				}
				return nil, false, nil
			})
			providers, err := (providerComposer{}).compose(providerComposition{protocol: scope.Providers(), child: []locator.Provider{child}})
			if err != nil {
				t.Fatal(err)
			}
			injector, err := bindly.NewInjector(bindly.WithProviders(providers...))
			if err != nil {
				t.Fatal(err)
			}
			input := &numericfixture.Input{}
			side, err := plan.Bind(t.Context(), injector, input, providers...)
			if err != nil || reads != 0 {
				t.Fatalf("numeric evaluated before reader: reads=%d err=%v", reads, err)
			}
			carried, err := plan.CarryPage(&queryCompositionBinder{}, input, side, providers)
			if err != nil {
				t.Fatal(err)
			}
			checked := false
			for _, parameter := range plan.PageParameters() {
				if parameter.QuerySelector.View == "children" && parameter.QuerySelector.Property == spec.SelectorPropertyLimit {
					_, err = plan.ResolvePageParameter(t.Context(), carried, input, parameter)
					if err == nil || reads != 1 {
						t.Fatalf("winning child source not evaluated exactly once: reads=%d err=%v", reads, err)
					}
					checked = true
				}
			}
			if !checked {
				t.Fatal("deferred child limit missing")
			}
		})
	}
}
