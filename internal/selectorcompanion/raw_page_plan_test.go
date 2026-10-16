package selectorcompanion_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	pagefixture "github.com/viant/datly/bootstrap/testdata/companionpage"
	callbackfixture "github.com/viant/datly/bootstrap/testdata/companionpagecallbacks"
	securedfixture "github.com/viant/datly/bootstrap/testdata/companionsecured"
	"github.com/viant/datly/internal/selectorcompanion"
	compiler "github.com/viant/datly/runtime/handler/compiler"
	provider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
)

func pageComponent(t *testing.T) *spec.Component {
	t.Helper()
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[pagefixture.Component]().PkgPath()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	component, err := found.Components[0].Resolve(reflect.TypeFor[pagefixture.Input](), reflect.TypeFor[pagefixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	return component
}
func compiledPage(t *testing.T) *selectorcompanion.BindingPlan {
	t.Helper()
	c := pageComponent(t)
	result, err := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[pagefixture.Input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	return result.SelectorPlan
}
func TestDeferredPagePlanRetainsCanonicalOwnerButOmitsOnlyExecution(t *testing.T) {
	plan := compiledPage(t)
	if !plan.DeferredPage() {
		t.Fatal("explicit false path missing")
	}
	var page *spec.Parameter
	for _, b := range plan.Bindings() {
		p := b.Extension.(*spec.Parameter)
		if p.QuerySelector.Property == spec.SelectorPropertyPage {
			page = p
		}
	}
	if page == nil || !plan.IsDeferredPage(page) {
		t.Fatal("canonical Page identity missing")
	}
	if f, ok := plan.Field(page); !ok || f.Type != reflect.TypeFor[int]() {
		t.Fatal("canonical Page field/type lost")
	}
	if _, ok := reflect.TypeFor[pagefixture.Input]().FieldByName("Page"); ok {
		t.Fatal("transport Page leaked into public Input")
	}
	if _, ok := reflect.TypeFor[pagefixture.Input]().FieldByName("Fields"); ok {
		t.Fatal("transport Fields leaked into public Input")
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	var seen = map[any]bool{}
	for i := 0; i < 2; i++ {
		scope, err := requestprovider.New(httptest.NewRequest("GET", "/page-records?id=7&fields=id&page=bad&_page=3", nil))
		if err != nil {
			t.Fatal(err)
		}
		scoped, err := injector.ForScope(scope.Providers()...)
		if err != nil {
			t.Fatal(err)
		}
		input := &pagefixture.Input{}
		if err := scoped.Bind(t.Context(), input, bindly.WithPlan(plan.PublicPlan()), bindly.WithSource(input)); err != nil {
			t.Fatal(err)
		}
		marker := input.Has
		value, err := plan.Bind(t.Context(), scoped, input)
		if err != nil {
			t.Fatalf("unevaluated Page executed during binding: %v", err)
		}
		if seen[value] {
			t.Fatal("companion storage shared across invocations")
		}
		seen[value] = true
		if input.ID != 7 || input.Has != marker || marker == nil || !marker.ID {
			t.Fatal("public state replaced")
		}
		carried, err := plan.CarryPage(&delegatingBinder{}, input, value, scope.Providers())
		if err != nil {
			t.Fatal(err)
		}
		resolved, err := plan.Value(carried, input)
		if err != nil || !reflect.DeepEqual(resolved.Elem().FieldByName("Fields").Interface(), []string{"id"}) {
			t.Fatal("native Fields binding lost")
		}
		prepared := &pagefixture.Input{}
		transferred, err := plan.CarryPrepared(carried, input, prepared, &delegatingBinder{})
		if err != nil {
			t.Fatalf("deferred transfer evaluated Page or rejected canonical clone: %v", err)
		}
		preparedPage, err := plan.ResolvePage(t.Context(), transferred, prepared)
		if err != nil || preparedPage != 3 {
			t.Fatalf("deferred source lost after preparation: %d %v", preparedPage, err)
		}
		got, err := plan.ResolvePage(t.Context(), carried, input)
		if err != nil || got != 3 {
			t.Fatalf("late value=%d err=%v", got, err)
		}
		if _, err := plan.ResolvePage(t.Context(), carried, &pagefixture.Input{}); err == nil {
			t.Fatal("different canonical Input pointer admitted")
		}
		scope.Close()
	}
}
func TestDeferredPageRequiresExplicitFalsePolicy(t *testing.T) {
	for _, value := range []*bool{nil, new(bool)} {
		c := pageComponent(t)
		if value != nil {
			*value = true
		}
		c.Settings.IgnoreEmptyQueryParameters = value
		result, err := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[pagefixture.Input]()}).Compile()
		if err != nil {
			t.Fatal(err)
		}
		if result.SelectorPlan.DeferredPage() {
			t.Fatal("nil/true policy granted Page compatibility")
		}
	}
}
func TestDeferredPageRejectsProviderSubstitutionBeforeAnyResolution(t *testing.T) {
	plan := compiledPage(t)
	calls := 0
	custom := provider.Named("query", func(context.Context, reflect.Type, string) (any, bool, error) { calls++; return "7", true, nil })
	native := requestprovider.NewValues().Query()
	composed, err := locator.ComposeProviders("query", locator.ProviderLayer{Name: "native", Provider: native})
	if err != nil {
		t.Fatal(err)
	}
	for _, providers := range [][]locator.Provider{{custom}, {priorityProvider{Provider: native}}, {composed}, {native, custom}, nil} {
		if err := plan.ValidateProviders(providers); err == nil {
			t.Fatalf("unknown/composed query authority accepted: %#v", providers)
		}
	}
	if calls != 0 {
		t.Fatal("rejected provider was resolved")
	}
	if err := plan.ValidateProviders([]locator.Provider{native}); err != nil {
		t.Fatal(err)
	}
}
func TestDeferredPageRejectsInputInitializer(t *testing.T) {
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[securedfixture.Component]().PkgPath()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := found.Components[0].Resolve(reflect.TypeFor[securedfixture.Input](), reflect.TypeFor[securedfixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	c.Settings = &spec.Settings{IgnoreEmptyQueryParameters: new(bool)}
	_, err = compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[securedfixture.Input]()}).Describe()
	if err == nil || !strings.Contains(err.Error(), "input callbacks") {
		t.Fatalf("initializer mode admitted: %v", err)
	}
}
func TestDeferredPageLateFailurePreservesCanonicalBindingCause(t *testing.T) {
	plan := compiledPage(t)
	scope, _ := requestprovider.New(httptest.NewRequest("GET", "/page-records?_page=bad", nil))
	defer scope.Close()
	injector, _ := bindly.NewInjector()
	scoped, _ := injector.ForScope(scope.Providers()...)
	input := &pagefixture.Input{}
	value, err := plan.Bind(t.Context(), scoped, input)
	if err != nil {
		t.Fatal(err)
	}
	carried, err := plan.CarryPage(&delegatingBinder{}, input, value, scope.Providers())
	if err != nil {
		t.Fatal(err)
	}
	_, err = plan.ResolvePage(t.Context(), carried, input)
	var binding *bindly.BindingError
	if !errors.As(err, &binding) || binding.Name != "Page" || binding.Path != "Page" || binding.Kind != "query" || binding.In != "page" || binding.Cause == nil {
		t.Fatalf("canonical native ownership changed: %v", err)
	}
}

func TestDeferredPageRejectsInvokerAndMCPInputCallbacks(t *testing.T) {
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[callbackfixture.InvokerComponent]().PkgPath()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(found.Components) != 2 {
		t.Fatal("genuine callback holders missing")
	}
	for _, source := range found.Components {
		component, err := source.Resolve(source.LinkedInputType, source.LinkedOutputType)
		if err != nil {
			t.Fatal(err)
		}
		_, err = compiler.New(compiler.Input{Component: component, InputType: source.LinkedInputType}).Compile()
		if err == nil || !strings.Contains(err.Error(), "input callbacks") {
			t.Fatalf("%s callback admitted: %v", source.HolderType, err)
		}
	}
}
