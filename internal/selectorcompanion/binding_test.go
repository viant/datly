package selectorcompanion_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
)

// Lower-level native binding control. Direct compilation still rejects missing
// public fields without the separately checked runtime companion integration.
func TestExistingBindlyCompanionPlanUsesFreshNativeQueryStorage(t *testing.T) {
	pkg := reflect.TypeFor[fixture.Component]().PkgPath()
	reflected, err := bootstrap.ReflectSelectedPackages([]string{pkg}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(reflected.Components) != 1 {
		t.Fatal("actual fixture holder missing")
	}
	component, err := reflected.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = handlercompiler.BuildBindingSpecs(component, reflect.TypeFor[fixture.Input](), nil); err == nil {
		t.Fatal("missing-public-field baseline unexpectedly accepted")
	}
	descriptor, err := component.SelectorCompanion.Companion()
	if err != nil {
		t.Fatal(err)
	}
	companion := *component
	companion.Parameters = nil
	for _, param := range component.Parameters {
		if param.QuerySelector != nil {
			companion.Parameters = append(companion.Parameters, param)
		}
	}
	bindings, err := handlercompiler.BuildBindingSpecs(&companion, descriptor.Type, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 2 {
		t.Fatal("canonical companion bindings missing")
	}
	for i, p := range companion.Parameters {
		if bindings[i].Extension != p {
			t.Fatal("canonical parameter identity lost")
		}
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(descriptor.Type, bindings...)
	if err != nil {
		t.Fatal(err)
	}
	var previous any
	for _, tc := range []struct {
		name, query string
		page        int
		fields      []string
		bad         bool
	}{
		{"existing repeated CSV semantics", "?fields=id,label&fields=id&page=2", 2, []string{"id", "label", "id"}, false},
		{"missing", "", 0, nil, false},
		{"malformed page", "?page=bad", 0, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest("GET", "/selector-records"+tc.query, nil)
			scope, err := requestprovider.New(request)
			if err != nil {
				t.Fatal(err)
			}
			defer scope.Close()
			scoped, err := injector.ForScope(scope.Providers()...)
			if err != nil {
				t.Fatal(err)
			}
			scratch := reflect.New(descriptor.Type)
			if scratch.Interface() == previous {
				t.Fatal("per-request scratch reused")
			}
			previous = scratch.Interface()
			err = scoped.Bind(context.Background(), scratch.Interface(), bindly.WithPlan(plan))
			if tc.bad {
				var failure *bindly.BindingError
				if !errors.As(err, &failure) || failure.Path != "Page" || failure.Kind != "query" || failure.In != "page" || failure.Cause == nil {
					t.Fatal("native malformed binding provenance missing")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if scratch.Elem().FieldByName("Page").Int() != int64(tc.page) || !reflect.DeepEqual(scratch.Elem().FieldByName("Fields").Interface(), tc.fields) {
				t.Fatalf("native query values changed: fields=%#v page=%v", scratch.Elem().FieldByName("Fields").Interface(), scratch.Elem().FieldByName("Page").Int())
			}
		})
	}
	// Runtime ordering, canonical Input identity, error ownership and explicit
	// selector overrides are exercised separately by the native runtime tests.
}
